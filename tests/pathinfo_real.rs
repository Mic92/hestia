//! Integration tests for the store database client against real Nix stores:
//! a hermetic scratch store created per test, plus cross-checks against the
//! system store (`/nix/store`).
//!
//! Tests skip themselves (with a notice) when the required tooling or
//! database is unavailable (e.g. inside the Nix build sandbox).

mod support;

use hestia::manifest::Hash32;
use hestia::pathinfo::Lookup;
use hestia::upstream::UpstreamFilter;

use support::store::{ScratchStore, find_real_store_path, nix_path_info_json, system_db_or_skip};

#[test]
fn scratch_store_query_matches_nix_oracle() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("oracle", 42);
    let database = store.database();

    let lookup = database
        .query(fixture.to_str().unwrap())
        .expect("query failed");
    let Lookup::Found(info) = lookup else {
        panic!("freshly added path must be found, got {lookup:?}");
    };

    // Compare against nix's own record of the path.
    let oracle = store
        .path_info_json(&fixture)
        .expect("nix path-info oracle unavailable for scratch store");
    assert_eq!(
        info.nar_hash,
        Hash32::parse_sha256(oracle["narHash"].as_str().unwrap()).unwrap(),
        "narHash mismatch"
    );
    assert_eq!(info.nar_size, oracle["narSize"].as_u64().unwrap());

    // nix-store --add registers content-addressed paths: ca must be set,
    // and there are no signatures.
    assert!(info.ca.is_some(), "added paths are content-addressed");
    assert!(info.signatures.is_empty(), "added paths are unsigned");
    assert!(info.references.is_empty());

    // The store path's own hash round-trips.
    assert_eq!(
        info.path_hash().to_string(),
        fixture
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split_once('-')
            .unwrap()
            .0
    );
}

#[test]
fn scratch_store_references_are_recorded() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let (top, dep) = store.add_paths_with_reference("refs");
    let database = store.database();

    let Lookup::Found(top_info) = database.query(top.to_str().unwrap()).unwrap() else {
        panic!("top path must be found");
    };
    let Lookup::Found(dep_info) = database.query(dep.to_str().unwrap()).unwrap() else {
        panic!("dep path must be found");
    };

    // top references dep (and only dep).
    let reference_hashes = top_info.reference_hashes();
    assert_eq!(reference_hashes, vec![dep_info.path_hash()]);

    // dep references nothing.
    assert!(dep_info.references.is_empty());
}

#[test]
fn scratch_store_fake_upstream_signature_is_filtered() {
    // Hermetic version of the upstream-filter requirement: sign a path with
    // a key named exactly like the cache.nixos.org key and verify the
    // default filter skips it, while unsigned paths pass.
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let signed = store.add_fixture("signed", 1);
    let unsigned = store.add_fixture("unsigned", 2);
    store.sign_path(&signed, "cache.nixos.org-1");

    let database = store.database();
    let filter = UpstreamFilter::default();

    let Lookup::Found(signed_info) = database.query(signed.to_str().unwrap()).unwrap() else {
        panic!("signed path must be found");
    };
    assert_eq!(signed_info.signatures.len(), 1);
    assert_eq!(signed_info.signatures[0].name(), "cache.nixos.org-1");
    assert!(
        filter.is_upstream_signed(&signed_info.signatures),
        "path signed with the upstream key name must be filtered"
    );

    let Lookup::Found(unsigned_info) = database.query(unsigned.to_str().unwrap()).unwrap() else {
        panic!("unsigned path must be found");
    };
    assert!(
        !filter.is_upstream_signed(&unsigned_info.signatures),
        "locally built (unsigned) paths must not be filtered"
    );
}

#[test]
fn scratch_store_unknown_and_malformed_lookups() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    // Register something so the database exists.
    store.add_fixture("present", 3);
    let database = store.database();

    // Well-formed but unregistered path.
    let unknown = format!(
        "{}/00000000000000000000000000000000-not-registered",
        database.store_dir()
    );
    assert!(matches!(database.query(&unknown).unwrap(), Lookup::Unknown));

    // Paths from a different store dir are malformed for this store.
    assert!(matches!(
        database
            .query("/nix/store/00000000000000000000000000000000-foreign")
            .unwrap(),
        Lookup::Malformed { .. }
    ));

    // Garbage.
    assert!(matches!(
        database.query("not a path at all").unwrap(),
        Lookup::Malformed { .. }
    ));
}

#[test]
fn scratch_store_batch_query_mixes_all_lookup_kinds() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("batch", 4);
    let database = store.database();

    let unknown = format!(
        "{}/00000000000000000000000000000000-unknown",
        database.store_dir()
    );
    let results = database
        .query_batch([
            fixture.to_str().unwrap().to_string(),
            unknown,
            "/elsewhere/bogus".to_string(),
        ])
        .expect("batch query failed");

    assert_eq!(results.len(), 3);
    assert!(matches!(results[0].1, Lookup::Found(_)));
    assert!(matches!(results[1].1, Lookup::Unknown));
    assert!(matches!(results[2].1, Lookup::Malformed { .. }));
}

#[test]
fn system_database_matches_nix_path_info_json() {
    // Cross-check against the production database: hestia's direct SQLite
    // read must agree with what `nix path-info --json` reports.
    let Some(database) = system_db_or_skip() else {
        return;
    };
    let Some(store_path) = find_real_store_path() else {
        eprintln!("skipping: no real /nix/store path available");
        return;
    };
    let Some(oracle) = nix_path_info_json(&store_path) else {
        eprintln!("skipping: nix path-info not available");
        return;
    };

    let Lookup::Found(info) = database.query(store_path.to_str().unwrap()).unwrap() else {
        panic!("path known to nix path-info must be in the database");
    };

    assert_eq!(
        info.nar_hash,
        Hash32::parse_sha256(oracle["narHash"].as_str().unwrap()).unwrap(),
        "narHash mismatch"
    );
    assert_eq!(info.nar_size, oracle["narSize"].as_u64().unwrap());

    // References agree (sorted).
    let mut expected_refs: Vec<String> = oracle["references"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_string())
        .collect();
    expected_refs.sort();
    let mut actual_refs: Vec<String> = info
        .references
        .iter()
        .map(|reference| format!("{}", database.store_dir().display(reference)))
        .collect();
    actual_refs.sort();
    assert_eq!(actual_refs, expected_refs, "references mismatch");

    // Signature key names agree.
    let expected_sigs: Vec<&str> = oracle["signatures"]
        .as_array()
        .map(|sigs| {
            sigs.iter()
                .map(|sig| sig.as_str().unwrap().split_once(':').unwrap().0)
                .collect()
        })
        .unwrap_or_default();
    let actual_sigs: Vec<&str> = info.signatures.iter().map(|sig| sig.name()).collect();
    assert_eq!(actual_sigs, expected_sigs, "signature key names mismatch");
}

#[test]
fn nar_hash_from_database_matches_local_nar_serialization() {
    // The pipeline's integrity check depends on this property: the database's
    // recorded NAR hash equals what hestia's own chunker produces.
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("narcheck", 5);
    let database = store.database();

    let Lookup::Found(info) = database.query(fixture.to_str().unwrap()).unwrap() else {
        panic!("fixture must be found");
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (nar_hash, nar_size) = runtime
        .block_on(hestia::chunker::nar_hash_and_size(&fixture))
        .expect("local NAR serialization failed");
    assert_eq!(nar_hash, info.nar_hash, "NAR hash mismatch");
    assert_eq!(nar_size, info.nar_size, "NAR size mismatch");
}
