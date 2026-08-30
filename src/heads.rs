//! Head names, head records, and the view rule.
//!
//! ```text
//! g-<epoch>-<time>-<d>                GC head, body = GcRecord, <d> = sha256(body)
//! h-<base_epoch>-<root>-<time>-<seg>  drain head, no body
//! c-<base_epoch>-<root>-<time>-<d>    compaction head, body = CompactionRecord
//! ```
//!
//! All fields are fixed-width lowercase hex so listings sort by epoch and
//! names fit OCI tag and cache-key alphabets. `<time>` is the writer's
//! unix clock, so a head's age needs no backend metadata. `<root>` is a 64-bit hash of
//! the root name. The name itself is found in the GC record's root table
//! or in a `c-*` body, so a brand-new root's first publish must be a `c-*`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use minicbor::{Decode, Encode};

use crate::manifest::SegDigest;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("head record cbor: {0}")]
    Cbor(#[from] minicbor::decode::Error),
    #[error("head record: {0}")]
    Format(String),
}

// ---------------------------------------------------------------- names

pub type RootId = u64;

pub fn root_id(name: &str) -> RootId {
    u64::from_le_bytes(
        blake3::hash(name.as_bytes()).as_bytes()[..8]
            .try_into()
            .unwrap(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HeadName {
    Gc {
        epoch: u64,
        time: u64,
        digest: SegDigest,
    },
    Drain {
        base_epoch: u64,
        root: RootId,
        time: u64,
        seg: SegDigest,
    },
    Compaction {
        base_epoch: u64,
        root: RootId,
        time: u64,
        digest: SegDigest,
    },
}

fn hex_u64(s: &str) -> Option<u64> {
    let canonical = s.len() == 16 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'));
    canonical.then(|| u64::from_str_radix(s, 16).ok()).flatten()
}

impl HeadName {
    pub fn parse(s: &str) -> Option<HeadName> {
        let parts: Vec<&str> = s.split('-').collect();
        Some(match parts.as_slice() {
            ["g", e, t, d] => HeadName::Gc {
                epoch: hex_u64(e)?,
                time: hex_u64(t)?,
                digest: SegDigest::from_hex(d)?,
            },
            ["h", e, r, t, d] => HeadName::Drain {
                base_epoch: hex_u64(e)?,
                root: hex_u64(r)?,
                time: hex_u64(t)?,
                seg: SegDigest::from_hex(d)?,
            },
            ["c", e, r, t, d] => HeadName::Compaction {
                base_epoch: hex_u64(e)?,
                root: hex_u64(r)?,
                time: hex_u64(t)?,
                digest: SegDigest::from_hex(d)?,
            },
            _ => return None,
        })
    }

    pub fn time(&self) -> u64 {
        match self {
            HeadName::Gc { time, .. }
            | HeadName::Drain { time, .. }
            | HeadName::Compaction { time, .. } => *time,
        }
    }
}

impl fmt::Display for HeadName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeadName::Gc {
                epoch,
                time,
                digest,
            } => write!(f, "g-{epoch:016x}-{time:016x}-{digest}"),
            HeadName::Drain {
                base_epoch,
                root,
                time,
                seg,
            } => write!(f, "h-{base_epoch:016x}-{root:016x}-{time:016x}-{seg}"),
            HeadName::Compaction {
                base_epoch,
                root,
                time,
                digest,
            } => write!(f, "c-{base_epoch:016x}-{root:016x}-{time:016x}-{digest}"),
        }
    }
}

// ---------------------------------------------------------------- records

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RootRow {
    #[n(0)]
    pub name: String,
    #[n(1)]
    pub seg: SegDigest,
    /// Unix time GC last folded a writer head for this root.
    #[n(2)]
    pub stamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Origin {
    #[n(0)]
    pub seg: SegDigest,
    #[n(1)]
    pub identity: String,
    #[n(2)]
    pub epoch: u64,
}

/// Body of `g-*`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode)]
pub struct GcRecord {
    #[n(0)]
    pub epoch: u64,
    /// One compacted segment per root.
    #[n(1)]
    pub roots: Vec<RootRow>,
    /// Who introduced which segment, for revocation.
    #[n(2)]
    pub origin: Vec<Origin>,
    /// Segments this run replaced. The next run deletes them.
    #[n(3)]
    pub retired: Vec<SegDigest>,
    /// Heads this run consumed. This run's sweep deletes them.
    #[n(4)]
    pub folded: Vec<String>,
    #[n(5)]
    pub orphan_cursor: Option<String>,
    /// Unix time written.
    #[n(6)]
    pub time: u64,
}

/// Body of `c-*`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct CompactionRecord {
    #[n(0)]
    pub root: String,
    #[n(1)]
    pub added: SegDigest,
    #[n(2)]
    pub replaces: Vec<SegDigest>,
    #[n(3)]
    pub subsumes: Vec<String>,
    #[n(4)]
    pub time: u64,
}

const MAX_RECORD_BYTES: usize = 64 << 20;

fn decode_record<'a, T: Decode<'a, ()>>(bytes: &'a [u8]) -> Result<T, Error> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::Format("record exceeds cap".into()));
    }
    Ok(minicbor::decode(bytes)?)
}

impl GcRecord {
    pub fn encode(&self) -> Vec<u8> {
        minicbor::to_vec(self).expect("Vec write")
    }
    pub fn decode(bytes: &[u8]) -> Result<GcRecord, Error> {
        let r: GcRecord = decode_record(bytes)?;
        if r.roots.windows(2).any(|w| w[0].name >= w[1].name) {
            return Err(Error::Format("roots not sorted/unique".into()));
        }
        Ok(r)
    }
    pub fn head_name(&self) -> HeadName {
        HeadName::Gc {
            epoch: self.epoch,
            time: self.time,
            digest: SegDigest::digest(self.encode()),
        }
    }
}

impl CompactionRecord {
    pub fn encode(&self) -> Vec<u8> {
        minicbor::to_vec(self).expect("Vec write")
    }
    pub fn decode(bytes: &[u8]) -> Result<CompactionRecord, Error> {
        decode_record(bytes)
    }
    pub fn head_name(&self, base_epoch: u64) -> HeadName {
        HeadName::Compaction {
            base_epoch,
            root: root_id(&self.root),
            time: self.time,
            digest: SegDigest::digest(self.encode()),
        }
    }
}

// ---------------------------------------------------------------- view

/// `g-*` names at the highest epoch. More than one means a clobbered tag or
/// a re-run after eviction. The caller keeps the one whose body matches its digest.
pub fn newest_gc<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    let gcs: Vec<(u64, &str)> = names
        .into_iter()
        .filter_map(|n| match HeadName::parse(n)? {
            HeadName::Gc { epoch, .. } => Some((epoch, n)),
            _ => None,
        })
        .collect();
    let max = gcs.iter().map(|(e, _)| *e).max();
    gcs.into_iter()
        .filter(|(e, _)| Some(*e) == max)
        .map(|(_, n)| n)
        .collect()
}

/// Writer heads GC has not compacted yet: based on this epoch or the one
/// before (published while GC ran), and not already folded.
fn pending<'a>(
    names: impl IntoIterator<Item = &'a str>,
    gc: Option<&GcRecord>,
) -> Vec<(&'a str, HeadName)> {
    let epoch = gc.map_or(0, |g| g.epoch);
    let folded: BTreeSet<&str> = gc
        .into_iter()
        .flat_map(|g| g.folded.iter().map(String::as_str))
        .collect();
    let mut out: Vec<(&str, HeadName)> = names
        .into_iter()
        .filter(|n| !folded.contains(n))
        .filter_map(|n| Some((n, HeadName::parse(n)?)))
        .filter(|(_, h)| match h {
            HeadName::Gc { .. } => false,
            HeadName::Drain { base_epoch, .. } | HeadName::Compaction { base_epoch, .. } => {
                *base_epoch <= epoch && *base_epoch + 1 >= epoch
            }
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(b.0));
    out.dedup_by(|a, b| a.0 == b.0);
    out
}

/// `c-*` names whose bodies [`View::compute`] needs.
pub fn compactions_to_fetch<'a>(
    names: impl IntoIterator<Item = &'a str>,
    gc: Option<&GcRecord>,
) -> Vec<&'a str> {
    pending(names, gc)
        .into_iter()
        .filter(|(_, h)| matches!(h, HeadName::Compaction { .. }))
        .map(|(n, _)| n)
        .collect()
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct View {
    pub epoch: u64,
    /// Live segments per root, GC's segment first, then in name order.
    pub roots: BTreeMap<String, Vec<SegDigest>>,
    /// Writer heads that contributed and the segment each added:
    /// `subsumes`/`replaces` for a compaction, `folded` for GC.
    pub heads: Vec<(String, SegDigest)>,
}

impl View {
    /// `compactions` holds the fetched bodies of [`compactions_to_fetch`]. A
    /// name missing there (fetch failed, proof rejected) counts as not listed.
    pub fn compute<'a>(
        names: impl IntoIterator<Item = &'a str>,
        gc: Option<&GcRecord>,
        compactions: &HashMap<String, CompactionRecord>,
    ) -> View {
        let mut heads = pending(names, gc);
        heads.retain(|(n, h)| match h {
            HeadName::Compaction { root, .. } => compactions
                .get(*n)
                .is_some_and(|c| root_id(&c.root) == *root),
            _ => true,
        });
        let subsumed: BTreeSet<&str> = heads
            .iter()
            .filter_map(|(n, _)| compactions.get(*n))
            .flat_map(|c| c.subsumes.iter().map(String::as_str))
            .collect();
        heads.retain(|(n, _)| !subsumed.contains(n));

        let gc_roots = gc.map_or(&[][..], |g| &g.roots);
        let root_names: HashMap<RootId, &str> = gc_roots
            .iter()
            .map(|r| r.name.as_str())
            .chain(
                heads
                    .iter()
                    .filter_map(|(n, _)| compactions.get(*n))
                    .map(|c| c.root.as_str()),
            )
            .map(|name| (root_id(name), name))
            .collect();

        let mut roots: BTreeMap<String, Vec<SegDigest>> = gc_roots
            .iter()
            .map(|r| (r.name.clone(), vec![r.seg]))
            .collect();
        let mut replaced: BTreeSet<(&str, SegDigest)> = BTreeSet::new();
        let mut used = Vec::new();
        for (n, h) in &heads {
            let (root, seg) = match h {
                HeadName::Drain { root, seg, .. } => (*root, *seg),
                HeadName::Compaction { root, .. } => (*root, compactions[*n].added),
                HeadName::Gc { .. } => unreachable!(),
            };
            let Some(&name) = root_names.get(&root) else {
                continue;
            };
            roots.entry(name.to_owned()).or_default().push(seg);
            if let Some(c) = compactions.get(*n) {
                replaced.extend(c.replaces.iter().map(|s| (name, *s)));
            }
            used.push(((*n).to_owned(), seg));
        }
        for (name, segs) in &mut roots {
            let mut seen = BTreeSet::new();
            segs.retain(|s| !replaced.contains(&(name.as_str(), *s)) && seen.insert(*s));
        }
        roots.retain(|_, segs| !segs.is_empty());
        View {
            epoch: gc.map_or(0, |g| g.epoch),
            roots,
            heads: used,
        }
    }

    pub fn segments(&self) -> BTreeSet<SegDigest> {
        self.roots.values().flatten().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(b: u8) -> SegDigest {
        SegDigest([b; 32])
    }

    fn gc(epoch: u64, roots: &[(&str, u8)], folded: &[&str]) -> GcRecord {
        let mut roots: Vec<RootRow> = roots
            .iter()
            .map(|(n, s)| RootRow {
                name: n.to_string(),
                seg: d(*s),
                stamp: 0,
            })
            .collect();
        roots.sort_by(|a, b| a.name.cmp(&b.name));
        GcRecord {
            epoch,
            roots,
            folded: folded.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    fn drain(base: u64, root: &str, seg: u8) -> String {
        HeadName::Drain {
            base_epoch: base,
            root: root_id(root),
            time: 0,
            seg: d(seg),
        }
        .to_string()
    }

    fn crec(root: &str, added: u8, replaces: &[u8], subsumes: &[&str]) -> CompactionRecord {
        CompactionRecord {
            root: root.into(),
            added: d(added),
            replaces: replaces.iter().map(|b| d(*b)).collect(),
            subsumes: subsumes.iter().map(|s| s.to_string()).collect(),
            time: 0,
        }
    }

    fn segs(v: &View, root: &str) -> BTreeSet<SegDigest> {
        v.roots[root].iter().copied().collect()
    }

    #[test]
    fn names_roundtrip_and_reject_junk() {
        for h in [
            HeadName::Gc {
                epoch: 3,
                time: 0,
                digest: d(1),
            },
            HeadName::Drain {
                base_epoch: u64::MAX,
                root: 7,
                time: 0,
                seg: d(2),
            },
            HeadName::Compaction {
                base_epoch: 0,
                root: root_id("main-x86_64-linux"),
                time: 0,
                digest: d(3),
            },
        ] {
            let s = h.to_string();
            assert!(
                s.len() <= 128 && s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'),
                "{s}"
            );
            assert_eq!(HeadName::parse(&s), Some(h));
        }
        let ok = HeadName::Gc {
            epoch: 1,
            time: 0,
            digest: d(0xab),
        }
        .to_string();
        for junk in [
            "",
            "g-1-00",
            "x-0000000000000000-",
            "h-0000000000000000-0000000000000000-0000000000000000",
            &(ok.clone() + "-x"),
            &ok.to_uppercase(),
        ] {
            assert_eq!(HeadName::parse(junk), None, "{junk}");
        }
        // fixed width makes lexical = numeric order
        assert!(
            HeadName::Gc {
                epoch: 9,
                time: 0,
                digest: d(0xff)
            }
            .to_string()
                < HeadName::Gc {
                    epoch: 10,
                    time: 0,
                    digest: d(0)
                }
                .to_string()
        );
    }

    #[test]
    fn records_roundtrip_and_name_binds_body() {
        let g = gc(4, &[("main", 1), ("dev", 2)], &["h-x"]);
        assert_eq!(GcRecord::decode(&g.encode()).unwrap(), g);
        assert_eq!(
            g.head_name(),
            HeadName::Gc {
                epoch: 4,
                time: 0,
                digest: SegDigest::digest(g.encode())
            }
        );
        let mut unsorted = g.clone();
        unsorted.roots.swap(0, 1);
        assert!(GcRecord::decode(&unsorted.encode()).is_err());

        let c = crec("main", 5, &[1], &["h-y"]);
        assert_eq!(CompactionRecord::decode(&c.encode()).unwrap(), c);
    }

    #[test]
    fn empty_store() {
        assert_eq!(View::compute([], None, &HashMap::new()), View::default());
    }

    #[test]
    fn new_root_is_named_by_a_compaction() {
        let h = drain(0, "main", 1);
        assert!(
            View::compute([h.as_str()], None, &HashMap::new())
                .roots
                .is_empty()
        );

        let c = crec("main", 2, &[], &[]);
        let cn = c.head_name(0).to_string();
        let v = View::compute(
            [h.as_str(), cn.as_str()],
            None,
            &HashMap::from([(cn.clone(), c)]),
        );
        assert_eq!(segs(&v, "main"), BTreeSet::from([d(1), d(2)]));
    }

    #[test]
    fn view_rule() {
        let h_cur = drain(5, "main", 11);
        let h_prev = drain(4, "main", 12);
        let h_old = drain(3, "main", 13);
        let h_future = drain(6, "main", 14);
        let h_unknown_root = drain(5, "nope", 15);
        let h_dev = drain(5, "dev", 21);
        let h_folded = drain(5, "main", 16);
        let g = gc(5, &[("main", 10), ("dev", 20)], &[&h_folded]);

        // replaces base 10 and h_cur's 11, subsumes h_cur
        let c = crec("main", 30, &[10, 11], &[&h_cur]);
        let cn = c.head_name(5).to_string();
        // body not fetched: ignored entirely, so h_dev stays
        let c_missing = crec("dev", 31, &[20], &[&h_dev]);
        let cn_missing = c_missing.head_name(5).to_string();
        // name says main, body says dev: ignored
        let c_lie = crec("dev", 32, &[], &[]);
        let cn_lie = HeadName::Compaction {
            base_epoch: 5,
            root: root_id("main"),
            time: 0,
            digest: d(99),
        }
        .to_string();
        let gn = g.head_name().to_string();

        let names = [
            &h_cur,
            &h_prev,
            &h_old,
            &h_future,
            &h_unknown_root,
            &h_dev,
            &h_folded,
            &cn,
            &cn_missing,
            &cn_lie,
            &gn,
        ];
        let bodies = HashMap::from([(cn.clone(), c), (cn_lie.clone(), c_lie)]);
        let v = View::compute(
            names.iter().map(|s| s.as_str()).chain(["junk"]),
            Some(&g),
            &bodies,
        );

        assert_eq!(v.epoch, 5);
        assert_eq!(segs(&v, "main"), BTreeSet::from([d(12), d(30)]));
        assert_eq!(v.roots["dev"], vec![d(20), d(21)]);
        let used: BTreeSet<&str> = v.heads.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            used,
            BTreeSet::from([h_prev.as_str(), h_dev.as_str(), cn.as_str()])
        );
    }

    #[test]
    fn helpers_pick_what_to_fetch() {
        let c_folded = HeadName::Compaction {
            base_epoch: 5,
            root: 0,
            time: 0,
            digest: d(7),
        }
        .to_string();
        let g4 = gc(4, &[], &[]);
        let g5 = gc(5, &[], &[&c_folded]);
        let (n4, n5) = (g4.head_name().to_string(), g5.head_name().to_string());
        let n5_clobbered = HeadName::Gc {
            epoch: 5,
            time: 0,
            digest: d(0),
        }
        .to_string();
        let c_ok = HeadName::Compaction {
            base_epoch: 4,
            root: 0,
            time: 0,
            digest: d(1),
        }
        .to_string();
        let c_old = HeadName::Compaction {
            base_epoch: 3,
            root: 0,
            time: 0,
            digest: d(2),
        }
        .to_string();
        let names = [
            n4.as_str(),
            n5.as_str(),
            n5_clobbered.as_str(),
            c_ok.as_str(),
            c_old.as_str(),
            c_folded.as_str(),
        ];

        let newest: BTreeSet<&str> = newest_gc(names).into_iter().collect();
        assert_eq!(newest, BTreeSet::from([n5.as_str(), n5_clobbered.as_str()]));
        assert_eq!(compactions_to_fetch(names, Some(&g5)), vec![c_ok.as_str()]);
    }
}
