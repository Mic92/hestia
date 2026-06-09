// Alloy 6 model of `hestia gc` (src/gc.rs). Check with:
//
//   alloy6 exec docs/gc.als
//
// GC's danger zone is the delete step: everything else it does is
// additive or committed atomically, but a REST delete is forever, and it
// races whatever else is going on. So this model puts a full GC run next
// to the things that actually happen around one. A drain uploads a pack
// and later commits a manifest that merges its drain-start snapshot back
// in, which quietly resurrects anything GC dropped in between. An upload
// of content that already exists is a CAS no-op, so a drain can end up
// depending on a pack it never transferred. GitHub can evict any cache
// entry whenever it likes, including the newest manifest version (the
// head then falls back to m#N-1). GC itself can crash between any two
// steps and re-plan from scratch, its plan can be stale by the time it
// commits, and a repack source can vanish mid-run or produce an output
// identical to a pack that already exists.
//
// Two guards keep the delete steps safe. A pack is only deleted if no
// surviving manifest version references it (`verRefs`) -- a drain can
// only resurrect what its snapshot version knew, and that version sticks
// around long enough to protect it. A pack is also spared while its entry
// was created or accessed within min_age (`fresh`), because a recent
// touch means some drain depends on it; that is why upload_pack touches
// the existing entry on a no-op. Both filters run again at delete time,
// since the world can change between commit and delete.
//
// One assumption carries the whole thing (the crossed1/crossed2
// machinery): a drain never spans two GC commits. In practice min_age
// outlives any drain and CI kills jobs long before two nightly runs; a
// drain that stalls past the horizon simply loses -- its commit never
// lands.
//
// Two losses are accepted rather than flagged as bugs. If a manifest
// version is destroyed, the packs only it referenced are genuinely
// garbage -- there is no metadata left that could ever use them. And the
// model assumes LRU eviction prefers old entries, so it does not chase
// the case of GitHub evicting a minutes-old version from under a running
// drain.
//
// Not modeled: SaveMutable's reservation mechanics (commits are an atomic
// CAS here), the ManifestLookupInconsistent guard, plain LRU touches
// (eviction is already fully nondeterministic), overlapping GC runs (the
// gc.yml concurrency group prevents them), and chunk integrity checks
// (corrupt frames abort the run). There are no clocks either: grace/TTL
// expiry becomes "an unrooted path MAY be dropped", which
// over-approximates every clock policy.

module gc

enum GPC { GIdle, Planned, Repacked, Committed, PacksDeleted }
enum PPC { PIdle, PUpNew, PUpDedup }   // PUpDedup: upload was a CAS no-op

sig Pack {}
sig Chunk {}
sig Path { chunks: set Chunk }

// ---- GitHub cache ----------------------------------------------------------
var sig stored in Pack {}     // pack-* entries GitHub currently stores
var sig fresh in Pack {}      // created or accessed within min_age
var sig gcDeleted in Pack {}  // ghost: removed by GC (not by GitHub eviction)

// ---- committed manifest (the SaveMutable head) -------------------------------
var sig mPaths in Path {}     // path table
var sig roots in Path {}      // GC roots (flattened to the paths they mark)
one sig M {
  var loc: Chunk -> lone Pack,  // chunk table
  var packs: set Pack           // pack table
}

// Packs referenced by any surviving manifest version. Commits add the new
// version's references; cleanup (end of a GC run) discards versions that
// aged out, keeping the head and any version an in-flight drain snapshot
// still needs (younger than min_age by the crossing assumption).
var sig verRefs in Pack {}

// ---- previous head (m#N-1), the target of a head regression -------------------
var sig hPaths in Path {}
var sig hRoots in Path {}
one sig H {
  var hLoc: Chunk -> lone Pack,
  var hPacks: set Pack
}

// ---- the GC run ---------------------------------------------------------------
one sig GC {
  var pc: one GPC,
  var planOrphans: set Pack,  // plan-time: stored, aged, in no surviving version
  var replaced: set Pack,     // repack source packs
  var copied: set Chunk,      // chunks copied into the output pack
  var news: lone Pack,        // repack output pack
  var deletable: set Pack     // computed at commit
}

// ---- one concurrent push (drain) ----------------------------------------------
one sig Push {
  var ppc: one PPC,
  var target: lone Path,  // path being pushed
  var up: lone Pack,      // pack the drain depends on for target's chunks
  // manifest snapshot from drain start; the commit merges it back in
  // (pipeline.rs: base.merge(current).merge(delta))
  var snapPaths: set Path,
  var snapRoots: set Path,
  var snapPacks: set Pack,
  var snapLoc: Chunk -> lone Pack
}
// GC commits the in-flight drain has lived through (assumption: < 2)
var sig crossed1 in Push {}
var sig crossed2 in Push {}

fact init {
  no stored + fresh + gcDeleted + verRefs
  no mPaths + roots + hPaths + hRoots + Push.snapPaths + Push.snapRoots
  no M.loc + H.hLoc + Push.snapLoc
  no M.packs + H.hPacks + Push.snapPacks
  no GC.planOrphans + GC.replaced + GC.news + GC.deletable and no GC.copied
  no Push.target + Push.up and no crossed1 + crossed2
  GC.pc = GIdle and Push.ppc = PIdle
}

// ---- frame helpers --------------------------------------------------------------
pred storeSame { stored' = stored and fresh' = fresh and gcDeleted' = gcDeleted }
pred manifestSame {
  mPaths' = mPaths and roots' = roots and M.loc' = M.loc and M.packs' = M.packs
}
pred gcVarsSame {
  GC.planOrphans' = GC.planOrphans and GC.replaced' = GC.replaced
  GC.copied' = GC.copied and GC.news' = GC.news and GC.deletable' = GC.deletable
}
pred gcSame { GC.pc' = GC.pc and gcVarsSame }
pred headSame {
  hPaths' = hPaths and hRoots' = hRoots
  H.hLoc' = H.hLoc and H.hPacks' = H.hPacks
}
// plan-time snapshots survive the repack and commit steps
pred planVarsSame {
  GC.planOrphans' = GC.planOrphans and GC.replaced' = GC.replaced
}
// a commit supersedes the head: the pre-state head becomes m#N-1, and the
// new version's references join the surviving set
pred snapHead {
  hPaths' = mPaths and hRoots' = roots
  H.hLoc' = M.loc and H.hPacks' = M.packs
  verRefs' = verRefs + Chunk.(M.loc')
}
pred histSame { headSame and verRefs' = verRefs }
pred crossedSame { crossed1' = crossed1 and crossed2' = crossed2 }
pred pushSame {
  Push.ppc' = Push.ppc and Push.target' = Push.target and Push.up' = Push.up
  Push.snapPaths' = Push.snapPaths and Push.snapRoots' = Push.snapRoots
  Push.snapPacks' = Push.snapPacks and Push.snapLoc' = Push.snapLoc
}
// GC forgets its run state (orphan sweep done, or crash)
pred gcReset {
  GC.pc' = GIdle
  no GC.planOrphans' + GC.replaced' + GC.news' + GC.deletable' and no GC.copied'
}

// ---- environment ------------------------------------------------------------------

pred stutter { storeSame and manifestSame and gcSame and pushSame and histSame and crossedSame }

// GitHub evicts any entry at any time -- even an in-flight push's upload.
pred evict[k: Pack] {
  k in stored
  stored' = stored - k and fresh' = fresh - k and gcDeleted' = gcDeleted
  manifestSame and gcSame and pushSame and histSame and crossedSame
}

// The head regresses to m#N-1: GitHub evicted the newest version. verRefs
// is kept (LRU eviction assumption above).
pred evictHead {
  mPaths != hPaths or roots != hRoots or M.loc != H.hLoc or M.packs != H.hPacks
  mPaths' = hPaths and roots' = hRoots
  M.loc' = H.hLoc and M.packs' = H.hPacks
  histSame and storeSame and gcSame and pushSame and crossedSame
}

// A pack ages out of min_age. ASSUMPTION (min_age = 1h): a drain finishes
// within min_age, so the pack it depends on never ages out under it.
pred age[k: Pack] {
  k in fresh
  Push.ppc in PUpNew + PUpDedup implies k != Push.up
  fresh' = fresh - k
  stored' = stored and gcDeleted' = gcDeleted
  manifestSame and gcSame and pushSame and histSame and crossedSame
}

// ---- push (drain) -------------------------------------------------------------------

// Drain start: snapshot the manifest, target a path, depend on pack k --
// either uploaded fresh, or a CAS no-op (the identical pack already
// exists / every chunk dedups against the snapshot). The no-op branch
// TOUCHES the pack (1-byte read resets its LRU clock and makes it recent),
// exactly like GC's repack does on already_exists.
pred pushUpload[p: Path, k: Pack] {
  Push.ppc = PIdle
  k in stored implies {
    Push.ppc' = PUpDedup
    stored' = stored and fresh' = fresh + k and gcDeleted' = gcDeleted
  } else {
    Push.ppc' = PUpNew
    stored' = stored + k and fresh' = fresh + k and gcDeleted' = gcDeleted - k
  }
  Push.target' = p and Push.up' = k
  Push.snapPaths' = mPaths and Push.snapRoots' = roots
  Push.snapPacks' = M.packs and Push.snapLoc' = M.loc
  no crossed1' + crossed2'
  manifestSame and gcSame and histSame
}

// Drain commit: SaveMutable merge of (latest manifest, snapshot, delta).
// The unions resurrect anything GC dropped since the snapshot. The commit
// goes through even if `up` was meanwhile evicted -- the drain cannot know
// (dangling refs are healed by the next GC run). A drain that lived
// through two GC commits is assumed dead (CI timeout); its commit never
// lands.
pred pushCommit {
  Push.ppc in PUpNew + PUpDedup
  no crossed2
  mPaths' = mPaths + Push.snapPaths + Push.target
  M.packs' = M.packs + Push.snapPacks + Push.up
  // ChunkLocation::merge picks one side deterministically; the model
  // allows any choice (over-approximation)
  let cand = M.loc + Push.snapLoc + (Push.target.chunks -> Push.up) {
    M.loc' in cand
    all c: Chunk | some c.cand iff some c.(M.loc')
  }
  // Root::merge: same-run roots union, newer replaces older
  Push.target in roots' and roots' in roots + Push.snapRoots + Push.target
  Push.ppc' = PIdle and no Push.target' + Push.up'
  no Push.snapPaths' + Push.snapRoots' + Push.snapPacks' and no Push.snapLoc'
  no crossed1' + crossed2'
  storeSame and gcSame and snapHead
}

// ---- GC steps (gc.rs flow: plan, repack, commit, delete packs, orphans) --------------

pred gcPlan {
  GC.pc = GIdle
  // orphans: in GitHub, aged out of min_age, and referenced by no
  // surviving manifest version
  GC.planOrphans' = stored - fresh - M.packs - verRefs
  // repack sources: any subset of referenced packs (liveness/consolidation
  // policy over-approximated as free choice)
  GC.replaced' in M.packs
  GC.copied' = M.loc.(GC.replaced')
  no GC.news' and no GC.deletable'
  GC.pc' = Planned
  storeSame and manifestSame and pushSame and histSame and crossedSame
}

// Range-copy live chunks into one new pack and upload it. Sources that
// vanished since planning are skipped (copied shrinks).
pred gcRepack {
  GC.pc = Planned
  GC.copied' in GC.copied
  some GC.copied' implies {
    one GC.news'
    // fresh upload, or CAS no-op (output reproduces an existing pack,
    // which gets touched -> recent)
    (GC.news' not in stored and stored' = stored + GC.news'
      and fresh' = fresh + GC.news' and gcDeleted' = gcDeleted - GC.news')
    or
    (GC.news' in stored and stored' = stored
      and fresh' = fresh + GC.news' and gcDeleted' = gcDeleted)
  } else {
    no GC.news' and storeSame
  }
  GC.pc' = Repacked
  planVarsSame and no GC.deletable'
  manifestSame and pushSame and histSame and crossedSame
}

// Commit: re-plan against the LATEST manifest and re-listing (gc.rs commit
// re-lists and re-plans inside the SaveMutable merge closure), apply the
// repack output, prune, compute deletable. Deletable excludes packs any
// surviving version references and recent packs; both filters are applied
// again at delete time.
pred gcCommit {
  GC.pc = Repacked
  let dead = M.packs - stored,   // ① reconcile: packs GitHub evicted
      loc1 = M.loc - (Chunk -> dead),
      // relocate only chunks still located in a replaced pack
      loc2 = loc1 ++ ({ c: GC.copied | c.loc1 in GC.replaced } -> GC.news),
      broken = { p: mPaths | some c: p.chunks | no c.loc2 } {
    roots' in roots              // ③ root TTL: any subset may expire
    // ① heal (mandatory) + ②③ mark/sweep: unrooted paths MAY drop
    broken in mPaths - mPaths'
    mPaths - mPaths' in broken + (mPaths - roots')
    mPaths' in mPaths
    // ⑤ prune chunks then packs by reference
    M.loc' = mPaths'.chunks <: loc2
    M.packs' = Chunk.(M.loc')
    GC.deletable' = (M.packs + GC.news) - verRefs' - fresh
  }
  GC.pc' = Committed
  planVarsSame and GC.copied' = GC.copied and GC.news' = GC.news
  // an in-flight drain has now lived through one more GC commit
  Push.ppc = PIdle implies no crossed1' + crossed2' else {
    crossed1' = Push and crossed2' = crossed1
  }
  pushSame
  storeSame and snapHead
}

// REST-delete packs no surviving manifest version references. Both
// protective filters re-checked against the CURRENT state: a re-upload
// (fresh) or a drain commit (verRefs) since gcCommit must win.
pred gcDeletePacks {
  GC.pc = Committed
  let victims = GC.deletable - verRefs - fresh {
    stored' = stored - victims
    fresh' = fresh - victims
    gcDeleted' = gcDeleted + (victims & stored)
  }
  GC.pc' = PacksDeleted
  gcVarsSame and manifestSame and pushSame and histSame and crossedSame
}

// REST-delete orphans (same delete-time re-checks), then cleanup: aged
// superseded manifest versions are deleted, so verRefs shrinks to the head
// plus whatever an in-flight drain's (young) snapshot still needs.
pred gcDeleteOrphans {
  GC.pc = PacksDeleted
  let victims = GC.planOrphans - M.packs - verRefs - fresh {
    stored' = stored - victims
    fresh' = fresh - victims
    gcDeleted' = gcDeleted + (victims & stored)
  }
  Push.ppc = PIdle implies verRefs' = Chunk.(M.loc)
    else verRefs' = Chunk.(M.loc) + Chunk.(Push.snapLoc)
  gcReset
  manifestSame and pushSame and headSame and crossedSame
}

// Crash between any two steps; uncommitted repack uploads become orphans
// for a later run.
pred gcCrash {
  GC.pc != GIdle
  gcReset
  storeSame and manifestSame and pushSame and histSame and crossedSame
}

fact behavior {
  always (
    stutter
    or (some k: Pack | evict[k])
    or evictHead
    or (some k: Pack | age[k])
    or (some p: Path, k: Pack | pushUpload[p, k])
    or pushCommit
    or gcPlan or gcRepack or gcCommit or gcDeletePacks or gcDeleteOrphans
    or gcCrash
  )
}

// ---- checks ---------------------------------------------------------------------------

// The manifest is internally closed -- every chunk location points at a
// pack its own pack table knows (eviction makes packs missing from GitHub,
// never dangling inside the manifest).
assert ManifestClosed { always Chunk.(M.loc) in M.packs }
check ManifestClosed for 4 but 3 Chunk, 2 Path, 12 steps

// A GC-deleted pack stays gone until the same content is re-uploaded.
assert DeletedNotStored { always no (gcDeleted & stored) }
check DeletedNotStored for 4 but 3 Chunk, 2 Path, 12 steps

// A drain's in-flight upload is never GC-deleted, even when GC's repack
// output CAS-collides with it or it is re-uploaded into a stale delete set.
assert InFlightUploadSafe {
  always (Push.ppc = PUpNew implies Push.up not in gcDeleted)
}
check InFlightUploadSafe for 4 but 3 Chunk, 2 Path, 12 steps

// A pack a drain CAS-deduped onto is never GC-deleted under it (the touch
// keeps it recent; surviving versions keep it referenced).
assert DedupTargetSafe {
  always (Push.ppc = PUpDedup implies Push.up not in gcDeleted)
}
check DedupTargetSafe for 4 but 3 Chunk, 2 Path, 12 steps

// GC delete steps never remove a pack the committed manifest head
// references at the moment of deletion -- including references a drain
// merge-commit resurrected in the commit->delete window.
assert DeleteRespectsCommit {
  always (gcDeletePacks implies
    no ((GC.deletable - verRefs - fresh) & Chunk.(M.loc)))
  always (gcDeleteOrphans implies
    no ((GC.planOrphans - M.packs - verRefs - fresh) & Chunk.(M.loc)))
}
check DeleteRespectsCommit for 4 but 3 Chunk, 2 Path, 12 steps

// ---- sanity: the interesting behaviors are reachable -----------------------------------

// Deferred deletion still deletes: a dead pack is gone once no surviving
// version references it (two GC cycles after losing its last reference).
run garbageStillCollected {
  eventually some mPaths
  eventually some gcDeleted
} for 4 but 3 Chunk, 2 Path, 18 steps

run repackHappens {
  eventually (some GC.news and GC.pc = Repacked)
} for 4 but 3 Chunk, 2 Path, 10 steps

run healAfterEviction {
  eventually (
    (some p: mPaths | some c: p.chunks | c.(M.loc) not in stored)
    and eventually no mPaths
  )
} for 4 but 3 Chunk, 2 Path, 14 steps

// A crash mid-run leaves an orphan that a later full run deletes.
run crashThenOrphanSweep {
  eventually (GC.pc = Repacked and some GC.news and after gcCrash)
  eventually (gcDeleteOrphans and some (GC.planOrphans - M.packs - verRefs - fresh))
} for 4 but 3 Chunk, 2 Path, 16 steps
