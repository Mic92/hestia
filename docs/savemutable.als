// Alloy 6 model of SaveMutable (src/gha/savemutable.rs). Check with:
//
//   alloy6 exec docs/savemutable.als
//
// SaveMutable fakes a mutable cache entry on top of write-once keys with
// an index sequence m#1, m#2, ...: load returns the newest visible
// version, a writer reserves the next index, uploads, finalizes. The
// model covers the parts the service makes hard: reservations are
// strongly consistent (two writers can never both reserve one index),
// but lookups are eventually consistent and non-monotonic -- a load may
// return ANY finalized version, or none at all. Writers crash at any
// point; a reservation whose writer died blocks its index forever, so
// the conflict loop skips an index after enough fruitless waiting
// (stale_skip_after).
//
// The model encodes the skip's justifying assumption directly: an index
// is only skipped if the writer holding it actually crashed. In code
// this is a timing argument -- stale_skip_after * retry_delay is far
// above both the lookup propagation lag and a healthy writer's
// finalize time.
//
// Writers keep the newest version they have ever seen as their merge
// base (the base ratchet): an eventually consistent load that regresses
// below it is ignored. Without the ratchet, a regressed load right
// after a stale-skip merges against an old version and silently drops a
// finalized version's contributions from every later version (NoLostUpdate
// finds the counterexample if `load` is changed to `w.base' = v`).
//
// Contents are abstracted to the set of contributors whose delta a
// version includes; merge = base's set plus the committing contributor.
// Two kinds of contributor: Writer atoms run the full protocol with all
// its interleavings; Env atoms commit atomically against the true newest
// version (a healthy writer that hit no anomaly) and exist only to grow
// the version chain cheaply -- the interesting races need a history of
// several committed versions, and modeling every historic writer's
// step-by-step run would blow up the trace length for no extra behavior.
// gc.als models what happens to the committed versions afterwards
// (eviction, GC deletes); this model stops at finalize.

module savemutable

open util/ordering[Idx] as ord

sig Idx {}

enum WPC { Idle, Loaded, Reserved, Done, Crashed }

abstract sig Contributor {}
// Background committers: atomic, always against the true newest version.
sig Env extends Contributor {}
sig Writer extends Contributor {
  var wpc: one WPC,
  var base: lone Idx,    // newest version folded into the pending merge
  var skipTo: lone Idx,  // indexes at or below are judged abandoned
  var resv: lone Idx     // index this writer holds a reservation on
}

var sig finalized in Idx {}
// An index is reserved iff a writer holds it or it is finalized (env
// commits reserve and finalize atomically; writers keep `resv` after
// finalizing or crashing). Derived, not stored: fewer solver variables.
fun reserved: set Idx { Writer.resv + finalized }
one sig S { var content: Idx -> Contributor }  // contributions per version

fact init {
  no finalized and no S.content
  Writer.wpc = Idle
  no Writer.base and no Writer.skipTo and no Writer.resv
}

// The index a writer tries to reserve: one past the newest version it
// has seen or skipped (base.max(skip_through) + 1).
fun target[w: Writer]: lone Idx {
  let seen = w.base + w.skipTo {
    no seen implies ord/first else ord/max[seen].(ord/next)
  }
}

// ---- frames -----------------------------------------------------------------
pred globalSame { finalized' = finalized and S.content' = S.content }
pred writerSame[w: Writer] {
  w.wpc' = w.wpc and w.base' = w.base and w.skipTo' = w.skipTo and w.resv' = w.resv
}
pred othersSame[w: Writer] { all u: Writer - w | writerSame[u] }

// ---- events -------------------------------------------------------------------

pred stutter { globalSame and all w: Writer | writerSame[w] }

// A healthy background writer commits: reserves the index right after
// the true newest version and finalizes in one step (no anomaly hit it).
// Each Env atom commits at most once.
pred envCommit[e: Env] {
  e not in Idx.(S.content)
  let t = no finalized implies ord/first else ord/max[finalized].(ord/next) {
    some t and t not in reserved
    finalized' = finalized + t
    S.content' = S.content + (t -> (ord/max[finalized].(S.content) + e))
  }
  all w: Writer | writerSame[w]
}

// Load + merge. The lookup is eventually consistent: it returns any
// finalized version, or a miss. The base ratchet keeps the newest
// version ever seen.
pred load[w: Writer, v: Idx] {
  w.wpc = Idle
  v in finalized
  w.base' = { i: v + w.base | all j: v + w.base | not ord/lt[i, j] } // max of both
  w.wpc' = Loaded
  w.skipTo' = w.skipTo and w.resv' = w.resv
  globalSame and othersSame[w]
}

// Lookup miss (nothing visible yet): no new information.
pred loadMiss[w: Writer] {
  w.wpc = Idle
  w.wpc' = Loaded
  w.base' = w.base and w.skipTo' = w.skipTo and w.resv' = w.resv
  globalSame and othersSame[w]
}

// Reservation succeeds: the index was free (strong consistency).
pred reserve[w: Writer] {
  w.wpc = Loaded
  some target[w] and target[w] not in reserved
  w.resv' = target[w] and w.wpc' = Reserved
  w.base' = w.base and w.skipTo' = w.skipTo
  globalSame and othersSame[w]
}

// Reservation conflicts (already_exists): wait and go around the loop.
pred conflict[w: Writer] {
  w.wpc = Loaded
  target[w] in reserved
  w.wpc' = Idle
  w.base' = w.base and w.skipTo' = w.skipTo and w.resv' = w.resv
  globalSame and othersSame[w]
}

// Stale-skip: the index has been blocked for stale_skip_after rounds.
// ASSUMPTION (timing, see header): this only ever fires for a
// reservation whose writer actually crashed before finalizing.
pred skip[w: Writer] {
  w.wpc = Loaded
  let t = target[w] {
    t in reserved - finalized
    some h: Writer | h.resv = t and h.wpc = Crashed
    w.skipTo' = t
  }
  w.wpc' = Idle
  w.base' = w.base and w.resv' = w.resv
  globalSame and othersSame[w]
}

// Upload + finalize: the version's contributions are the merge base's
// plus the writer's own delta.
pred finalize[w: Writer] {
  w.wpc = Reserved
  finalized' = finalized + w.resv
  S.content' = S.content + (w.resv -> (w.base.(S.content) + w))
  w.wpc' = Done
  w.base' = w.base and w.skipTo' = w.skipTo and w.resv' = w.resv
  othersSame[w]
}

// Crash anywhere before Done; a held reservation blocks its index forever.
pred crash[w: Writer] {
  w.wpc not in Done + Crashed
  w.wpc' = Crashed
  w.base' = w.base and w.skipTo' = w.skipTo and w.resv' = w.resv
  globalSame and othersSame[w]
}

fact behavior {
  always (
    stutter
    or (some e: Env | envCommit[e])
    or (some w: Writer, v: Idx | load[w, v])
    or (some w: Writer | loadMiss[w] or reserve[w] or conflict[w] or skip[w]
        or finalize[w] or crash[w])
  )
}

// ---- checks ---------------------------------------------------------------------

// Strong consistency of reservations: an index never has two holders.
assert OneHolderPerIndex {
  always (all i: Idx | lone resv.i)
}
check OneHolderPerIndex for 4 but 4 Idx, 2 Writer, 2 Env, 12 steps

// Write-once: a finalized version's contributions never change.
assert FinalizedImmutable {
  always (all i: finalized | i.(S.content') = i.(S.content))
}
check FinalizedImmutable for 4 but 4 Idx, 2 Writer, 2 Env, 12 steps

// No lost updates: a newly finalized version includes the contributions
// of every finalized version below it. This is the property gc.als
// builds on (a manifest commit never silently undoes an earlier one),
// and it depends on BOTH the skip assumption and the base ratchet.
assert NoLostUpdate {
  always (all i: Idx | i in finalized' - finalized implies
    (finalized & ord/prevs[i]).(S.content) in i.(S.content'))
}
check NoLostUpdate for 4 but 4 Idx, 2 Writer, 2 Env, 12 steps

// ---- sanity: the protocol can actually make progress ------------------------------

// All writers commit; the newest version carries everyone's contribution.
run allCommit {
  eventually (Writer.wpc = Done and ord/max[finalized].(S.content) = Contributor)
} for 4 but 4 Idx, 2 Writer, 2 Env, 12 steps

// A crashed writer's reservation gets skipped and the survivor still lands.
run skipRecovers {
  eventually (some w: Writer | w.wpc = Crashed and some w.resv)
  eventually (some w: Writer | some w.skipTo and w.wpc = Done)
} for 4 but 4 Idx, 2 Writer, 2 Env, 12 steps

// The lost-update shape stays reachable up to its last step: committed
// versions below a crashed reservation, the victim skips it and then a
// regressed lookup shows it an old version. The base ratchet is what
// keeps the subsequent merge complete.
run skipThenRegressedRead {
  eventually (some w: Writer, v: Idx {
    some w.skipTo
    load[w, v]
    ord/lt[v, ord/max[finalized]]
    eventually (w.wpc = Done)
  })
} for 4 but 4 Idx, 2 Writer, 2 Env, 12 steps
