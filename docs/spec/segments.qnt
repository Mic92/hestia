// -*- mode: Bluespec; -*-
//
// hestia segmented manifest: drains (optionally compacting their root),
// GC compaction + sweep, head listing lag, repack, GC crash,
// cache eviction (gha). Store is gha, OCI or S3: named immutable objects and a
// listing that may lag. One root. Packs are atomic. Ids stand in for hashes.
// The only clock is the GC epoch.
//
//   quint run    docs/spec/segments.qnt --main segments_main  --invariant safety --max-steps 50 --max-samples 20000
//   quint verify docs/spec/segments.qnt --main segments_main  --invariant safety --max-steps 30 --backend tlc
//   quint run    docs/spec/segments.qnt --main segments_bad_span --invariant safety   # counterexample
//
// A loaded drain doubles as reader: its view must stay fetchable while it is in time.
// Assumptions (segments_bad_span breaks A1):
//   A1  a drain publishes, visibly, under a view at most MAX_JOB_SPAN epochs old: it re-reads
//       the GC head before publishing and refreshes its view if GC ran twice since. What remains
//       assumed is that two GC runs do not fit between that refresh and the head PUT.
//   A2  GC load sees every published head.

module segments {
  const WRITERS: Set[str]
  const MAX_JOB_SPAN: int
  const MAX_ID: int
  const MAX_EVICT: int

  // ---------------------------------------------------------------- types

  type Seg   = { paths: Set[int], packs: Set[int] }
  type GHead = { epoch: int, segs: Set[int], retired: Set[int], folded: Set[int] }
  type WHead = { base: int, added: Set[int], replaces: Set[int], visible: bool }
  type Store = { packs: Set[int], segs: int -> Seg, gheads: int -> GHead, wheads: int -> WHead }
  type View  = { epoch: int, ghead: int, wheads: Set[int], segs: Set[int], packs: Set[int], paths: Set[int] }

  /// idle -> loaded -> staged -> idle
  type Writer = { phase: str, view: View, path: int, pack: int, seg: int, compact: bool }
  /// idle -> loaded -> published -> sweptPacks -> sweptSegs -> idle. May crash to idle after publish.
  type Gc     = { phase: str, view: View, listed: Set[int], head: int, seg: int, newpack: int, crashed: bool }

  type State = {
    store: Store, writers: str -> Writer, gc: Gc, next_id: int,
    // observation only
    committed: Set[(int, int)], orphans: Set[int], evicted: Set[int],
  }

  var s: State

  // ---------------------------------------------------------------- views

  pure def without(m: int -> a, ks: Set[int]): int -> a = m.keys().exclude(ks).mapBy(k => m.get(k))

  /// Ids grow monotonically, so the newest GC head is the largest id.
  pure def maxId(ids: Set[int]): int = ids.fold(0, (a, b) => if (b > a) b else a)

  pure def packsOf(st: Store, segs: Set[int]): Set[int] =
    segs.filter(x => st.segs.keys().contains(x)).map(x => st.segs.get(x).packs).flatten()
  pure def pathsOf(st: Store, segs: Set[int]): Set[int] =
    segs.filter(x => st.segs.keys().contains(x)).map(x => st.segs.get(x).paths).flatten()

  /// Newest GC head + writer heads based on it or the epoch before, not yet folded.
  pure def viewOf(st: Store, onlyVisible: bool): View = {
    val gids = st.gheads.keys()
    val gh   = maxId(gids)
    val epoch  = if (gh == 0) 0 else st.gheads.get(gh).epoch
    val base = if (gh == 0) Set() else st.gheads.get(gh).segs
    val done = if (gh == 0) Set() else st.gheads.get(gh).folded
    val ws   = st.wheads.keys().filter(h => (st.wheads.get(h).visible or not(onlyVisible))
                                        and st.wheads.get(h).base >= epoch - 1 and not(done.contains(h)))
    val segs = base.union(ws.map(h => st.wheads.get(h).added).flatten())
                   .exclude(ws.map(h => st.wheads.get(h).replaces).flatten())
    { epoch: epoch, ghead: gh, wheads: ws, segs: segs, packs: packsOf(st, segs), paths: pathsOf(st, segs) }
  }

  val EmptyView: View = { epoch: 0, ghead: 0, wheads: Set(), segs: Set(), packs: Set(), paths: Set() }
  /// Idle actors are reset to these so equal situations are equal states.
  val IdleW: Writer = { phase: "idle", view: EmptyView, path: 0, pack: 0, seg: 0, compact: false }
  val IdleGc: Gc    = { phase: "idle", view: EmptyView, listed: Set(), head: 0, seg: 0, newpack: 0, crashed: false }

  val store = s.store
  val gv    = viewOf(s.store, false)
  val vv    = viewOf(s.store, true)
  def room(n: int): bool = s.next_id + n <= MAX_ID + 1
  def allVisible: bool   = store.wheads.keys().forall(h => store.wheads.get(h).visible)
  def curEpoch: int        = gv.epoch

  /// A1
  def inTime(base: int): bool =
    curEpoch - base < MAX_JOB_SPAN or (curEpoch - base == MAX_JOB_SPAN and s.gc.phase != "loaded")

  // ------------------------------------------------------------- actions

  action init = s' = {
    store:   { packs: Set(), segs: Map(), gheads: Map(), wheads: Map() },
    writers: WRITERS.mapBy(_ => IdleW),
    gc:      IdleGc,
    next_id: 1, committed: Set(), orphans: Set(), evicted: Set(),
  }

  // -- drain --

  action wLoad(w: str): bool = {
    val st = s.writers.get(w)
    all {
      st.phase == "idle", room(1),
      // writers are symmetric and stateless when idle: fill names in order
      w == "a" or s.writers.get("a").phase != "idle",
      s' = { ...s, next_id: s.next_id + 1,
             writers: s.writers.set(w, { ...st, phase: "loaded", view: vv, path: s.next_id }) },
    }
  }

  /// Upload pack + segment. Local dedup may reference one existing pack.
  /// With `compact` the segment carries the whole view and replaces it.
  action wStage(w: str): bool = {
    val st   = s.writers.get(w)
    val pack = s.next_id
    val seg  = s.next_id + 1
    all {
      st.phase == "loaded", room(2),
      nondet r = st.view.packs.union(Set(0)).oneOf()
      nondet compact = Set(false, true).oneOf()
      val reuse = if (r == 0) Set() else Set(r)
      val body  = if (compact) { paths: st.view.paths.union(Set(st.path)), packs: st.view.packs.union(Set(pack)) }
                  else         { paths: Set(st.path), packs: reuse.union(Set(pack)) }
      s' = { ...s, next_id: s.next_id + 2,
             store: { ...store, packs: store.packs.union(Set(pack)), segs: store.segs.put(seg, body) },
             writers: s.writers.set(w, { ...st, phase: "staged", pack: pack, seg: seg, compact: compact }) },
    }
  }

  action wCommit(w: str): bool = {
    val st = s.writers.get(w)
    val h  = { base: st.view.epoch, added: Set(st.seg), replaces: if (st.compact) st.view.segs else Set(), visible: false }
    all {
      st.phase == "staged", room(1), inTime(st.view.epoch),
      s' = { ...s, next_id: s.next_id + 1,
             store: { ...store, wheads: store.wheads.put(s.next_id, h) },
             writers: s.writers.set(w, IdleW),
             committed: s.committed.union(Set((st.path, st.pack))) },
    }
  }

  /// View too old to publish under: reload it and redo dedup/staging.
  /// Anything already staged against the old view becomes an orphan.
  action wRefresh(w: str): bool = {
    val st = s.writers.get(w)
    all {
      st.phase != "idle", not(inTime(st.view.epoch)),
      s' = { ...s, writers: s.writers.set(w, { ...st, phase: "loaded", view: vv, pack: 0, seg: 0 }),
             orphans: s.orphans.union(if (st.phase == "staged") Set(st.pack, st.seg) else Set()) },
    }
  }

  action becomeVisible = {
    val hidden = store.wheads.keys().filter(h => not(store.wheads.get(h).visible))
    all {
      hidden != Set(),
      nondet h = hidden.oneOf()
      s' = { ...s, store: { ...store, wheads: store.wheads.set(h, { ...store.wheads.get(h), visible: true }) } },
    }
  }

  // -- gc --

  action gLoad = all {
    s.gc.phase == "idle", allVisible,   // A2
    s' = { ...s, gc: { ...s.gc, phase: "loaded", view: gv, listed: store.wheads.keys() } },
  }

  /// Compact the view into one segment (optionally repacking one pack),
  /// retire every input segment, fold every head listed (also those an older,
  /// crashed run already folded but never deleted, and those too old for the view).
  action gPublish = {
    val v   = s.gc.view
    val seg = s.next_id
    val hid = s.next_id + 1
    val inputs = v.segs.union(v.wheads.filter(h => store.wheads.keys().contains(h)).map(h => store.wheads.get(h).replaces).flatten())
                       .union(if (store.gheads.keys().contains(v.ghead)) store.gheads.get(v.ghead).segs else Set())
    all {
      s.gc.phase == "loaded", room(3),
      nondet r = v.packs.union(Set(0)).oneOf()
      val newpack  = if (r == 0) 0 else s.next_id + 2
      val segPacks = if (r == 0) v.packs else v.packs.exclude(Set(r)).union(Set(newpack))
      val gh = { epoch: v.epoch + 1, segs: Set(seg), retired: inputs,
                   folded: s.gc.listed.union(store.gheads.keys()) }
      s' = { ...s, next_id: s.next_id + 3,
             store: { ...store,
               packs:  if (r == 0) store.packs else store.packs.union(Set(newpack)),
               segs:   store.segs.put(seg, { paths: v.paths, packs: segPacks }),
               gheads: store.gheads.put(hid, gh) },
             gc: { ...s.gc, phase: "published", head: hid, seg: seg, newpack: newpack } },
    }
  }

  /// Segments retired by any earlier epoch are dead now. Normally that
  /// is just the previous head. After a crashed run older heads are still
  /// around and their lists get swept too.
  val gcOlder: Set[int] = store.gheads.keys().filter(h => h != s.gc.head)
  val gcPrev: int = maxId(gcOlder)
  val gcDead: Set[int] = gcOlder.map(h => store.gheads.get(h).retired).flatten()
  val gcMine: GHead = store.gheads.get(s.gc.head)

  /// Dead packs = packs of dead segments not named by anything current or
  /// just-retired (readers and in-flight drains may still hold those).
  action gSweepPacks = {
    val protected = packsOf(store, gcMine.segs.union(gcMine.retired).union(vv.segs))
    val blind     = gcDead.exists(x => not(store.segs.keys().contains(x)) and not(s.evicted.contains(x)))
    all {
      s.gc.phase == "published", store.gheads.keys().contains(s.gc.head),
      s' = { ...s,
        store: { ...store, packs: store.packs.exclude(packsOf(store, gcDead).exclude(protected)) },
        gc: { ...s.gc, phase: "sweptPacks" } },
    }
  }
  action gSweepSegs = all {
    s.gc.phase == "sweptPacks",
    s' = { ...s, store: { ...store, segs: without(store.segs, gcDead) }, gc: { ...s.gc, phase: "sweptSegs" } },
  }
  action gSweepHeads = all {
    s.gc.phase == "sweptSegs", store.gheads.keys().contains(s.gc.head),
    s' = { ...s,
      store: { ...store, wheads: without(store.wheads, gcMine.folded), gheads: without(store.gheads, gcMine.folded.exclude(Set(gcPrev))) },
      gc: IdleGc },
  }
  /// GC dies after publishing (or its head got evicted under it). Next run re-plans.
  action gCrash = all {
    Set("published", "sweptPacks", "sweptSegs").contains(s.gc.phase),
    s' = { ...s, gc: { ...IdleGc, crashed: true } },
  }

  // -- environment (gha) --

  action evict = {
    val objs = store.packs.union(store.segs.keys()).union(store.gheads.keys()).union(store.wheads.keys())
    all {
      s.evicted.size() < MAX_EVICT, objs != Set(),
      nondet o = objs.oneOf()
      s' = { ...s, evicted: s.evicted.union(Set(o)),
             store: { packs: store.packs.exclude(Set(o)), segs: without(store.segs, Set(o)),
                      gheads: without(store.gheads, Set(o)), wheads: without(store.wheads, Set(o)) } },
    }
  }

  action step = any {
    nondet w = WRITERS.oneOf()
    any { wLoad(w), wStage(w), wCommit(w), wRefresh(w) },
    becomeVisible,
    gLoad, gPublish, gSweepPacks, gSweepSegs, gSweepHeads, gCrash,
    evict,
  }

  // ---------------------------------------------------------- properties

  val pristine = s.evicted == Set()
  def present(x: int): bool = store.segs.keys().contains(x) or store.packs.contains(x) or s.evicted.contains(x)

  /// Whatever the merged view names exists, unless the cache service took it.
  val integrity = gv.segs.forall(x => present(x)) and gv.packs.forall(p => present(p))

  val noLostWrite = not(pristine) or s.committed.map(c => c._1).subseteq(gv.paths)

  /// An in-time drain (= a reader within one epoch) never holds a
  /// reference to something GC deleted. Eviction can regress the visible
  /// epoch, so only checked pristine.
  val inflightRefsExist = not(pristine) or WRITERS.forall(w => {
    val st = s.writers.get(w)
    st.phase == "idle" or not(inTime(st.view.epoch)) or (
      st.view.segs.forall(x => present(x)) and st.view.packs.forall(p => present(p))
      and (st.phase != "staged" or (present(st.pack) and present(st.seg))))
  })

  val core = all { integrity, noLostWrite, inflightRefsExist }

  val inflight = WRITERS.filter(w => s.writers.get(w).phase == "staged").map(w => Set(s.writers.get(w).seg, s.writers.get(w).pack)).flatten()
    .union(if (s.gc.phase != "idle" and s.gc.phase != "loaded") Set(s.gc.seg, s.gc.newpack) else Set())

  /// Every stored segment is live, retired (awaiting sweep), in flight, or a known orphan.
  val noLeak = not(pristine) or s.gc.crashed or {
    val named = store.gheads.keys().map(h => store.gheads.get(h).segs.union(store.gheads.get(h).retired)).flatten()
      .union(store.wheads.keys().map(h => store.wheads.get(h).added.union(store.wheads.get(h).replaces)).flatten())
    store.segs.keys().subseteq(named.union(inflight).union(s.orphans))
    and store.packs.subseteq(packsOf(store, store.segs.keys()).union(inflight).union(s.orphans))
  }

  val quiet = s.gc.phase == "idle" and not(s.gc.crashed) and pristine
    and WRITERS.forall(w => s.writers.get(w).phase == "idle") and store.wheads.keys() == Set() and curEpoch > 0

  /// After a clean GC with nothing in flight: live + last retired + orphans, nothing else.
  val gcTight = not(quiet) or {
    val gh = store.gheads.get(gv.ghead)
    val keep = gh.segs.union(gh.retired)
    store.segs.keys().exclude(s.orphans).subseteq(keep)
    and store.packs.exclude(s.orphans).subseteq(packsOf(store, keep))
    and store.gheads.keys().size() <= 2
  }

  val safety = all { core, noLeak, gcTight }

  val witnessQuiet       = not(quiet and s.committed.size() >= 2)
  val witnessRepackSwept = not(integrity and pristine and s.committed.exists(c => not(store.packs.contains(c._2))))
  val witnessCompacted   = not(store.wheads.keys().exists(h => store.wheads.get(h).replaces != Set()) and s.committed.size() >= 2)
}

/// The cache service may evict one arbitrary entry (gha). Traces without an
/// eviction cover a store that never loses objects (OCI, S3).
module segments_main {
  import segments(WRITERS = Set("a", "b"), MAX_JOB_SPAN = 1, MAX_ID = 13, MAX_EVICT = 1).*
}

/// S3 / OCI: no eviction, same rules. Strong listing consistency on S3 is a
/// subset of the lagged listings explored here, so no separate model.
module segments_s3 {
  import segments(WRITERS = Set("a", "b"), MAX_JOB_SPAN = 1, MAX_ID = 14, MAX_EVICT = 0).*
}

/// A1 violated: a drain publishes (or a reader fetches) under a view two epochs old.
module segments_bad_span {
  import segments(WRITERS = Set("a", "b"), MAX_JOB_SPAN = 2, MAX_ID = 14, MAX_EVICT = 0).*
}
