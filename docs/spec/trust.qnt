// -*- mode: Bluespec; -*-
//
// hestia trust model: what an authorised but malicious writer can do.
// Companion to segments.qnt (which has no identities and no adversary, but
// lag, crash and eviction). Here GC is atomic and there is no clock. What is
// shared with segments.qnt is the shape of the view rule: added − replaces
// over accepted heads, GC output as base.
//
//   quint run    docs/spec/trust.qnt --main trust_main --invariant safety
//   quint verify docs/spec/trust.qnt --main trust_main --invariant safety --max-steps 12 --backend tlc
//   quint run    docs/spec/trust.qnt --main trust_open        --invariant mainClean   # counterexamples
//   quint run    docs/spec/trust.qnt --main trust_same_repo   --invariant mainClean
//   quint run    docs/spec/trust.qnt --main trust_bad_aud      --invariant mainClean
//   quint run    docs/spec/trust.qnt --main trust_bad_replaces --invariant noHide
//
// Proofs are abstract: { repo, ref, digest } stands for an OIDC token or a key signature alike.
// Actors: `honest` builds on refs/heads/main and writes root "main" (protected).
//         `contrib` builds a PR and writes root "pr".
//         `mal` holds packages:write from refs/heads/feature (root "feat"). It may put any
//         well-typed object, claim any root, attach no proof, its own proof, or
//         replay a proof it has seen. It cannot forge a signature or collide ids.
//         `gc` runs from the default branch.

module trust {
  /// "strict": a root accepts heads proven for its own ref. "same_repo": any proven ref of this
  /// repository. "open": anything, proof optional.
  const POLICY: str
  /// the adversary pushes from a branch of this repo (true) or holds a token of another repo (false)
  const MAL_SAME_REPO: bool
  const BIND_AUD: bool           // a proof is only valid on the record it was issued for
  const PER_ROOT_REPLACES: bool  // replaces only affects segments of the head's own root
  const MAX_ID: int

  type Entry = { path: int, nar: str }                 // nar: "good" | "bad"
  type Seg   = { root: str, entries: Set[Entry] }
  type Proof = { some: bool, repo: str, ref: str, digest: int }
  type Head  = { root: str, added: Set[int], replaces: Set[int], proof: Proof, gc: bool }

  type State = {
    segs: int -> Seg,
    heads: int -> Head,
    next_id: int,
    // ground truth for the invariants
    committed: Set[(str, int)],     // (root, path) published by honest/contrib
  }
  var s: State

  val PATHS = Set(1)
  val ROOTS = Set("main", "pr", "feat")
  val HONEST_ROOTS = Set("main", "pr")
  pure def refOf(root: str): str =
    if (root == "main") "refs/heads/main" else if (root == "pr") "refs/pull/7/merge" else "refs/heads/feature"
  val GC_REF = "gc-workflow@refs/heads/main"
  val NoProof: Proof = { some: false, repo: "", ref: "", digest: 0 }
  val REPO = "owner/repo"

  def room(n: int): bool = s.next_id + n <= MAX_ID + 1

  // ---------------------------------------------------------------- policy

  def accepted(hid: int): bool = {
    val h = s.heads.get(hid)
    val pr = h.proof
    val bound = pr.some and pr.repo == REPO and (pr.digest == hid or not(BIND_AUD))
    if (POLICY == "open") true
    else if (POLICY == "same_repo") bound
    else bound and (pr.ref == refOf(h.root) or pr.ref == GC_REF)
  }

  /// Segments a reader of `root` uses.
  def view(root: str): Set[int] = {
    val hs = s.heads.keys().filter(k => accepted(k) and s.heads.get(k).root == root)
    val added = hs.map(k => s.heads.get(k).added).flatten().filter(x => s.segs.keys().contains(x) and s.segs.get(x).root == root)
    val repl = if (PER_ROOT_REPLACES) hs.map(k => s.heads.get(k).replaces).flatten()
               else s.heads.keys().filter(k => accepted(k)).map(k => s.heads.get(k).replaces).flatten()
    added.exclude(repl)
  }
  def entriesOf(root: str): Set[Entry] = view(root).map(x => s.segs.get(x).entries).flatten()

  // --------------------------------------------------------------- actions

  action init = s' = { segs: Map(), heads: Map(), next_id: 1, committed: Set() }

  def publish(st: State, root: str, entries: Set[Entry], replaces: Set[int], proof: Proof, isGc: bool): State = {
    val sid = st.next_id
    val hid = st.next_id + 1
    val pr  = if (proof.some and proof.digest == 0) { ...proof, digest: hid } else proof   // digest 0 = "sign this record"
    { ...st, next_id: st.next_id + 2,
      segs: st.segs.put(sid, { root: root, entries: entries }),
      heads: st.heads.put(hid, { root: root, added: Set(sid), replaces: replaces, proof: pr, gc: isGc }) }
  }

  /// Honest drain or writer-side compaction on its own root with a proof for its own ref.
  action honestWrite(root: str): bool = all {
    room(2),
    nondet p = PATHS.oneOf()
    nondet compact = Set(false, true).oneOf()
    val mine = view(root)
    val entries = if (compact) entriesOf(root).union(Set({ path: p, nar: "good" })) else Set({ path: p, nar: "good" })
    val st = publish(s, root, entries, if (compact) mine else Set(), { some: true, repo: REPO, ref: refOf(root), digest: 0 }, false)
    s' = { ...st, committed: s.committed.union(Set((root, p))) },
  }

  /// The adversary: any root, any entries, any replaces over existing segments, any obtainable proof.
  action malWrite = all {
    room(2),
    nondet root = ROOTS.oneOf()
    nondet p = PATHS.oneOf()
    nondet withRepl = Set(false, true).oneOf()
    val repl = if (withRepl) s.segs.keys() else Set()
    nondet proofKind = Set("none", "own", "replay").oneOf()
    val seen = s.heads.keys().map(k => s.heads.get(k).proof).filter(pr => pr.some)
    nondet replayed = seen.union(Set(NoProof)).oneOf()
    val proof = if (proofKind == "none") NoProof
                else if (proofKind == "own") { some: true, repo: if (MAL_SAME_REPO) REPO else "fork/repo", ref: "refs/heads/feature", digest: 0 }
                else replayed
    s' = publish(s, root, Set({ path: p, nar: "bad" }), repl, proof, false),
  }

  /// GC: per root, fold the policy-filtered view into one segment, delete everything else.
  val liveRoots = ROOTS.filter(r => view(r) != Set())
  action gc = all {
    room(2 * liveRoots.size()), liveRoots != Set(),
    val st = liveRoots.fold(s, (acc, root) => publish(acc, root, entriesOf(root), Set(), { some: true, repo: REPO, ref: GC_REF, digest: 0 }, true))
    val newHeads = st.heads.keys().exclude(s.heads.keys())
    val newSegs  = st.segs.keys().exclude(s.segs.keys())
    s' = { ...st, heads: newHeads.mapBy(k => st.heads.get(k)), segs: newSegs.mapBy(k => st.segs.get(k)) },
  }

  action step = any { honestWrite("main"), honestWrite("pr"), malWrite, gc }

  // ------------------------------------------------------------ properties

  /// A root only ever serves what its own ref (or GC) published.
  val mainClean = HONEST_ROOTS.forall(r => entriesOf(r).forall(e => e.nar == "good"))

  /// The adversary cannot make honest paths disappear from someone else's root.
  val noHide = s.committed.forall(c => entriesOf(c._1).exists(e => e.path == c._2))

  val safety = all { mainClean, noHide }

  val witnessMalOnFeat = not(entriesOf("feat").exists(e => e.nar == "bad"))
}

/// strict: a same-repo branch pusher can touch nothing but its own root.
module trust_main          { import trust(POLICY = "strict",    MAL_SAME_REPO = true,  BIND_AUD = true,  PER_ROOT_REPLACES = true,  MAX_ID = 8).* }
/// same_repo: a fork / foreign token can touch nothing. (A same-repo pusher can: see trust_same_repo.)
module trust_same_repo_fork{ import trust(POLICY = "same_repo", MAL_SAME_REPO = false, BIND_AUD = true,  PER_ROOT_REPLACES = true,  MAX_ID = 8).* }
/// expected violations, documenting what each setting gives up
module trust_same_repo     { import trust(POLICY = "same_repo", MAL_SAME_REPO = true,  BIND_AUD = true,  PER_ROOT_REPLACES = true,  MAX_ID = 8).* }
module trust_open          { import trust(POLICY = "open",      MAL_SAME_REPO = false, BIND_AUD = true,  PER_ROOT_REPLACES = true,  MAX_ID = 8).* }
module trust_bad_aud       { import trust(POLICY = "strict",    MAL_SAME_REPO = true,  BIND_AUD = false, PER_ROOT_REPLACES = true,  MAX_ID = 8).* }
module trust_bad_replaces  { import trust(POLICY = "strict",    MAL_SAME_REPO = true,  BIND_AUD = true,  PER_ROOT_REPLACES = false, MAX_ID = 8).* }
