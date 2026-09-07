"""Concurrent indexing pipeline (temporary host infrastructure).

Architecture (mirrors docs/ARCHITECTURE.md; every stage here is a stand-in
for missing MNCS effects — pressure/PRESS-001..003):

    enumeration (single thread: walk, normalize, admit; discover.py)
        |
        v
    bounded read queue -> parallel file readers (bytes in hand; discover.py)
        |
        v
    plan pool (files -> window/token work items) --+
        |                                           | bounded queue
        v                                           | (backpressure real)
    kernel pool (ONE MNCS `execute` per item)  -----+
        |
        v
    deterministic assembly (ordered leaves -> MNCS tree combine,
                            carried `open` fix-up, term records)
        |
        v
    canonical merge (sort by the MNCS-specified rule)
        |
        v
    candidate snapshot (validated + published by store.py, never here)

Nothing here defines meaning: digests, classes, kinds, ordering, change
verdicts, and match predicates are MNCS kernel verdicts. The host moves
bytes, bounds queues, threads carries in canonical order, and sorts by
the specified key.
"""

from __future__ import annotations

import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from .discover import CorpusSnapshot
from .errors import BuildCancelled, BuildFailed
from .kernels import Kernels

__all__ = ["BuildConfig", "BuildFailed", "BuildCancelled", "FileResult", "Pipeline"]

WINDOW = 64
TOKEN_MAX = 32
TERMS_PER_FILE_CAP = 256

K_LEAF = "leaf"
K_TOKVAL = "tokval"
K_TOKDIG = "tokdig"


@dataclass
class BuildConfig:
    workers: int = 4
    queue_size: int = 64
    seed: int | None = None
    delay_ms: float = 0.0
    cancel_event: threading.Event | None = None
    # `workers` also bounds the parallel file-reader threads and
    # `queue_size` also bounds the discovery read queue: one shared host
    # budget across acquisition (discover.py) and kernel execution
    # (below), so a single --workers/--queue-size pair saturates both
    # stages together (temporary PRESS-backed sizing).
    # Compute the MNCS canonical fingerprint (one kernel call per 64
    # canonical bytes). Disable for config-sweep builds: byte-equality of
    # canonical output plus dedicated fingerprint tests carry the claim.
    mncs_digest: bool = True


@dataclass
class KernelItem:
    kind: str
    file_id: int
    seq: int = 0
    chunk: bytes = b""
    toks: tuple = ()


@dataclass
class FilePlan:
    file_id: int
    path: str
    data: bytes
    kind: int
    windows: list[bytes]
    tokens: list[bytes]  # distinct, sorted, capped, validated later


@dataclass
class FileResult:
    plan: FilePlan
    digest: int = 0
    words: int = 0
    lines: int = 0
    terms: list[tuple[str, int]] = field(default_factory=list)  # (token, tid)


class Pipeline:
    def __init__(self, kernels: Kernels, config: BuildConfig):
        self.k = kernels
        self.cfg = config
        self._errors: list[BaseException] = []
        self._err_lock = threading.Lock()
        self._stop = threading.Event()
        self._rng = (
            random.Random(config.seed) if config.seed is not None else random.Random()
        )
        self._rng_lock = threading.Lock()

    # -- public ----------------------------------------------------------
    def _plan_all(self, items: list[tuple[str, bytes]]) -> list[FilePlan]:
        try:
            return [
                self._plan_file(i, path, data) for i, (path, data) in enumerate(items)
            ]
        except (BuildCancelled, BuildFailed):
            raise
        except Exception as exc:
            raise BuildFailed(f"indexing failed during planning: {exc}") from exc

    def build_all(self, snap: CorpusSnapshot) -> list[FileResult]:
        """Full concurrent build of every discovered file."""
        order = list(snap.items)
        if self.cfg.seed is not None:
            rnd = random.Random(self.cfg.seed)
            rnd.shuffle(order)
        plans = self._plan_all([(it.path, it.data) for it in order])
        # File ids follow the (possibly shuffled) execution order; record
        # identity never uses them, only canonical (kind, path, seq) keys.
        return self._run(plans)

    def build_files(self, files: list[tuple[str, bytes]]) -> list[FileResult]:
        """Concurrent build of an explicit (path, data) list (incremental)."""
        return self._run(self._plan_all(files))

    # -- planning (host plumbing; semantic verdicts stay in MNCS) ---------
    def _plan_file(self, file_id: int, path: str, data: bytes) -> FilePlan:
        # Extension slicing is shared with the v2 extractor
        # (`extract.path_extension`): planner and extractor must map a
        # path to the same kind rank.
        from .extract import path_extension

        kind = self.k.classify_kind(path_extension(path))
        windows = [data[o : o + WINDOW] for o in range(0, len(data), WINDOW)]
        tokens = self._split_tokens(data, self.k.byte_class)
        return FilePlan(file_id, path, data, kind, windows, tokens)

    @staticmethod
    def _split_tokens(data: bytes, classify) -> list[bytes]:
        """Split on non-symbol bytes (class != 3). Every class verdict comes
        from MNCS `classify_byte` (cached pure calls); splitting applies
        those verdicts at corpus scale (pressure/PRESS-005)."""
        out: set[bytes] = set()
        cur = bytearray()
        overlong = False
        for b in data:
            if classify(b) == 3:
                if overlong:
                    continue
                cur.append(b)
                if len(cur) > TOKEN_MAX:
                    # Drop the whole token (keep determinism; record the
                    # bound in src/README.md) rather than indexing a suffix.
                    cur = bytearray()
                    overlong = True
            else:
                if cur:
                    out.add(bytes(cur))
                    cur = bytearray()
                overlong = False
        if cur:
            out.add(bytes(cur))
        toks = sorted(t for t in out if len(t) <= TOKEN_MAX)
        return toks[:TERMS_PER_FILE_CAP]

    # -- execution ---------------------------------------------------------
    def _fail(self, exc: BaseException) -> None:
        with self._err_lock:
            if not self._errors:
                self._errors.append(exc)
        self._stop.set()

    def _maybe_delay(self) -> None:
        if self.cfg.delay_ms > 0:
            with self._rng_lock:
                ms = self._rng.random() * self.cfg.delay_ms
            if ms > 0:
                time.sleep(ms / 1000.0)

    def _check_cancel(self) -> None:
        if self._stop.is_set():
            raise BuildCancelled("pipeline stopped")
        evt = self.cfg.cancel_event
        if evt is not None and evt.is_set():
            self._stop.set()
            raise BuildCancelled("cancel requested")

    def _run(self, plans: list[FilePlan]) -> list[FileResult]:
        work: queue.Queue = queue.Queue(maxsize=max(1, self.cfg.queue_size))
        leaves: dict[tuple[int, int], dict] = {}
        tok_digests: dict[bytes, int] = {}
        submitted_digests: set[bytes] = set()
        state_lock = threading.Lock()
        producers_done = threading.Event()

        def produce(plan: FilePlan) -> None:
            self._check_cancel()
            if plan.windows:
                for seq, chunk in enumerate(plan.windows):
                    self._check_cancel()
                    self._put(work, KernelItem(K_LEAF, plan.file_id, seq, chunk))
            # Empty files need no MNCS call: their digest is defined by
            # the kernel as empty_digest() (handled in assembly).
            # Token validation in batches of 8 through validate8.
            uncached = [t for t in plan.tokens if t not in self.k._token_valid]
            for b in range(0, len(uncached), 8):
                self._check_cancel()
                batch = tuple(uncached[b : b + 8])
                self._put(work, KernelItem(K_TOKVAL, plan.file_id, b // 8, toks=batch))
            # Token digests for tokens not yet known.
            for tok in plan.tokens:
                with state_lock:
                    if tok in tok_digests or tok in submitted_digests:
                        continue
                    if tok in self.k._token_digest:
                        tok_digests[tok] = self.k._token_digest[tok]
                        continue
                    submitted_digests.add(tok)
                self._check_cancel()
                self._put(work, KernelItem(K_TOKDIG, plan.file_id, toks=(tok,)))

        def consume() -> None:
            # Drain protocol: every dequeued item is task_done'd (processed
            # or discarded after stop), so work.join() always terminates and
            # a failed/cancelled build can never hang the pipeline.
            while True:
                try:
                    item = work.get(timeout=0.1)
                except queue.Empty:
                    if producers_done.is_set():
                        return
                    continue
                try:
                    if self._stop.is_set():
                        continue
                    self._check_cancel()
                    self._maybe_delay()
                    self._do_item(item, leaves, tok_digests, state_lock)
                except BuildCancelled as exc:
                    self._fail(exc)
                except Exception as exc:  # noqa: BLE001 — failure tree root
                    self._fail(exc)
                finally:
                    work.task_done()

        n_plan = max(1, min(self.cfg.workers, max(1, len(plans))))
        n_kern = max(1, self.cfg.workers)
        plan_pool = ThreadPoolExecutor(
            max_workers=n_plan, thread_name_prefix="mncs-plan"
        )
        kern_pool = ThreadPoolExecutor(
            max_workers=n_kern, thread_name_prefix="mncs-kern"
        )
        try:
            consumers = [kern_pool.submit(consume) for _ in range(n_kern)]
            chunks = [plans[i::n_plan] for i in range(n_plan)]
            producers = [
                plan_pool.submit(self._produce_chunk, produce, c) for c in chunks
            ]
            for fut in producers:
                fut.result()
            producers_done.set()
            work.join()
            for fut in consumers:
                fut.result()
        finally:
            plan_pool.shutdown(wait=True, cancel_futures=True)
            kern_pool.shutdown(wait=True, cancel_futures=True)
        if self._errors:
            exc = self._errors[0]
            if isinstance(exc, BuildCancelled):
                raise exc
            raise BuildFailed(f"indexing failed: {exc}") from exc

        return self._assemble(plans, leaves, tok_digests)

    def _produce_chunk(self, produce, chunk) -> None:
        try:
            for plan in chunk:
                produce(plan)
        except BaseException as exc:  # noqa: BLE001
            self._fail(exc)

    def _put(self, work: queue.Queue, item: KernelItem) -> None:
        while True:
            self._check_cancel()
            try:
                work.put(item, timeout=0.1)
                return
            except queue.Full:
                continue

    def _do_item(self, item: KernelItem, leaves, tok_digests, state_lock) -> None:
        if item.kind == K_LEAF:
            rec = self.k.leaf_window(self.k.empty_digest(), item.chunk, False)
            with state_lock:
                leaves[(item.file_id, item.seq)] = rec
        elif item.kind == K_TOKVAL:
            mask = self.k.validate8(list(item.toks))
            with self.k._lock:
                for k, tok in enumerate(item.toks):
                    self.k._token_valid[tok] = bool(mask & (1 << k))
        elif item.kind == K_TOKDIG:
            tok = item.toks[0]
            digest = self.k.token_digest(tok)
            with state_lock:
                tok_digests[tok] = digest

    # -- assembly (canonical order threading; deterministic) -----------------
    def _assemble(self, plans, leaves, tok_digests) -> list[FileResult]:
        classify = self.k.byte_class
        results: list[FileResult] = []
        for plan in plans:
            n = len(plan.windows)
            if n == 0:
                digest = self.k.empty_digest()
                words = lines = 0
            else:
                states = []
                word_counts = []
                line_counts = []
                opens = []
                for seq in range(n):
                    rec = leaves.get((plan.file_id, seq))
                    if rec is None:
                        raise BuildFailed(f"missing leaf for {plan.path} window {seq}")
                    states.append(rec["state"])
                    word_counts.append(rec["words"])
                    line_counts.append(rec["lines"])
                    opens.append(rec["open"])
                # Thread the carried `open` flag in canonical window order
                # and repair the one boundary overcount it implies: a word
                # spanning windows was counted in both.
                words = 0
                for seq in range(n):
                    w = word_counts[seq]
                    # Words are non-space runs (kernel rule): a boundary
                    # word continues iff the next byte is not space/newline
                    # (class 1 or 2), regardless of symbol class.
                    if (
                        seq > 0
                        and opens[seq - 1]
                        and classify(plan.windows[seq][0]) not in (1, 2)
                    ):
                        w -= 1
                    words += w
                lines = sum(line_counts)
                digest = self.k.tree_combine(states, self.cfg.workers)
            terms: list[tuple[str, int]] = []
            for tok in plan.tokens:
                if not self.k._token_valid.get(tok, False):
                    continue
                # Term identity is (path, token) with the MNCS content tag
                # `token_digest` (session-cached per token vocabulary entry);
                # file binding travels through the sort key and the parent
                # document digest rather than a per-term combine call.
                tdigest = tok_digests.get(tok)
                if tdigest is None:
                    tdigest = self.k._token_digest.get(tok)
                if tdigest is None:
                    raise BuildFailed(f"missing token digest for {plan.path}")
                terms.append((tok.decode("utf-8", "strict"), tdigest))
            terms.sort(key=lambda t: t[0].encode("utf-8"))
            terms = terms[:TERMS_PER_FILE_CAP]
            results.append(
                FileResult(
                    plan=plan, digest=digest, words=words, lines=lines, terms=terms
                )
            )
        return results
