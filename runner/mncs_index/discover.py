"""Deterministic corpus discovery.

Filesystem enumeration order must never become canonical ordering: this
module returns items in a stable order and the pipeline explicitly
shuffles (seeded) or sorts downstream. Provenance paths are normalized
to forward-slash relative form.

Content acquisition is genuinely concurrent in `discover_concurrent`
(TEMPORARY host infrastructure — PRESS-001/002/003: no MNCS task, queue,
or filesystem effects exist, so enumeration, bounded queues, reader
threads, shutdown, and failure propagation live here):

    enumeration (single thread: walk, normalize, admit)
        |
        v
    bounded read queue (`queue.Queue(maxsize=queue_size)`, backpressure real)
        |
        +----> parallel reader (open + bounded read)
        +----> parallel reader
        +----> parallel reader
        |
        v
    deterministic assembly (results keyed by path, sorted; identity from
    sorted (path, size, crc) entries — completion order never escapes)

`discover` is the sequential reference implementation over the same
enumerate/read helpers. Both must produce identical snapshots
(`tests/test_concurrent_read.py`); the concurrent form is the production
build path (`indexer.py`), the sequential form stays for watch hints and
as the equivalence oracle.

Shutdown (PRESS-012 workaround): readers block in `queue.get` with a
short timeout and exit on `feeder_done`/`stop` — no unbounded wait; a
failed reader or a cancelled build sets `stop`, every dequeued item is
still `task_done`'d so the join always terminates, and reader threads are
joined on every path before return/raise. In-flight file reads run to
completion (bounded by MAX_FILE_BYTES); they cannot be interrupted, only
superseded — a file that changes under a run fails the build rather than
silently redefining the run's identity.
"""

from __future__ import annotations

import os
import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass

from .errors import BuildCancelled
from .kernels import ADMITTED_EXTENSIONS
from .model import check_path_safe, crc32_of, discovery_id

MAX_FILE_BYTES = 64 * 1024 * 1024


@dataclass
class FileItem:
    path: str  # normalized relative posix path
    size: int
    crc: int  # crc32 content hint (RFC 0005: hints, not truth)
    data: bytes


@dataclass
class CorpusSnapshot:
    root: str
    items: list
    snapshot_id: str
    skipped: int


@dataclass
class ReadStats:
    """Test/evidence instrumentation for the concurrent reader stage."""

    files: int = 0
    max_in_flight: int = 0


def _extension(name: str) -> str:
    """Admission extension; agrees with `extract.path_extension` on kind.

    Every dotfile (`.foo`, `.foo.mncs`) maps to `""` so an admitted file
    is indexed under the same kind the pipeline planner derives: odd
    names take the unknown rank instead of being admitted under one
    extension and indexed under another.
    """
    base = name.rsplit("/", 1)[-1]
    if "." not in base or base.startswith("."):
        return ""
    return base.rsplit(".", 1)[-1].lower()


def enumerate_corpus(root: str) -> tuple[list[tuple[str, str]], int]:
    """Walk `root`, return (sorted admitted [(rel, full)], skipped).

    Single-threaded metadata pass only: no file content is read here, so
    non-admitted paths are skipped without being opened (a file we would
    never index cannot fail discovery). `skipped` counts exactly what the
    sequential implementation always skipped: symlinks/non-files and
    non-admitted extensions.
    """
    if not os.path.isdir(root):
        raise DiscoveryError(f"corpus root is not a directory: {root}")
    admitted: list[tuple[str, str]] = []
    skipped = 0
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames.sort()
        for name in sorted(filenames):
            full = os.path.join(dirpath, name)
            if os.path.islink(full) or not os.path.isfile(full):
                skipped += 1
                continue
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            try:
                rel.encode("utf-8")
                check_path_safe(rel)
            except (UnicodeEncodeError, ValueError) as exc:
                raise DiscoveryError(
                    f"path cannot be canonicalized: {rel!r}: {exc}"
                )
            if _extension(rel) not in ADMITTED_EXTENSIONS:
                skipped += 1
                continue
            admitted.append((rel, full))
    admitted.sort()
    return admitted, skipped


def read_one_file(full: str) -> bytes:
    """Read one enumerated file with the corpus size bound enforced."""
    try:
        with open(full, "rb") as fh:
            data = fh.read(MAX_FILE_BYTES + 1)
    except OSError as exc:
        raise DiscoveryError(f"cannot read during discovery: {full}") from exc
    if len(data) > MAX_FILE_BYTES:
        raise DiscoveryError(f"file exceeds size bound: {full}")
    return data


def discover(root: str) -> CorpusSnapshot:
    """Walk `root`, read admitted files sequentially (reference form)."""
    admitted, skipped = enumerate_corpus(root)
    items = [
        FileItem(path=rel, size=len(data), crc=crc32_of(data), data=data)
        for rel, full in admitted
        for data in (read_one_file(full),)
    ]
    snap_id = discovery_id([(it.path, it.size, it.crc) for it in items])
    return CorpusSnapshot(root=root, items=items, snapshot_id=snap_id, skipped=skipped)


def discover_concurrent(
    root: str,
    *,
    readers: int = 4,
    queue_size: int = 64,
    cancel_event: threading.Event | None = None,
    read_bytes: Callable[[str], bytes] | None = None,
    stats: ReadStats | None = None,
) -> CorpusSnapshot:
    """Walk `root`, read admitted files with parallel bounded readers.

    `readers` bounds reader-thread count, `queue_size` bounds the read
    queue (unread-path backlog); both share the caller's worker/queue
    budget. `read_bytes(full)` overrides the file read (tests only:
    slow/failing/blocking readers); production always reads real files.
    `stats`, when given, records files completed and peak read overlap.
    """
    if cancel_event is not None and cancel_event.is_set():
        raise BuildCancelled("cancel requested")
    admitted, skipped = enumerate_corpus(root)
    if cancel_event is not None and cancel_event.is_set():
        raise BuildCancelled("cancel requested")
    if not admitted:
        return CorpusSnapshot(
            root=root, items=[], snapshot_id=discovery_id([]), skipped=skipped
        )
    # Module-global lookup at call time so tests can monkeypatch the read.
    read = read_bytes if read_bytes is not None else read_one_file

    n = max(1, min(readers, len(admitted)))
    work_q: queue.Queue = queue.Queue(maxsize=max(1, queue_size))
    feeder_done = threading.Event()
    stop = threading.Event()
    lock = threading.Lock()
    errors: list[BaseException] = []
    results: dict[str, bytes] = {}
    in_flight = 0

    def _fail(exc: BaseException) -> None:
        with lock:
            if not errors:
                errors.append(exc)
        stop.set()

    def _check_cancel() -> None:
        if stop.is_set():
            raise BuildCancelled("pipeline stopped")
        if cancel_event is not None and cancel_event.is_set():
            stop.set()
            raise BuildCancelled("cancel requested")

    def reader() -> None:
        nonlocal in_flight
        # Drain protocol (mirrors pipeline.py): every dequeued item is
        # task_done'd — processed or discarded after stop — so the join
        # always terminates and a failed/cancelled run can never hang.
        while True:
            try:
                item = work_q.get(timeout=0.1)
            except queue.Empty:
                if feeder_done.is_set() or stop.is_set():
                    return
                continue
            try:
                if item is None:  # shutdown sentinel
                    return
                if stop.is_set():
                    continue
                _check_cancel()
                with lock:
                    in_flight += 1
                    if stats is not None:
                        stats.max_in_flight = max(stats.max_in_flight, in_flight)
                try:
                    data = read(item[1])
                finally:
                    with lock:
                        in_flight -= 1
                _check_cancel()
                with lock:
                    results[item[0]] = data
                    if stats is not None:
                        stats.files += 1
            except BuildCancelled as exc:
                _fail(exc)
            except DiscoveryError as exc:
                _fail(exc)
            except OSError as exc:
                _fail(DiscoveryError(f"cannot read during discovery: {exc}"))
            except Exception as exc:  # noqa: BLE001 — failure tree root
                _fail(DiscoveryError(f"cannot read during discovery: {exc}"))
            finally:
                work_q.task_done()

    def _feed(item: tuple[str, str] | None) -> bool:
        while True:
            if stop.is_set():
                return False
            if cancel_event is not None and cancel_event.is_set():
                _fail(BuildCancelled("cancel requested"))
                return False
            try:
                work_q.put(item, timeout=0.1)
                return True
            except queue.Full:
                continue

    threads = [threading.Thread(target=reader, name=f"mncs-reader-{i}") for i in range(n)]
    for t in threads:
        t.start()
    try:
        for pair in admitted:
            if not _feed(pair):
                break
        feeder_done.set()
        # Best-effort sentinel wake-ups; readers also exit via the
        # feeder_done/empty and stop/timeout paths, so a full queue here
        # delays but never prevents shutdown.
        for _ in range(n):
            try:
                work_q.put_nowait(None)
            except queue.Full:
                break
        work_q.join()
    finally:
        feeder_done.set()
        stop.set()
        for t in threads:
            t.join()
    if errors:
        raise errors[0]
    items = [
        FileItem(path=rel, size=len(results[rel]), crc=crc32_of(results[rel]),
                 data=results[rel])
        for rel, _full in admitted
    ]
    snap_id = discovery_id([(it.path, it.size, it.crc) for it in items])
    return CorpusSnapshot(root=root, items=items, snapshot_id=snap_id, skipped=skipped)


class DiscoveryError(Exception):
    pass
