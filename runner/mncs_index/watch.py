"""Polling watch prototype (MNCS has no filesystem-watch capability).

Semantics: poll discovery hints; require a quiet period (two consecutive
stable polls) to coalesce bursts; then run one deterministic incremental
transaction and publish. Duplicate polls and coalesced bursts cannot
corrupt canonical state because hints only select *which* incremental
transaction runs — the transaction itself re-derives truth from content
(RFC 0005). Real event-driven watching is pressure/PRESS-007.
"""

from __future__ import annotations

import threading
import time

from .discover import discover
from .indexer import incremental_snapshot
from .kernels import Kernels
from .pipeline import BuildConfig
from .store import Store


class Watcher:
    def __init__(
        self,
        root: str,
        store: Store,
        kernels: Kernels,
        config: BuildConfig,
        interval_s: float = 0.5,
        quiet_polls: int = 2,
    ):
        self.root = root
        self.store = store
        self.kernels = kernels
        self.config = config
        self.interval_s = interval_s
        self.quiet_polls = quiet_polls
        self.events: list[dict] = []
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self, max_generations: int | None = None) -> list[int]:
        if self.store.head() is None:
            raise WatchError("watch requires a published baseline generation")
        published: list[int] = []
        last_hint: tuple | None = None
        stable = 0
        while not self._stop.is_set():
            corpus = discover(self.root)
            hint = (corpus.snapshot_id, len(corpus.items))
            if hint != last_hint:
                last_hint = hint
                stable = 0
            else:
                stable += 1
            if stable >= self.quiet_polls:
                prev, established, prev_crc = self.store.load_head()
                if prev.snapshot_id != corpus.snapshot_id:
                    gen = prev.generation + 1
                    new_snap, canon, _, verdicts = incremental_snapshot(
                        self.root, gen, prev, prev_crc, self.kernels, self.config
                    )
                    estat = dict(established)
                    for d in new_snap.docs:
                        estat.setdefault(d.path, gen)
                    cur = {it.path: it.crc for it in corpus.items}
                    self.store.publish(new_snap, canon, estat, cur)
                    published.append(gen)
                    self.events.append(
                        {"generation": gen, "verdicts": verdicts, "hint": hint[0][:12]}
                    )
                    if (
                        max_generations is not None
                        and len(published) >= max_generations
                    ):
                        return published
            time.sleep(self.interval_s)
        return published


class WatchError(Exception):
    pass
