"""Polling watch prototype (MNCS has no filesystem-watch capability).

Semantics: poll discovery hints; require a quiet period (consecutive
stable polls) to coalesce bursts; then run one deterministic incremental
transaction and publish. Hints schedule *prompt* publication, but they
are not truth (RFC 0005): every `validate_every`-th quiet window runs
the incremental transaction even when hints are unchanged, because a
same-size/same-CRC32 content change leaves `snapshot_id` identical and
would otherwise keep HEAD stale forever. Hint-invisible changes thus
surface within a bounded number of quiet windows. A validation that
finds every verdict unchanged (0) publishes nothing. The transaction
itself re-derives truth from content, so duplicate polls and coalesced
bursts cannot corrupt canonical state. Real event-driven watching is
pressure/PRESS-007; the validation backstop prices PRESS-010.

With `rich=True` the watcher runs the canonical-v2 incremental
transaction so a `--rich` store keeps its sym/heading/rel/press tables;
without it a v2 store would silently degrade to v1 on the next watch
publication.
"""

from __future__ import annotations

import threading
import time

from .indexer import (
    _acquire,
    incremental_snapshot,
    incremental_snapshot_v2,
)
from .kernels import Kernels
from .pipeline import BuildConfig
from .store import StaleGenerationError, Store


class Watcher:
    def __init__(
        self,
        root: str,
        store: Store,
        kernels: Kernels,
        config: BuildConfig,
        interval_s: float = 0.5,
        quiet_polls: int = 2,
        rich: bool = False,
        validate_every: int = 4,
    ):
        self.root = root
        self.store = store
        self.kernels = kernels
        self.config = config
        self.interval_s = interval_s
        self.quiet_polls = quiet_polls
        self.rich = rich
        self.validate_every = max(1, validate_every)
        self.events: list[dict] = []
        self.validations = 0
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def _transact(self, gen: int, prev, prev_crc):
        if self.rich:
            return incremental_snapshot_v2(
                self.root, gen, prev, prev_crc, self.kernels, self.config
            )
        return incremental_snapshot(
            self.root, gen, prev, prev_crc, self.kernels, self.config
        )

    def run(self, max_generations: int | None = None) -> list[int]:
        if self.store.head() is None:
            raise WatchError("watch requires a published baseline generation")
        published: list[int] = []
        last_hint: tuple | None = None
        stable = 0
        quiet_windows = 0
        while not self._stop.is_set():
            # Same concurrent acquisition stage as production builds, so
            # watch pressure resembles the eventual MNCS design.
            corpus = _acquire(self.root, self.config)
            hint = (corpus.snapshot_id, len(corpus.items))
            if hint != last_hint:
                last_hint = hint
                stable = 0
            else:
                stable += 1
            if stable >= self.quiet_polls:
                quiet_windows += 1
                prev, established, prev_crc = self.store.load_head()
                hints_moved = prev.snapshot_id != corpus.snapshot_id
                due = quiet_windows % self.validate_every == 0
                if not hints_moved and not due:
                    time.sleep(self.interval_s)
                    continue
                gen = prev.generation + 1
                new_snap, canon, _, verdicts = self._transact(
                    gen, prev, prev_crc
                )
                self.validations += 1
                if not hints_moved and all(v == 0 for v in verdicts.values()):
                    # Periodic authoritative validation confirmed HEAD is
                    # current; publish nothing, keep watching.
                    self.events.append(
                        {
                            "generation": gen,
                            "validated": True,
                            "hint": hint[0][:12],
                        }
                    )
                    time.sleep(self.interval_s)
                    continue
                estat = dict(established)
                for d in new_snap.docs:
                    estat.setdefault(d.path, gen)
                cur = {it.path: it.crc for it in corpus.items}
                try:
                    self.store.publish(new_snap, canon, estat, cur, prev.generation)
                except StaleGenerationError:
                    # Another writer published first; this candidate is
                    # stale. Drop it and re-poll from the new HEAD.
                    self.events.append(
                        {
                            "generation": gen,
                            "superseded": True,
                            "hint": hint[0][:12],
                        }
                    )
                    last_hint = None
                    stable = 0
                    continue
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
