"""Derived Index projection over the Store typed commit feed.

The binary decoders here are transport adapters for the versioned Store
records. They do not persist canonical data, allocate logical Store identity,
or decide Commons semantics. Losing a ``DerivedStoreIndex`` is safe: callers
rebuild it from the same Store commit feed and relation objects.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


RELATION_SIZE = 80
FEED_SIZE = 56
PROVENANCE_SIZE = 112


@dataclass(frozen=True)
class StoreRelation:
    kind: int
    source: bytes
    target: bytes
    generation: int
    provenance: bytes
    ordinal: int
    raw: bytes

    @classmethod
    def decode(cls, raw: bytes) -> "StoreRelation":
        raw = bytes(raw)
        if (
            len(raw) != RELATION_SIZE
            or raw[:4] != b"MR\x01\x00"
            or raw[5:8] != b"\x00\x00\x00"
            or raw[4] not in (1, 2, 3, 4)
        ):
            raise ValueError("invalid store.relationship.v1 record")
        if raw[4] not in (1, 2, 3, 4) or raw[5:8] != b"\0\0\0":
            raise ValueError("invalid Store relation kind/reserved bytes")
        return cls(
            kind=raw[4],
            source=raw[8:20],
            target=raw[20:32],
            generation=int.from_bytes(raw[32:40], "big"),
            provenance=raw[40:72],
            ordinal=int.from_bytes(raw[72:80], "big"),
            raw=raw,
        )

    @property
    def identity(self) -> tuple[int, bytes, bytes, int]:
        return (self.kind, self.source, self.target, self.ordinal)


@dataclass(frozen=True)
class StoreProvenance:
    source: bytes
    producer: bytes
    transformation: bytes
    generation: int
    evidence: bytes
    ancestry: bytes
    raw: bytes

    @classmethod
    def decode(cls, raw: bytes) -> "StoreProvenance":
        raw = bytes(raw)
        if len(raw) != PROVENANCE_SIZE or raw[:4] != b"MP\x01\x00":
            raise ValueError("invalid store.provenance.v1 record")
        return cls(
            source=raw[4:16],
            producer=raw[16:28],
            transformation=raw[28:40],
            generation=int.from_bytes(raw[40:48], "big"),
            evidence=raw[48:80],
            ancestry=raw[80:112],
            raw=raw,
        )


@dataclass(frozen=True)
class StoreFeed:
    generation: int
    object_count: int
    relation_count: int
    provenance_count: int
    root: bytes
    raw: bytes

    @classmethod
    def decode(cls, raw: bytes) -> "StoreFeed":
        raw = bytes(raw)
        if len(raw) != FEED_SIZE or raw[:4] != b"MC\x01\x00":
            raise ValueError("invalid store.commit_feed.v1 record")
        return cls(
            generation=int.from_bytes(raw[4:12], "big"),
            object_count=int.from_bytes(raw[12:16], "big"),
            relation_count=int.from_bytes(raw[16:20], "big"),
            provenance_count=int.from_bytes(raw[20:24], "big"),
            root=raw[24:56],
            raw=raw,
        )


@dataclass(frozen=True)
class StoreCommit:
    feed: StoreFeed
    relations: tuple[StoreRelation, ...] = ()
    provenance: tuple[StoreProvenance, ...] = ()

    def validate(self) -> None:
        if self.feed.relation_count != len(self.relations):
            raise ValueError("Store feed relation count does not match commit")
        if self.feed.provenance_count != len(self.provenance):
            raise ValueError("Store feed provenance count does not match commit")
        if any(r.generation != self.feed.generation for r in self.relations):
            raise ValueError("relation generation does not match Store feed")


@dataclass(frozen=True)
class DerivedQuery:
    store_generation: int
    index_through_generation: int
    freshness: str
    complete: bool
    source_authority: str
    result_identity: str
    relations: tuple[StoreRelation, ...]

    def as_projection(self) -> dict:
        """JSON-friendly external projection; never the canonical identity."""
        return {
            "store_generation": self.store_generation,
            "index_through_generation": self.index_through_generation,
            "freshness": self.freshness,
            "complete": self.complete,
            "source_authority": self.source_authority,
            "result_identity": self.result_identity,
            "relations": [
                {
                    "kind": r.kind,
                    "source_identity": r.source.hex(),
                    "target_identity": r.target.hex(),
                    "generation": r.generation,
                    "provenance_identity": r.provenance.hex(),
                    "ordinal": r.ordinal,
                }
                for r in self.relations
            ],
        }


def _freshness(index_generation: int, store_generation: int) -> tuple[str, bool]:
    if index_generation == store_generation:
        return "complete", True
    if index_generation < store_generation:
        return "stale", False
    return "future_invalid", False


class DerivedStoreIndex:
    """Disposable relation projection with explicit Store generation state."""

    def __init__(self) -> None:
        self._relations: dict[tuple[int, bytes, bytes, int], StoreRelation] = {}
        self.store_generation = 0
        self.index_through_generation = 0

    @classmethod
    def rebuild(cls, commits: list[StoreCommit]) -> "DerivedStoreIndex":
        index = cls()
        for position, commit in enumerate(sorted(commits, key=lambda value: value.feed.generation)):
            index._accept(commit, replace=position == 0)
        return index

    def _accept(self, commit: StoreCommit, replace: bool) -> None:
        commit.validate()
        if commit.feed.generation < self.index_through_generation:
            raise ValueError("Store commits must advance monotonically")
        if replace:
            self._relations.clear()
        for relation in commit.relations:
            self._relations[relation.identity] = relation
        self.index_through_generation = commit.feed.generation
        self.store_generation = max(self.store_generation, commit.feed.generation)

    def apply(self, commit: StoreCommit) -> None:
        """Apply one deterministic Store change description."""
        if commit.feed.generation <= self.index_through_generation:
            raise ValueError("Index cannot apply a non-advancing Store generation")
        self._accept(commit, replace=False)

    def observe_store_generation(self, generation: int) -> None:
        """Record a newer Store head without pretending the index caught up."""
        if generation < self.store_generation:
            raise ValueError("Store generation moved backwards")
        self.store_generation = generation

    def destroy(self) -> None:
        """Drop all derived state; canonical Store records are unaffected."""
        self._relations.clear()
        self.index_through_generation = 0
        self.store_generation = 0

    @property
    def relations(self) -> tuple[StoreRelation, ...]:
        return tuple(sorted(self._relations.values(), key=lambda r: r.identity))

    def query_relations(
        self,
        *,
        target: bytes | None = None,
        source: bytes | None = None,
        kind: int | None = None,
    ) -> DerivedQuery:
        freshness, complete = _freshness(
            self.index_through_generation, self.store_generation
        )
        hits = tuple(
            relation
            for relation in self.relations
            if (target is None or relation.target == bytes(target))
            and (source is None or relation.source == bytes(source))
            and (kind is None or relation.kind == kind)
        )
        result_identity = hashlib.sha256(
            b"mncs-index/result-v1\0" + b"".join(r.raw for r in hits)
        ).hexdigest()
        return DerivedQuery(
            store_generation=self.store_generation,
            index_through_generation=self.index_through_generation,
            freshness=freshness,
            complete=complete,
            source_authority="Commons pressure lifecycle and family semantics",
            result_identity=result_identity,
            relations=hits,
        )
