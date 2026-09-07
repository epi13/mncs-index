"""Typed wrappers over the MNCS index kernels.

Every function here executes real MNCS source from `src/*.mncs` through
the bridge. The caches below are keyed by input bytes for pure,
referentially transparent kernels (byte class, kind, token validity,
token digest); caching changes call counts, never meaning.
"""

from __future__ import annotations

import threading

from .bridge import Bridge, boolean, seq_bytes, u64
from .bridge import byte as byte_enc

DIGEST_SRC = "digest.mncs"
DIGEST_MOD = "mncs.index.digest.v1"
SCAN_SRC = "scan.mncs"
SCAN_MOD = "mncs.index.scan.v1"
KIND_SRC = "kind.mncs"
KIND_MOD = "mncs.index.kind.v1"
ORDER_SRC = "order.mncs"
ORDER_MOD = "mncs.index.order.v1"
EXTRACT_SRC = "extract.mncs"
EXTRACT_MOD = "mncs.index.extract.v1"

MASK64 = (1 << 64) - 1
FNV_BASIS = 14695981039346656037

# Canonical kind ranks (mirror of kind.mncs; the MNCS kernel is
# authoritative — KIND_NAMES here is display only).
KIND_NAMES = {
    0: "unknown",
    1: "mncs",
    2: "md",
    3: "json",
    4: "toml",
    5: "yaml",
    6: "text",
    7: "log",
}
TERM_KIND = 100

# File extensions admitted to the corpus (lowercased, without dot).
# Anything else is skipped at discovery, never indexed as unknown: the
# unknown rank exists for explicit extensionless/odd names.
ADMITTED_EXTENSIONS = frozenset(
    {"mncs", "md", "json", "toml", "yaml", "yml", "txt", "text", "log", ""}
)


class Kernels:
    def __init__(self, bridge: Bridge):
        self.b = bridge
        self._lock = threading.Lock()
        self._byte_class: dict[int, int] = {}
        self._kind: dict[bytes, int] = {}
        self._token_valid: dict[bytes, bool] = {}
        self._token_digest: dict[bytes, int] = {}
        self._decl: dict[bytes, int] = {}
        self._heading: dict[bytes, int] = {}
        self._link: dict[bytes, bool] = {}
        self._press: dict[bytes, bool] = {}
        self._rfc: dict[bytes, int] = {}

    # -- digest.v1 ------------------------------------------------------
    def empty_digest(self) -> int:
        return self.b.call_u64(DIGEST_SRC, DIGEST_MOD, "empty_digest", [])

    def leaf_window(self, state: int, chunk: bytes, open_in: bool) -> dict:
        return self.b.call_record(
            DIGEST_SRC,
            DIGEST_MOD,
            "leaf_window",
            [u64(state), seq_bytes(chunk), u64(len(chunk)), boolean(open_in)],
        )

    def fold_window(self, state: int, chunk: bytes) -> int:
        return self.b.call_u64(
            DIGEST_SRC,
            DIGEST_MOD,
            "fold_window",
            [u64(state), seq_bytes(chunk), u64(len(chunk))],
        )

    def combine(self, left: int, right: int) -> int:
        return self.b.call_u64(
            DIGEST_SRC, DIGEST_MOD, "combine", [u64(left), u64(right)]
        )

    def tree_combine(self, leaves: list[int], workers: int) -> int:
        """Canonical Merkle reduction over ordered leaves (PRESS-001/002).

        Pairing is fixed left-to-right; an odd tail pairs with 0. Levels
        are sequential, pairs within a level run concurrently; results are
        placed by index, so scheduling cannot affect the digest.
        """
        from concurrent.futures import ThreadPoolExecutor

        if not leaves:
            return self.empty_digest()
        level = list(leaves)
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            while len(level) > 1:
                pairs = [
                    (level[i], level[i + 1] if i + 1 < len(level) else 0)
                    for i in range(0, len(level), 2)
                ]
                level = list(pool.map(lambda p: self.combine(p[0], p[1]), pairs))
        return level[0]

    def tree_digest_windows(self, data: bytes, workers: int) -> int:
        """Merkle digest of a byte string: parallel MNCS leaf folds plus
        canonical tree reduction. Same construction as per-file digests,
        so one scheme covers content and canonical bytes alike."""
        from concurrent.futures import ThreadPoolExecutor

        if not data:
            return self.empty_digest()
        chunks = [data[o : o + 64] for o in range(0, len(data), 64)]
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            leaves = list(pool.map(lambda c: self.fold_window(FNV_BASIS, c), chunks))
        return self.tree_combine(leaves, workers)

    # -- scan.v1 ----------------------------------------------------------
    def byte_class(self, value: int) -> int:
        with self._lock:
            hit = self._byte_class.get(value)
        if hit is None:
            hit = self.b.call_u64(
                SCAN_SRC, SCAN_MOD, "classify_byte", [byte_enc(value)]
            )
            with self._lock:
                self._byte_class[value] = hit
        return hit

    def byte_class_table(self) -> list[int]:
        return [self.byte_class(v) for v in range(256)]

    def is_symbol_token(self, tok: bytes) -> bool:
        with self._lock:
            hit = self._token_valid.get(tok)
        if hit is None:
            if len(tok) > 32:
                raise ValueError("token exceeds 32-byte kernel bound")
            hit = self.b.call_bool(
                SCAN_SRC,
                SCAN_MOD,
                "is_symbol_token",
                [seq_bytes(tok), u64(len(tok))],
            )
            with self._lock:
                self._token_valid[tok] = hit
        return hit

    def validate8(self, toks: list[bytes]) -> int:
        """Validate up to 8 tokens in one MNCS call; bit k = slot k valid."""
        args: list = []
        for k in range(8):
            tok = toks[k] if k < len(toks) else b""
            if len(tok) > 32:
                raise ValueError("token exceeds 32-byte kernel bound")
            args += [seq_bytes(tok), u64(len(tok))]
        return self.b.call_u64(SCAN_SRC, SCAN_MOD, "validate8", args)

    def token_digest(self, tok: bytes) -> int:
        with self._lock:
            hit = self._token_digest.get(tok)
        if hit is None:
            hit = self.b.call_u64(
                SCAN_SRC,
                SCAN_MOD,
                "token_digest",
                [seq_bytes(tok), u64(len(tok))],
            )
            with self._lock:
                self._token_digest[tok] = hit
        return hit

    # -- kind.v1 ------------------------------------------------------------
    def classify_kind(self, ext: bytes) -> int:
        with self._lock:
            hit = self._kind.get(ext)
        if hit is None:
            hit = self.b.call_u64(
                KIND_SRC,
                KIND_MOD,
                "classify_kind",
                [seq_bytes(ext), u64(len(ext))],
            )
            with self._lock:
                self._kind[ext] = hit
        return hit

    # -- order.v1 -----------------------------------------------------------
    def compare_window(self, a: bytes, b: bytes) -> int:
        return self.b.call(
            ORDER_SRC,
            ORDER_MOD,
            "compare_window",
            [seq_bytes(a), u64(len(a)), seq_bytes(b), u64(len(b))],
        )[0]["integer"]["value"]

    def path_less(self, a: bytes, b: bytes) -> bool:
        return self.b.call_bool(
            ORDER_SRC,
            ORDER_MOD,
            "path_less",
            [seq_bytes(a), u64(len(a)), seq_bytes(b), u64(len(b))],
        )

    def record_less(
        self, ka: int, pa: bytes, sa: int, kb: int, pb: bytes, sb: int
    ) -> bool:
        return self.b.call_bool(
            ORDER_SRC,
            ORDER_MOD,
            "record_less",
            [
                u64(ka),
                seq_bytes(pa),
                u64(len(pa)),
                u64(sa),
                u64(kb),
                seq_bytes(pb),
                u64(len(pb)),
                u64(sb),
            ],
        )

    def contains8(self, hay: bytes, needle: bytes) -> bool:
        padded = needle + b"\x00" * (8 - len(needle))
        return self.b.call_bool(
            ORDER_SRC,
            ORDER_MOD,
            "contains8",
            [seq_bytes(hay), u64(len(hay)), seq_bytes(padded), u64(len(needle))],
        )

    def query_match(
        self,
        kind: int,
        qkind: int,
        kind_any: bool,
        digest: int,
        qdigest: int,
        digest_any: bool,
        hay: bytes,
        needle: bytes,
        term_any: bool,
    ) -> bool:
        padded = needle + b"\x00" * (8 - len(needle))
        return self.b.call_bool(
            ORDER_SRC,
            ORDER_MOD,
            "query_match",
            [
                u64(kind),
                u64(qkind),
                boolean(kind_any),
                u64(digest),
                u64(qdigest),
                boolean(digest_any),
                seq_bytes(hay),
                u64(len(hay)),
                seq_bytes(padded),
                u64(len(needle)),
                boolean(term_any),
            ],
        )

    def classify_change(
        self, old_present: bool, old_digest: int, new_present: bool, new_digest: int
    ) -> int:
        return self.b.call_u64(
            ORDER_SRC,
            ORDER_MOD,
            "classify_change",
            [
                boolean(old_present),
                u64(old_digest),
                boolean(new_present),
                u64(new_digest),
            ],
        )

    # -- extract.v1 ---------------------------------------------------------
    # Line/token verdicts for the canonical-v2 normalized model. Pure and
    # referentially transparent like the scan kernels above: cached by
    # input bytes, meaning unchanged.
    def _cached_call(self, cache: dict, key: bytes, fn):
        with self._lock:
            hit = cache.get(key)
        if hit is None:
            hit = fn()
            with self._lock:
                cache[key] = hit
        return hit

    def classify_decl(self, line: bytes) -> int:
        """0 other, 1 module, 2 fn, 3 record, 4 use (line left-trimmed)."""
        if len(line) > 64:
            raise ValueError("decl line exceeds 64-byte kernel bound")
        return self._cached_call(
            self._decl,
            line,
            lambda: self.b.call_u64(
                EXTRACT_SRC,
                EXTRACT_MOD,
                "classify_decl",
                [seq_bytes(line), u64(len(line))],
            ),
        )

    def heading_level(self, line: bytes) -> int:
        """Markdown heading level 1..6 over a left-trimmed line, else 0."""
        if len(line) > 64:
            raise ValueError("heading line exceeds 64-byte kernel bound")
        return self._cached_call(
            self._heading,
            line,
            lambda: self.b.call_u64(
                EXTRACT_SRC,
                EXTRACT_MOD,
                "heading_level",
                [seq_bytes(line), u64(len(line))],
            ),
        )

    def contains_link(self, line: bytes) -> bool:
        """True iff the line window holds a `](` link seam."""
        if len(line) > 64:
            raise ValueError("link line exceeds 64-byte kernel bound")
        return self._cached_call(
            self._link,
            line,
            lambda: self.b.call_bool(
                EXTRACT_SRC,
                EXTRACT_MOD,
                "contains_link",
                [seq_bytes(line), u64(len(line))],
            ),
        )

    def is_press_id(self, tok: bytes) -> bool:
        """Exact `PRESS-` + 3 digits shape (length 9)."""
        if len(tok) > 32:
            raise ValueError("press token exceeds 32-byte kernel bound")
        return self._cached_call(
            self._press,
            tok,
            lambda: self.b.call_bool(
                EXTRACT_SRC,
                EXTRACT_MOD,
                "is_press_id",
                [seq_bytes(tok), u64(len(tok))],
            ),
        )

    def classify_rfc_token(self, tok: bytes) -> int:
        """0 other, 1 RFC keyword, 2 four-digit number, 3 RFCS keyword."""
        if len(tok) > 32:
            raise ValueError("rfc token exceeds 32-byte kernel bound")
        return self._cached_call(
            self._rfc,
            tok,
            lambda: self.b.call_u64(
                EXTRACT_SRC,
                EXTRACT_MOD,
                "classify_rfc_token",
                [seq_bytes(tok), u64(len(tok))],
            ),
        )

    def fold_bytes(self, data: bytes) -> int:
        """MNCS fold of arbitrary bytes: chunked `fold_window` thread.

        Same fold definition as content digests, so name/title digests
        share the scheme; chunking is host plumbing over unbounded text
        (PRESS-005/014), the step is the kernel.
        """
        state = FNV_BASIS
        for off in range(0, len(data), 64):
            state = self.fold_window(state, data[off : off + 64])
        return state
