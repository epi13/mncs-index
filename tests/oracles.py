"""Host-side oracles for differential testing (TESTS ONLY).

These mirror the MNCS kernel algorithms in plain Python. They are never
imported by `runner/mncs_index` production modules: production meaning
comes from MNCS execution, and these oracles exist only to cross-check it
on randomized inputs. If an oracle and a kernel disagree, the kernel is
the authority under investigation — see tests/test_differential.py.
"""

MASK64 = (1 << 64) - 1
FNV_BASIS = 14695981039346656037
FNV_PRIME = 1099511628211
COMBINE_C2 = 16777619


def mix_step(state: int, b: int) -> int:
    added = (state + b) & MASK64
    mulled = (added * FNV_PRIME) & MASK64
    return (mulled + (mulled >> 29)) & MASK64


def fold_window(state: int, chunk: bytes) -> int:
    h = state
    for b in chunk:
        h = mix_step(h, b)
    return h


def leaf_digest(chunk: bytes) -> int:
    return fold_window(FNV_BASIS, chunk)


def combine(left: int, right: int) -> int:
    mixed = ((left * FNV_PRIME) + (right * COMBINE_C2)) & MASK64
    return (mixed + (mixed >> 31)) & MASK64


def tree_digest(data: bytes) -> int:
    if not data:
        return FNV_BASIS
    leaves = [leaf_digest(data[o : o + 64]) for o in range(0, len(data), 64)]
    level = list(leaves)
    while len(level) > 1:
        nxt = []
        for i in range(0, len(level), 2):
            if i + 1 < len(level):
                nxt.append(combine(level[i], level[i + 1]))
            else:
                nxt.append(combine(level[i], 0))
        level = nxt
    return level[0]


def classify_byte(b: int) -> int:
    if b == 10:
        return 1
    if b in (32, 9, 13):
        return 2
    if b == 95 or 48 <= b <= 57 or 65 <= b <= 90 or 97 <= b <= 122:
        return 3
    return 0


def is_symbol_token(tok: bytes) -> bool:
    if not tok or len(tok) > 32:
        return False
    if 48 <= tok[0] <= 57:
        return False
    return all(classify_byte(b) == 3 for b in tok)


def token_digest(tok: bytes) -> int:
    return fold_window(FNV_BASIS, tok)


def scan_stats(data: bytes) -> tuple[int, int]:
    """(words, lines) with the kernel's space policy."""
    words = lines = 0
    in_word = False
    for b in data:
        cls = classify_byte(b)
        if b == 10:
            lines += 1
        is_sp = cls in (1, 2)
        if not in_word and not is_sp:
            words += 1
        in_word = not is_sp
    return words, lines


def classify_kind(ext: bytes) -> int:
    table = {
        b"mncs": 1,
        b"md": 2,
        b"json": 3,
        b"toml": 4,
        b"yaml": 5,
        b"yml": 5,
        b"txt": 6,
        b"text": 6,
        b"log": 7,
    }
    return table.get(ext, 0)


def compare_bytes(a: bytes, b: bytes) -> int:
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def contains(hay: bytes, needle: bytes) -> bool:
    return needle in hay


def classify_change(
    old_present: bool, old_digest: int, new_present: bool, new_digest: int
) -> int:
    if old_present:
        if new_present:
            return 0 if old_digest == new_digest else 3
        return 2
    return 1 if new_present else 0
