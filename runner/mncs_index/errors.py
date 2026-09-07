"""Shared pipeline error types (import-cycle free).

`discover.py` (content acquisition) and `pipeline.py` (kernel execution)
both need the same failure vocabulary; defining it here keeps the import
graph acyclic. Names are re-exported from `pipeline.py`, so existing
`from mncs_index.pipeline import BuildFailed` imports keep working.
"""


class BuildFailed(Exception):
    pass


class BuildCancelled(Exception):
    pass
