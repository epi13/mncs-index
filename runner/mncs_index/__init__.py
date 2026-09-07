"""mncs-index runner: thin host infrastructure driving MNCS index kernels.

Production meaning lives in `src/*.mncs`; this package provides only the
effects MNCS cannot yet express (filesystem, threads, scheduling, time,
SHA-256). Every module docstring names the pressure entries that will
remove it.
"""

from .bridge import Bridge, BridgeError, MNCSError
from .discover import CorpusSnapshot, DiscoveryError, discover
from .indexer import build_snapshot, incremental_snapshot
from .kernels import Kernels
from .model import Snapshot
from .pipeline import BuildCancelled, BuildConfig, BuildFailed, Pipeline
from .query import QueryEngine, QueryResult
from .store import Store, StoreError
from .watch import Watcher

__all__ = [
    "Bridge",
    "BridgeError",
    "BuildCancelled",
    "BuildConfig",
    "BuildFailed",
    "CorpusSnapshot",
    "DiscoveryError",
    "Kernels",
    "MNCSError",
    "Pipeline",
    "QueryEngine",
    "QueryResult",
    "Snapshot",
    "Store",
    "StoreError",
    "Watcher",
    "build_snapshot",
    "discover",
    "incremental_snapshot",
]
