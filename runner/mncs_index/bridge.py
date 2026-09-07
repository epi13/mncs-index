"""Thin host bridge to the MNCS reference executor.

TEMPORARY INFRASTRUCTURE (see pressure/PRESS-001, PRESS-002): every
subprocess call below stands in for a missing MNCS capability — native
task spawning, in-process kernel invocation, and effect-gated execution.
The bridge adds no semantics: it encodes arguments, invokes one pure
MNCS kernel function via `mncs execute`, and decodes the returned value.
All indexing meaning lives in `src/*.mncs`.

The bridge is instrumented (calls, max in-flight) so tests can prove
that real MNCS execution happened concurrently.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import threading
from dataclasses import dataclass


class BridgeError(Exception):
    """Transport-level failure: missing binary, crash, timeout, bad JSON."""


class MNCSError(Exception):
    """The MNCS kernel reported a non-`returned` status (failure, budget,
    unsupported, invalid request). Carries kernel + status for typed
    failure propagation (RFC 0002)."""

    def __init__(self, module: str, function: str, status: str, detail: str = ""):
        super().__init__(f"mncs {module}::{function}: {status} {detail}".strip())
        self.module = module
        self.function = function
        self.status = status
        self.detail = detail


def default_binary() -> str:
    if os.environ.get("MNCS_BIN"):
        return os.environ["MNCS_BIN"]
    here = os.path.dirname(os.path.abspath(__file__))
    sibling = os.path.normpath(
        os.path.join(here, "..", "..", "..", "mncs-language", "target", "debug", "mncs")
    )
    if os.path.isfile(sibling) and os.access(sibling, os.X_OK):
        return sibling
    return "mncs"


def u64(v: int) -> dict:
    if not 0 <= v < 2**64:
        raise ValueError(f"u64 out of range: {v}")
    return {"integer": {"value": v, "type": {"bits": 64, "signed": False}}}


def i64(v: int) -> dict:
    if not -(2**63) <= v < 2**63:
        raise ValueError(f"i64 out of range: {v}")
    return {"integer": {"value": v, "type": {"bits": 64, "signed": True}}}


def boolean(v: bool) -> dict:
    return {"boolean": {"value": bool(v)}}


def byte(v: int) -> dict:
    if not 0 <= v < 256:
        raise ValueError(f"byte out of range: {v}")
    return {"byte": {"value": v}}


def seq_bytes(data: bytes) -> dict:
    return {"sequence": {"values": [byte(b) for b in data]}}


def parse_record(value: dict) -> dict:
    """Decode an MNCS record value into {field: python-value}."""
    rec = value["record"]
    out: dict = {}
    for name, item in rec["fields"]:
        if "integer" in item:
            out[name] = item["integer"]["value"]
        elif "boolean" in item:
            out[name] = item["boolean"]["value"]
        elif "byte" in item:
            out[name] = item["byte"]["value"]
        else:
            raise BridgeError(f"unsupported record field type for {name}: {item}")
    return out


@dataclass
class BridgeStats:
    calls: int = 0
    failures: int = 0
    max_in_flight: int = 0


class Bridge:
    """Synchronous `mncs execute` driver with concurrency instrumentation."""

    def __init__(
        self,
        binary: str | None = None,
        src_dir: str | None = None,
        step_budget: int = 100000,
        timeout_s: float = 60.0,
    ):
        self.binary = binary or default_binary()
        if src_dir is None:
            here = os.path.dirname(os.path.abspath(__file__))
            src_dir = os.path.normpath(os.path.join(here, "..", "..", "src"))
        self.src_dir = src_dir
        self.step_budget = step_budget
        self.timeout_s = timeout_s
        self.stats = BridgeStats()
        self._lock = threading.Lock()
        self._in_flight = 0

    def _track_enter(self) -> None:
        with self._lock:
            self._in_flight += 1
            self.stats.calls += 1
            self.stats.max_in_flight = max(self.stats.max_in_flight, self._in_flight)

    def _track_exit(self, failed: bool) -> None:
        with self._lock:
            self._in_flight -= 1
            if failed:
                self.stats.failures += 1

    def call(self, src: str, module: str, function: str, args: list) -> list:
        """Invoke one MNCS kernel function; return the decoded `returned` list."""
        request = {
            "schema_version": "0.1",
            "target": {"module": module, "function": function},
            "arguments": args,
            "step_budget": self.step_budget,
        }
        self._track_enter()
        failed = True
        try:
            with tempfile.NamedTemporaryFile(
                "w", suffix=".json", delete=True
            ) as req_file:
                json.dump(request, req_file)
                req_file.flush()
                try:
                    proc = subprocess.run(
                        [
                            self.binary,
                            "execute",
                            os.path.join(self.src_dir, src),
                            req_file.name,
                        ],
                        capture_output=True,
                        check=False,
                        text=True,
                        timeout=self.timeout_s,
                    )
                except subprocess.TimeoutExpired as exc:
                    raise BridgeError(
                        f"mncs execute timed out: {module}::{function}"
                    ) from exc
                except OSError as exc:
                    raise BridgeError(
                        f"cannot run mncs binary {self.binary!r}: {exc}"
                    ) from exc
            try:
                result = json.loads(proc.stdout)
            except json.JSONDecodeError as exc:
                # No result envelope: the invocation itself failed
                # (missing binary and request-level rejections both land
                # here when nothing parseable was emitted).
                raise BridgeError(
                    f"mncs execute failed rc={proc.returncode} for "
                    f"{module}::{function}: {proc.stderr[:500]}"
                ) from exc
            status = result.get("status")
            if status != "returned":
                raise MNCSError(
                    module, function, str(status), json.dumps(result.get("failure"))
                )
            failed = False
            return result.get("returned", [])
        finally:
            self._track_exit(failed)

    def call_u64(self, src: str, module: str, function: str, args: list) -> int:
        returned = self.call(src, module, function, args)
        return int(returned[0]["integer"]["value"])

    def call_bool(self, src: str, module: str, function: str, args: list) -> bool:
        returned = self.call(src, module, function, args)
        return bool(returned[0]["boolean"]["value"])

    def call_record(self, src: str, module: str, function: str, args: list) -> dict:
        returned = self.call(src, module, function, args)
        return parse_record(returned[0])
