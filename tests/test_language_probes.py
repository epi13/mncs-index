"""Slice-4 language probes (PRESS-001/003/006/007).

Each test runs the smallest legal .mncs program for one granted
capability through the real executor and pins the recorded verdict:
granted use returns the expected value with the expected effect, and
ungranted use fails closed (never a value).

TEMPORARY INFRASTRUCTURE note: these tests shell out to
`experiment run` (per-invocation backend compile, ~0.1-0.5 s) rather
than the fast `execute` path, because host effects are only realizable
on the experiment path (PRESS-010). That cost is why the probes cannot
back production kernel calls.
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPRO = os.path.join(REPO, "pressure", "reproducers")
BACKEND = "mncs-research-bytecode"


def library_dir() -> str:
    if os.environ.get("MNCS_LIBRARY_PATH"):
        return os.environ["MNCS_LIBRARY_PATH"]
    return os.path.normpath(os.path.join(REPO, "..", "mncs-language", "library"))


@pytest.fixture(scope="module")
def lib() -> str:
    path = library_dir()
    if not os.path.isdir(path):
        pytest.skip(f"mncs-language library not found: {path}")
    return path


def run_experiment(binary: str, lib: str, prog: str, corpus: str, grants: list,
                   tmp_path, name: str, expect_ok: bool = True) -> dict:
    out = str(tmp_path / f"{name}.json")
    cmd = [binary, "experiment", "run", os.path.join(REPRO, prog),
           "--backend", BACKEND, "--corpus", os.path.join(REPRO, corpus)]
    cmd.extend(grants)
    env = dict(os.environ, MNCS_LIBRARY_PATH=lib)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                          cwd=REPO, env=env)
    if expect_ok:
        assert proc.returncode == 0, \
            f"{name}: rc={proc.returncode} {proc.stderr[:500]}"
    with open(out, "w") as fh:
        fh.write(proc.stdout)
    with open(out) as fh:
        return json.load(fh)


def assert_case(result: dict, case_id: str, returned=None) -> dict:
    cases = [c for c in result["cases"] if c["case_id"] == case_id]
    assert len(cases) == 1, f"expected one case {case_id}"
    case = cases[0]
    assert case["status"] == "returned", case
    assert case.get("expectation_met") is True, case
    assert case.get("effects_met") is True, case
    if returned is not None:
        assert case["returned"] == returned, case["returned"]
    return case


def assert_fails_closed(result: dict) -> None:
    leaked = [c["case_id"] for c in result["cases"]
              if c["status"] == "returned" and c.get("expectation_met")]
    assert not leaked, f"ungranted call produced a value: {leaked}"


def grant_read() -> list:
    return ["--grant-read",
            f"probe_reader={os.path.join(REPRO, 'probe-host-read-grant.txt')}"]


def test_host_read_granted(binary, lib, tmp_path):
    result = run_experiment(binary, lib, "probe-host-read.mncs",
                            "probe-host-read-corpus.json", grant_read(),
                            tmp_path, "read")
    case = assert_case(result, "blob-len", returned=[
        {"integer": {"value": 17, "type": {"bits": 64, "signed": False}}}])
    assert case["effects"][0]["kind"] == "host_read"
    assert case["effects"][0]["target"] == "blob_read"
    assert case["effects"][0]["capability"] == "probe_reader"


def test_host_read_without_grant_fails_closed(binary, lib, tmp_path):
    result = run_experiment(binary, lib, "probe-host-read.mncs",
                            "probe-host-read-corpus.json", [],
                            tmp_path, "read-neg", expect_ok=False)
    assert_fails_closed(result)


def test_sha256_nist_abc_vector(binary, lib, tmp_path):
    result = run_experiment(binary, lib, "probe-sha256-abc.mncs",
                            "probe-sha256-abc-corpus.json",
                            ["--grant-crypto", "verifier"], tmp_path, "sha")
    digest = ("ba7816bf8f01cfea414140de5dae2223"
              "b00361a396177a9cb410ff61f20015ad")
    want = [{"byte": {"value": int(digest[i:i + 2], 16)}}
            for i in range(0, 64, 2)]
    case = assert_case(result, "digest-abc", returned=[
        {"sequence": {"values": want}}])
    assert case["effects"][0]["provenance"] == "crypto:sha256"


def test_sha256_without_grant_fails_closed(binary, lib, tmp_path):
    result = run_experiment(binary, lib, "probe-sha256-abc.mncs",
                            "probe-sha256-abc-corpus.json", [],
                            tmp_path, "sha-neg", expect_ok=False)
    assert_fails_closed(result)


def test_clock_read_granted(binary, lib, tmp_path):
    result = run_experiment(binary, lib, "probe-clock.mncs",
                            "probe-clock-corpus.json",
                            ["--grant-time", "ticker"], tmp_path, "clock")
    case = assert_case(result, "past-expired", returned=[
        {"boolean": {"value": True}}])
    assert case["effects"][0]["kind"] == "clock_read"
    assert case["effects"][0]["capability"] == "ticker"


def test_clock_without_grant_fails_closed(binary, lib, tmp_path):
    result = run_experiment(binary, lib, "probe-clock.mncs",
                            "probe-clock-corpus.json", [], tmp_path, "clock-neg",
                            expect_ok=False)
    assert_fails_closed(result)


def run_execute(binary: str, lib: str, function: str) -> dict:
    request = {"schema_version": "0.1",
               "target": {"module": "pressure.probe_task_lifecycle",
                          "function": function},
               "arguments": [], "step_budget": 100000}
    env = dict(os.environ, MNCS_LIBRARY_PATH=lib)
    proc = subprocess.run(
        [binary, "execute", os.path.join(REPRO, "probe-task-lifecycle.mncs"),
         "/dev/stdin"],
        input=json.dumps(request), capture_output=True, text=True,
        timeout=60, cwd=REPO, env=env)
    assert proc.returncode == 0, f"{function}: {proc.stdout[:500]}"
    return json.loads(proc.stdout)


def test_task_lifecycle_happy_path(binary, lib):
    result = run_execute(binary, lib, "lifecycle_done")
    assert result["status"] == "returned", result
    assert result["returned"] == [{"boolean": {"value": True}}]


def test_task_cancel_is_terminal(binary, lib):
    result = run_execute(binary, lib, "cancel_terminal")
    assert result["status"] == "returned", result
    assert result["returned"] == [{"boolean": {"value": True}}]


def test_task_invalid_transitions_rejected(binary, lib):
    result = run_execute(binary, lib, "invalid_rejected")
    assert result["status"] == "returned", result
    assert result["returned"] == [
        {"integer": {"value": 0, "type": {"bits": 64, "signed": True}}}]


def test_probes_elaborate_cleanly(binary, lib):
    env = dict(os.environ, MNCS_LIBRARY_PATH=lib)
    for prog in ("probe-host-read.mncs", "probe-sha256-abc.mncs",
                 "probe-clock.mncs", "probe-task-lifecycle.mncs"):
        proc = subprocess.run(
            [binary, "source-study", os.path.join(REPRO, prog),
             "--node-id", "test"],
            capture_output=True, text=True, timeout=60, cwd=REPO, env=env)
        assert proc.returncode == 0, f"{prog}: {proc.stderr[:300]}"
        diags = json.loads(proc.stdout).get("diagnostics", [])
        errs = [d for d in diags
                if d.get("severity") == "error"
                or str(d.get("code", ""))[:3] in ("MNE", "MNB", "MNP")]
        assert not errs, f"{prog}: {errs}"
