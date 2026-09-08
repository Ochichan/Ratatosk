#!/usr/bin/env python3
"""Regression checks for the IPC mode of perf_guardrail_check.py."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "perf_guardrail_check.py"


class IpcGuardrailTests(unittest.TestCase):
    def run_checker(self, artifact: Path, *extra_args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(CHECKER), "--ipc", str(artifact), *extra_args],
            cwd=ROOT,
            capture_output=True,
            check=False,
            text=True,
        )

    def run_artifact(self, artifact_data: dict[str, object], *extra_args: str) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as temporary_directory:
            artifact = Path(temporary_directory) / "ipc.json"
            artifact.write_text(json.dumps(artifact_data), encoding="utf-8")
            return self.run_checker(artifact, *extra_args)

    def test_concurrent_row_is_printed_but_not_gated(self) -> None:
        result = self.run_artifact(
            {
                "schema_version": "ratatosk-ipc-bench/v1",
                "results": [
                    {
                        "server": "ratatosk",
                        "status": "ok",
                        "transport": "tcp",
                        "payload": "ping",
                        "concurrency": 4,
                        "p99_us": 999.0,
                    }
                ],
            }
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("conns=4", result.stdout)
        self.assertIn("guard=not-gated", result.stdout)

    def test_single_connection_row_above_guard_fails(self) -> None:
        result = self.run_artifact(
            {
                "schema_version": "ratatosk-ipc-bench/v1",
                "results": [
                    {
                        "server": "ratatosk",
                        "status": "ok",
                        "transport": "tcp",
                        "payload": "ping",
                        "concurrency": 1,
                        "p99_us": 60.01,
                    }
                ],
            }
        )

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("conns=1", result.stdout)
        self.assertIn("ping/tcp conns=1 p99 above guardrail", result.stdout)

    def test_shm_defaults_apply_to_single_connection_rows(self) -> None:
        result = self.run_artifact(
            {
                "schema_version": "ratatosk-ipc-bench/v1",
                "results": [
                    {
                        "server": "ratatosk",
                        "status": "ok",
                        "transport": "shm",
                        "payload": "ping",
                        "concurrency": 1,
                        "p99_us": 20.01,
                    },
                    {
                        "server": "ratatosk",
                        "status": "ok",
                        "transport": "shm",
                        "payload": "set64",
                        "concurrency": 1,
                        "p99_us": 40.01,
                    },
                ],
            }
        )

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("transport=shm conns=1 p99=20.010us guard=20.000us", result.stdout)
        self.assertIn("transport=shm conns=1 p99=40.010us guard=40.000us", result.stdout)

    def test_invalid_concurrency_failure_identifies_the_row_connection_count(self) -> None:
        result = self.run_artifact(
            {
                "schema_version": "ratatosk-ipc-bench/v1",
                "results": [
                    {
                        "server": "ratatosk",
                        "status": "ok",
                        "transport": "tcp",
                        "payload": "ping",
                        "concurrency": 0,
                        "p99_us": 1.0,
                    }
                ],
            }
        )

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("conns=0", result.stderr)


if __name__ == "__main__":
    unittest.main()
