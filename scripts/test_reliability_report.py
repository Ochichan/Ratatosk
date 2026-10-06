#!/usr/bin/env python3
"""Hermetic aggregation regressions for reliability_report.sh.

The fixture commands are stubs. A PASS here verifies report aggregation only;
it is not evidence that any Ratatosk product, recovery, or performance check ran.
"""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("reliability_report.sh")
LOCAL_REQUIRED = (
    "format,build,lint,test,audit,deny,gap-ledger,strict-mode,recovery-matrix,"
    "backup-restore-drill,perf-guardrail"
)
STUB = """#!/bin/sh
set -eu
name=${0##*/}
if [ -n "${STUB_TRACE:-}" ]; then
  printf '%s|%s\\n' "$name" "$*" >> "$STUB_TRACE"
fi
if [ -n "${STUB_MISSING_CARGO_SUBCOMMAND:-}" ] \
  && [ "$name" = cargo ] \
  && [ "${1:-}" = "$STUB_MISSING_CARGO_SUBCOMMAND" ] \
  && [ "${2:-}" = --version ]; then
  exit 127
fi
if [ -n "${STUB_WARN_PATTERN:-}" ]; then
  case "$*" in
    *"$STUB_WARN_PATTERN"*)
      printf '%s\\n' "${STUB_WARNING_TEXT:-warning: synthetic visible warning}"
      ;;
  esac
fi
if [ -n "${STUB_INFO_PATTERN:-}" ]; then
  case "$*" in
    *"$STUB_INFO_PATTERN"*) printf '%s\\n' 'note: the word warning is documentation' ;;
  esac
fi
if [ -n "${STUB_FAIL_PATTERN:-}" ]; then
  case "$*" in
    *"$STUB_FAIL_PATTERN"*)
      printf '%s\\n' "synthetic failure for $*" >&2
      exit "${STUB_FAIL_CODE:-7}"
      ;;
  esac
fi
exit 0
"""


class ReliabilityReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="reliability-report-test-")
        self.root = Path(self.temp_dir.name)
        self.scripts = self.root / "scripts"
        self.bin_dir = self.root / "stub-bin"
        self.output = self.root / "out"
        self.scripts.mkdir()
        self.bin_dir.mkdir()
        (self.root / "Cargo.toml").write_text(
            '[package]\nname = "fixture"\nversion = "9.9.9"\n', encoding="utf-8"
        )
        self.report_script = self.scripts / "reliability_report.sh"
        shutil.copyfile(SCRIPT, self.report_script)

        for name in ("cargo", "cargo-audit", "cargo-deny", "python3", "bash"):
            stub = self.bin_dir / name
            stub.write_text(STUB, encoding="utf-8")
            stub.chmod(0o755)

        for name in ("recovery_matrix.sh", "backup_restore_drill.sh"):
            placeholder = self.scripts / name
            placeholder.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
            placeholder.chmod(0o755)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def run_report(self, **overrides: str) -> tuple[subprocess.CompletedProcess[str], str]:
        trace = self.root / "stub-trace.log"
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin_dir}:/usr/bin:/bin",
                "RELIABILITY_OUT_DIR": str(self.output),
                "STUB_TRACE": str(trace),
                "LC_ALL": "C",
            }
        )
        env.update(overrides)
        result = subprocess.run(
            ["/bin/bash", str(self.report_script)],
            cwd=self.root,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
            check=False,
        )
        reports = list(self.output.glob("reliability-report-*.md"))
        self.assertEqual(1, len(reports), result.stdout)
        return result, reports[0].read_text(encoding="utf-8")

    def test_custom_pass_is_scoped_to_every_declared_check(self) -> None:
        result, report = self.run_report(
            RELIABILITY_REQUIRED_CHECKS=LOCAL_REQUIRED,
        )

        self.assertEqual(0, result.returncode, result.stdout)
        self.assertIn("`format,build,lint,test,audit,deny,gap-ledger,strict-mode,", report)
        self.assertIn("Policy scope: `custom`", report)
        self.assertIn("optional results are informational", report)
        self.assertIn("- PASS `perf-guardrail` (required)", report)
        self.assertIn("- SKIP `redis-differential` (optional)", report)
        self.assertIn("not executed by this report", report)
        self.assertIn("**Overall: PASS (SCOPED)**", report)
        self.assertIn("not full release qualification", report)

    def test_warning_lines_with_zero_command_exit_fail_policy_gate(self) -> None:
        warning_lines = (
            "warning: synthetic compiler warning",
            "warning[license-not-encountered]: synthetic policy warning",
            "WARN synthetic runtime warning",
            "[service WARN] synthetic bracketed warning",
            "2026-09-13T10:00:00Z WARN synthetic timestamped warning",
            "\x1b[33mwarning:\x1b[0m synthetic colored warning",
        )
        for warning_line in warning_lines:
            with self.subTest(warning_line=warning_line):
                shutil.rmtree(self.output, ignore_errors=True)
                result, report = self.run_report(
                    RELIABILITY_REQUIRED_CHECKS=LOCAL_REQUIRED,
                    STUB_WARN_PATTERN="fmt --all --check",
                    STUB_WARNING_TEXT=warning_line,
                )

                self.assertEqual(1, result.returncode, result.stdout)
                self.assertIn(warning_line, result.stdout)
                self.assertIn("- FAIL `format` (required)", report)
                self.assertIn("exit=0; warning-line-detected=", report)
                self.assertIn("**Overall: FAIL**", report)
                format_logs = list(self.output.glob("reliability-format-*.log"))
                self.assertEqual(1, len(format_logs))
                self.assertIn(warning_line, format_logs[0].read_text())

    def test_warning_word_in_non_warning_prose_does_not_fail(self) -> None:
        result, report = self.run_report(
            RELIABILITY_REQUIRED_CHECKS=LOCAL_REQUIRED,
            STUB_INFO_PATTERN="fmt --all --check",
        )

        self.assertEqual(0, result.returncode, result.stdout)
        self.assertIn("note: the word warning is documentation", result.stdout)
        self.assertIn("- PASS `format` (required)", report)
        self.assertIn("**Overall: PASS (SCOPED)**", report)

    def test_default_release_is_incomplete_while_diff_and_fuzz_are_unwired(self) -> None:
        result, report = self.run_report()

        self.assertEqual(1, result.returncode, result.stdout)
        self.assertIn("Policy scope: `default-release`", report)
        self.assertIn("- SKIP `redis-differential` (required)", report)
        self.assertIn("- SKIP `resp-fuzz` (required)", report)
        self.assertIn("**Overall: INCOMPLETE**", report)

    def test_required_skip_is_incomplete(self) -> None:
        result, report = self.run_report(
            RELIABILITY_REQUIRED_CHECKS=LOCAL_REQUIRED,
            RELIABILITY_RUN_RECOVERY="0",
        )

        self.assertEqual(1, result.returncode, result.stdout)
        self.assertIn("- SKIP `recovery-matrix` (required)", report)
        self.assertIn("disabled via RELIABILITY_RUN_RECOVERY=0", report)
        self.assertIn("**Overall: INCOMPLETE**", report)

    def test_missing_required_tool_is_incomplete(self) -> None:
        (self.bin_dir / "cargo-audit").unlink()
        result, report = self.run_report(
            RELIABILITY_REQUIRED_CHECKS=LOCAL_REQUIRED,
            STUB_MISSING_CARGO_SUBCOMMAND="audit",
        )

        self.assertEqual(1, result.returncode, result.stdout)
        self.assertIn("- SKIP `audit` (required)", report)
        self.assertIn("cargo-audit not installed", report)
        self.assertIn("**Overall: INCOMPLETE**", report)

    def test_failed_command_records_actual_exit_and_visible_output(self) -> None:
        result, report = self.run_report(
            RELIABILITY_REQUIRED_CHECKS=LOCAL_REQUIRED,
            STUB_FAIL_PATTERN="clippy",
            STUB_FAIL_CODE="23",
        )

        self.assertEqual(1, result.returncode, result.stdout)
        self.assertIn("synthetic failure for clippy", result.stdout)
        self.assertIn("- FAIL `lint` (required)", report)
        self.assertIn("exit=23; log=", report)
        self.assertIn("**Overall: FAIL**", report)

    def test_missing_required_result_is_detected_by_completeness_check(self) -> None:
        source = self.report_script.read_text(encoding="utf-8")
        recorded = (
            'add_result redis-differential SKIP "Redis/Valkey differential subset" \\\n'
            '  "run via .github/workflows/redis-interop.yml; not executed by this report"'
        )
        self.assertIn(recorded, source)
        self.report_script.write_text(
            source.replace(recorded, ': # deliberately omitted fixture result'),
            encoding="utf-8",
        )

        result, report = self.run_report(
            RELIABILITY_REQUIRED_CHECKS="redis-differential"
        )

        self.assertEqual(1, result.returncode, result.stdout)
        self.assertIn(
            "- ERROR `redis-differential` (required): no result recorded", report
        )
        self.assertIn("**Overall: INCOMPLETE**", report)

    def test_malformed_required_policies_fail_closed_before_checks(self) -> None:
        cases = (
            ("", "must be a non-empty comma-separated list"),
            ("format,unknown", "unknown required check ID 'unknown'"),
            ("format,format", "duplicate required check ID 'format'"),
            ("format\nresp-fuzz", "must not contain control characters"),
            ("\nformat", "must not contain control characters"),
            ("format\rresp-fuzz", "must not contain control characters"),
            ("format\x01resp-fuzz", "must not contain control characters"),
        )
        for policy, message in cases:
            with self.subTest(policy=policy):
                shutil.rmtree(self.output, ignore_errors=True)
                trace = self.root / "stub-trace.log"
                trace.unlink(missing_ok=True)
                result, report = self.run_report(
                    RELIABILITY_REQUIRED_CHECKS=policy
                )
                self.assertEqual(2, result.returncode, result.stdout)
                self.assertIn(message, report)
                self.assertIn("**Overall: ERROR**", report)
                if any(character in policy for character in "\n\r\x01"):
                    self.assertIn("`<invalid: contains control characters>`", report)
                self.assertFalse(trace.exists(), "a check command ran before policy validation")


if __name__ == "__main__":
    unittest.main()
