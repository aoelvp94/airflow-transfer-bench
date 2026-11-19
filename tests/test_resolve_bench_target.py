"""Scheduler decisions for the weekly PyPI check. No network."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from report_lib import latest_stable, pypi_stable_versions  # noqa: E402
from resolve_bench_target import decide  # noqa: E402


PYPI = {
    "releases": {
        "3.3.0": [{"yanked": False}],
        "3.4.0rc1": [{"yanked": False}],
        "3.4.0": [{"yanked": False}],
        "3.4.1": [{"yanked": True}],
        "3.5.0a1": [{"yanked": False}],
    }
}


class ResolveTest(unittest.TestCase):
    def test_latest_stable_skips_prereleases_and_yanked(self):
        self.assertEqual(pypi_stable_versions(PYPI), ["3.3.0", "3.4.0"])
        self.assertEqual(latest_stable(pypi_stable_versions(PYPI), minimum="3.4.0"), "3.4.0")

    def test_schedule_runs_when_only_a_laptop_result_exists(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "3.4.0" / "2026-11-19-apple-m4"
            run.mkdir(parents=True)
            (run / "environment.json").write_text(json.dumps({"ci": False, "env_slug": "apple-m4"}))
            decision = decide(
                mode="schedule",
                results=root,
                airflow_version="3.4.0",
                go_sdk_version="1.0.0",
                java_sdk_version="1.0.0",
                ts_sdk_version="1.0.0",
                rows=200000,
                runs=5,
                supervisor_schema_version="2026-10-30",
                pypi_payload=PYPI,
                sdk_latest={"go": "1.0.0", "java": "1.0.0", "typescript": "1.0.0"},
            )
            self.assertTrue(decision["should_run"])
            self.assertEqual(decision["airflow_version"], "3.4.0")
            self.assertEqual(decision["go_sdk_version"], "1.0.0")

    def test_schedule_skips_when_a_ci_run_exists(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "3.4.0" / "2026-11-19-github-actions"
            run.mkdir(parents=True)
            (run / "environment.json").write_text(json.dumps({"ci": True, "env_slug": "github-actions"}))
            decision = decide(
                mode="schedule",
                results=root,
                airflow_version="9.9.9",
                go_sdk_version="1.0.0",
                java_sdk_version="1.0.0",
                ts_sdk_version="1.0.0",
                rows=200000,
                runs=5,
                supervisor_schema_version="2026-10-30",
                pypi_payload=PYPI,
                sdk_latest={},
            )
            self.assertFalse(decision["should_run"])
            self.assertEqual(decision["airflow_version"], "3.4.0")

    def test_dispatch_uses_the_requested_version(self):
        decision = decide(
            mode="workflow_dispatch",
            results=Path("."),
            airflow_version="3.4.0",
            go_sdk_version="1.2.0",
            java_sdk_version="1.0.0",
            ts_sdk_version="1.0.0",
            rows=200000,
            runs=5,
            supervisor_schema_version="2026-10-30",
            pypi_payload=None,
            sdk_latest={},
        )
        self.assertTrue(decision["should_run"])
        self.assertEqual(decision["airflow_version"], "3.4.0")
        self.assertEqual(decision["go_sdk_version"], "1.2.0")

    def test_runs_below_five_are_rejected(self):
        with self.assertRaises(SystemExit):
            decide(
                mode="workflow_dispatch",
                results=Path("."),
                airflow_version="3.4.0",
                go_sdk_version="1.0.0",
                java_sdk_version="1.0.0",
                ts_sdk_version="1.0.0",
                rows=200000,
                runs=3,
                supervisor_schema_version="2026-10-30",
                pypi_payload=None,
                sdk_latest={},
            )


class RepoContractTest(unittest.TestCase):
    def test_readme_keeps_the_m4_sample_and_adds_the_new_sections(self):
        text = (ROOT / "README.md").read_text()
        self.assertIn("| transfer_go | go1.25.14 | 2.51 | 2.27 | 0.24 | 61.6 | 161 |", text)
        self.assertIn("| python_sql_to_s3 | CPython 3.12.16 | 14.03 | 13.05 | 0.82 | 10.7 | 1349 |", text)
        self.assertIn("146.6 MB", text)
        for heading in (
            "## Published results",
            "## Tracking across releases",
            "## How to contribute a run",
            "## Caveats",
        ):
            self.assertIn(heading, text)
        self.assertIn("GitHub-hosted runners are noisy", text)
        self.assertIn("customSmallerIsBetter", text)

    def test_license_stays_mit(self):
        self.assertTrue((ROOT / "LICENSE").read_text().startswith("MIT License"))

    def test_workflow_has_dispatch_schedule_and_the_benchmark_action(self):
        text = (ROOT / ".github/workflows/benchmark.yml").read_text()
        self.assertIn("workflow_dispatch:", text)
        self.assertIn("airflow_version:", text)
        self.assertIn("cron:", text)
        self.assertIn("customSmallerIsBetter", text)
        self.assertIn("benchmark-action/github-action-benchmark", text)
        self.assertIn("comment-on-alert: false", text)
        self.assertIn("apache-airflow", (ROOT / "scripts/resolve_bench_target.py").read_text())


if __name__ == "__main__":
    unittest.main()
