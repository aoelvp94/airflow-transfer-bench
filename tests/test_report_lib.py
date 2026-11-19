"""Unit tests for the report math and the results index. No Airflow, no Docker."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "dags"))

from bench.config import BYTE_IDENTICAL_TASK_IDS  # noqa: E402
from report_lib import (  # noqa: E402
    CUSTOM_TASK_IDS,
    allocate_run_dir,
    benchmark_payload,
    bootstrap_median_ci,
    choose_env_slug,
    cpu_model_slug,
    evaluate_md5,
    has_ci_run,
    human_bytes,
    parse_cpu_max,
    parse_cpu_model,
    parse_java_version,
    parse_mem_max,
    parse_node_version,
    render_index,
    split_warmup,
    stat_block,
    summarize_tasks,
    write_index,
)


class StatsTest(unittest.TestCase):
    def test_warmup_keeps_one_measured_run(self):
        warm, measured = split_warmup(["a", "b", "c", "d", "e"], warmup=1)
        self.assertEqual(warm, ["a"])
        self.assertEqual(measured, ["b", "c", "d", "e"])

    def test_single_run_is_not_discarded(self):
        warm, measured = split_warmup(["only"])
        self.assertEqual(warm, [])
        self.assertEqual(measured, ["only"])

    def test_median_min_max_and_constant_ci(self):
        block = stat_block([1.0, 1.0, 1.0, 1.0])
        self.assertEqual(block["median"], 1.0)
        self.assertEqual(block["min"], 1.0)
        self.assertEqual(block["max"], 1.0)
        self.assertEqual(block["n"], 4)
        self.assertEqual(bootstrap_median_ci([2.0, 2.0, 2.0, 2.0]), [2.0, 2.0])

    def test_even_median_averages_the_middle_pair(self):
        block = stat_block([1.0, 3.0, 5.0, 7.0])
        self.assertEqual(block["median"], 4.0)
        self.assertEqual(block["min"], 1.0)
        self.assertEqual(block["max"], 7.0)
        lo, hi = block["ci95"]
        self.assertLessEqual(lo, block["median"])
        self.assertGreaterEqual(hi, block["median"])

    def test_summarize_ignores_warmup_caller_already_filtered(self):
        records = [
            {"task_id": "transfer_go", "state": "success", "runtime": "go1.25.1", "rows": 10,
             "task_s": 1.0, "work_s": 0.8, "cpu_s": 0.5, "overhead_s": 0.2, "mib_per_s": 10.0, "peak_rss_mb": 100},
            {"task_id": "transfer_go", "state": "success", "runtime": "go1.25.1", "rows": 10,
             "task_s": 3.0, "work_s": 2.0, "cpu_s": 1.5, "overhead_s": 1.0, "mib_per_s": 12.0, "peak_rss_mb": 110},
        ]
        stats = summarize_tasks(records)
        self.assertEqual(stats["transfer_go"]["task_s"]["median"], 2.0)
        self.assertEqual(stats["transfer_go"]["n"], 2)


class Md5GateTest(unittest.TestCase):
    def test_custom_match_is_a_pass_even_if_pandas_differs(self):
        digest = "abc"
        other = "def"
        info = evaluate_md5({
            "r1": {task: digest for task in CUSTOM_TASK_IDS} | {"python_sql_to_s3": other},
            "r2": {task: digest for task in CUSTOM_TASK_IDS} | {"python_sql_to_s3": other},
        })
        self.assertTrue(info["ok"])
        self.assertFalse(info["all_five_match"])
        self.assertEqual(info["digest"], digest)

    def test_all_five_match(self):
        digest = "abc"
        runs = {f"r{i}": {task: digest for task in ("python_sql_to_s3", *CUSTOM_TASK_IDS)} for i in range(2)}
        info = evaluate_md5(runs)
        self.assertTrue(info["ok"])
        self.assertTrue(info["all_five_match"])

    def test_mismatch_fails(self):
        digests = {task: "aaa" for task in CUSTOM_TASK_IDS}
        digests["transfer_go"] = "bbb"
        info = evaluate_md5({"r1": digests})
        self.assertFalse(info["ok"])
        self.assertTrue(info["failures"])

    def test_task_ids_match_the_dag_config(self):
        self.assertEqual(tuple(BYTE_IDENTICAL_TASK_IDS), CUSTOM_TASK_IDS)


class LayoutTest(unittest.TestCase):
    def test_slug_and_cgroup_parsers(self):
        self.assertEqual(cpu_model_slug("Intel(R) Xeon(R) Processor"), "intel-xeon-processor")
        self.assertEqual(cpu_model_slug("Apple M4 @ 4.4GHz"), "apple-m4")
        self.assertEqual(choose_env_slug("Apple M4", override=None, ci=True), "github-actions")
        self.assertEqual(choose_env_slug("Apple M4", override="lab-a", ci=True), "lab-a")
        self.assertEqual(parse_cpu_model("model name\t: Intel(R) Xeon(R) Processor\n"), "Intel(R) Xeon(R) Processor")
        self.assertEqual(parse_cpu_max("200000 100000"), 2.0)
        self.assertIsNone(parse_cpu_max("max 100000"))
        self.assertEqual(parse_mem_max("3221225472"), 3221225472)
        self.assertIsNone(parse_mem_max("max"))
        self.assertIsNone(parse_mem_max(str(1 << 62)))
        self.assertEqual(human_bytes(3 * 1024**3), "3 GiB")
        self.assertEqual(parse_java_version('openjdk version "17.0.21" 2026-01-01'), "17.0.21")
        self.assertEqual(parse_node_version("v22.23.5"), "v22.23.5")

    def test_allocate_overwrites_the_same_run_ids(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = allocate_run_dir(root, "3.4.0", "2026-11-19", "intel-xeon", ["r1", "r2"])
            first.mkdir(parents=True)
            (first / "environment.json").write_text(json.dumps({"run_ids": ["r1", "r2"]}))
            again = allocate_run_dir(root, "3.4.0", "2026-11-19", "intel-xeon", ["r1", "r2"])
            self.assertEqual(first, again)
            other = allocate_run_dir(root, "3.4.0", "2026-11-19", "intel-xeon", ["r3"])
            self.assertNotEqual(other, first)
            self.assertTrue(other.name.endswith("-2"))

    def test_empty_index_does_not_invent_the_m4_sample(self):
        text = render_index([])
        self.assertIn("No published runs yet", text)
        self.assertNotIn("2.51", text)
        self.assertNotIn("61.6", text)
        self.assertIn("GitHub-hosted runners are noisy", text)

    def test_index_trend_is_per_environment(self):
        def run(env, airflow, task_s, ci=False):
            return {
                "airflow_version": airflow,
                "date": "2026-11-19",
                "env_slug": env,
                "rows": 200000,
                "n": 4,
                "git_sha": "abc123def456",
                "protocol_ok": True,
                "ci": ci,
                "sdk": {"go": "1.0.0", "java": "1.0.0", "typescript": "1.0.0"},
                "md5": {"ok": True, "digest": "deadbeef"},
                "relpath": f"{airflow}/2026-11-19-{env}",
                "stats": {"tasks": {"transfer_go": {"task_s": {"median": task_s}, "mib_per_s": {"median": 10.0}}}},
            }
        text = render_index([
            run("github-actions", "3.4.0", 9.0, ci=True),
            run("apple-m4", "3.4.0", 2.5),
        ])
        self.assertIn("### `github-actions`", text)
        self.assertIn("### `apple-m4`", text)
        self.assertIn("9.00", text)
        self.assertIn("2.50", text)

    def test_benchmark_payload_is_smaller_is_better_shape(self):
        meta = {
            "airflow_version": "3.4.0",
            "rows": 200000,
            "env_slug": "github-actions",
            "ci": True,
            "sdk": {"go": "1.0.0", "java": "1.0.0", "typescript": "1.0.0"},
        }
        stats = {"transfer_go": {"task_s": {"median": 1.5, "min": 1.2, "max": 1.8}}}
        points = benchmark_payload(meta, stats)
        self.assertEqual(points[0]["name"], "transfer_go task_s (200000 rows)")
        self.assertEqual(points[0]["unit"], "s")
        self.assertEqual(points[0]["value"], 1.5)
        self.assertIsInstance(points[0]["value"], float)
        self.assertIn("noisy", points[0]["extra"])

    def test_write_index_round_trip(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            text = write_index(root)
            self.assertTrue((root / "README.md").is_file())
            self.assertIn("No published runs yet", text)
            self.assertIn("environment.json", text)


class CiRunDetectionTest(unittest.TestCase):
    def test_a_laptop_run_does_not_count_as_ci(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "3.4.0" / "2026-11-19-apple-m4"
            run.mkdir(parents=True)
            (run / "environment.json").write_text(json.dumps({"ci": False, "env_slug": "apple-m4"}))
            self.assertFalse(has_ci_run(root, "3.4.0"))
            ci = root / "3.4.0" / "2026-11-20-github-actions"
            ci.mkdir()
            (ci / "environment.json").write_text(json.dumps({"ci": True, "env_slug": "github-actions"}))
            self.assertTrue(has_ci_run(root, "3.4.0"))


if __name__ == "__main__":
    unittest.main()
