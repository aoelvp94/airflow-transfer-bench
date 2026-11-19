"""Pure helpers for the transfer benchmark report.

No Airflow imports. ``scripts/report.py`` collects rows from the metadata DB
and calls into this module to drop the warm-up run, summarize, and write the
published-results index.
"""

from __future__ import annotations

import json
import math
import random
import re
import statistics
from pathlib import Path

# Hand-written implementations. Must stay aligned with bench.config.BYTE_IDENTICAL_TASK_IDS.
CUSTOM_TASK_IDS = (
    "transfer_python",
    "transfer_go",
    "transfer_java",
    "transfer_typescript",
)
TRANSFER_TASK_IDS = ("python_sql_to_s3",) + CUSTOM_TASK_IDS
TREND_TASKS = (
    "transfer_go",
    "transfer_java",
    "transfer_typescript",
    "transfer_python",
    "python_sql_to_s3",
)
TREND_LABELS = {
    "transfer_go": "go",
    "transfer_java": "java",
    "transfer_typescript": "typescript",
    "transfer_python": "python",
    "python_sql_to_s3": "sql_to_s3",
}
STAT_KEYS = ("task_s", "work_s", "cpu_s", "overhead_s", "mib_per_s", "peak_rss_mb")
CSV_FIELDS = (
    "run_id",
    "warmup",
    "task_id",
    "state",
    "language",
    "impl",
    "runtime",
    "rows",
    "bytes",
    "md5",
    "task_s",
    "work_s",
    "cpu_s",
    "overhead_s",
    "mib_per_s",
    "first_row_s",
    "peak_rss_mb",
    "key",
    "started",
)

CI_NOISE_NOTE = (
    "GitHub-hosted runners are noisy. These numbers track trends and regressions. "
    "They are not absolute speed and are not comparable with the Apple M4 sample "
    "in the repository README or with results from any other environment."
)

_STABLE_VERSION = re.compile(r"^\d+(?:\.\d+)+$")


def sanitize_slug(text: str, max_len: int = 48) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    cleaned = cleaned[:max_len].strip("-")
    return cleaned or "unknown"


def cpu_model_slug(model: str) -> str:
    cut = re.split(r"\s+@\s+", model or "", maxsplit=1)[0]
    cut = re.sub(r"\((?:r|tm)\)", " ", cut, flags=re.IGNORECASE)
    cut = re.sub(r"\bcpu\b", " ", cut, flags=re.IGNORECASE)
    return sanitize_slug(cut)


def choose_env_slug(cpu_model: str, *, override: str | None, ci: bool) -> str:
    if override and override.strip():
        return sanitize_slug(override)
    if ci:
        return "github-actions"
    return cpu_model_slug(cpu_model)


def sanitize_run_id(run_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", run_id or "").strip("-")
    return (cleaned[:120] or "run")


def parse_cpu_model(cpuinfo: str) -> str | None:
    for line in cpuinfo.splitlines():
        if line.lower().startswith("model name"):
            value = line.split(":", 1)[1].strip()
            return value or None
    return None


def parse_cpu_max(text: str) -> float | None:
    """cgroup v2 ``cpu.max`` (``quota period`` in microseconds). ``max`` means no cap."""
    parts = (text or "").split()
    if len(parts) != 2 or parts[0] == "max":
        return None
    try:
        quota, period = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    if quota <= 0 or period <= 0:
        return None
    return round(quota / period, 3)


def cpu_limit_from_quota(quota: int, period: int) -> float | None:
    """cgroup v1: a negative quota means the container is not capped."""
    if quota <= 0 or period <= 0:
        return None
    return round(quota / period, 3)


def parse_mem_max(text: str) -> int | None:
    raw = (text or "").strip()
    if not raw or raw == "max":
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    # cgroup v1 reports a huge sentinel when the limit is unset.
    if value <= 0 or value >= 1 << 60:
        return None
    return value


def human_bytes(n: int | None) -> str | None:
    if n is None:
        return None
    gib = n / (1024**3)
    if abs(gib - round(gib)) < 0.08:
        return f"{round(gib)} GiB"
    mib = n / (1024**2)
    return f"{round(mib)} MiB"


def parse_java_version(text: str) -> str | None:
    match = re.search(r'version "([^"]+)"', text or "")
    if match:
        return match.group(1)
    match = re.search(r"\b(\d+\.\d+\.\d+)\b", text or "")
    return match.group(1) if match else None


def parse_node_version(text: str) -> str | None:
    match = re.search(r"v\d+\.\d+\.\d+", text or "")
    return match.group(0) if match else None


def parse_go_runtime(runtime: str) -> str | None:
    match = re.search(r"go\d+\.\d+(?:\.\d+)?", runtime or "")
    return match.group(0) if match else None


def parse_numbered_runtime(runtime: str) -> str | None:
    match = re.search(r"(\d+\.\d+\.\d+)", runtime or "")
    return match.group(1) if match else None


def is_stable_version(version: str) -> bool:
    return bool(_STABLE_VERSION.fullmatch(version or ""))


def version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def latest_stable(versions: list[str], *, minimum: str) -> str | None:
    floor = version_key(minimum)
    eligible = [v for v in versions if is_stable_version(v) and version_key(v) >= floor]
    if not eligible:
        return None
    return max(eligible, key=version_key)


def pypi_stable_versions(payload: dict) -> list[str]:
    found = []
    for version, files in (payload.get("releases") or {}).items():
        if not is_stable_version(version) or not files:
            continue
        if all(item.get("yanked") for item in files):
            continue
        found.append(version)
    return found


def has_ci_run(results: Path, version: str) -> bool:
    """True when this Airflow version already has a GitHub-hosted published run.

    A laptop or other local run under the same version does not count: the
    scheduled workflow exists to put a ``github-actions`` point on the trend.
    """
    parent = results / version
    if not parent.is_dir():
        return False
    for env_path in parent.glob("*/environment.json"):
        try:
            meta = json.loads(env_path.read_text())
        except json.JSONDecodeError:
            continue
        if meta.get("ci") is True or meta.get("env_slug") == "github-actions":
            return True
    return False


def bootstrap_median_ci(
    values: list[float],
    *,
    iters: int = 5000,
    alpha: float = 0.05,
    seed: int = 20261119,
) -> list[float] | None:
    """Percentile bootstrap CI of the median. ``None`` when n < 2."""
    if len(values) < 2:
        return None
    rng = random.Random(seed)
    n = len(values)
    medians = []
    for _ in range(iters):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        medians.append(statistics.median(sample))
    medians.sort()
    lo_i = int(math.floor((alpha / 2) * iters))
    hi_i = int(math.ceil((1 - alpha / 2) * iters) - 1)
    lo_i = min(max(lo_i, 0), iters - 1)
    hi_i = min(max(hi_i, 0), iters - 1)
    return [round(medians[lo_i], 4), round(medians[hi_i], 4)]


def stat_block(values: list[float]) -> dict | None:
    if not values:
        return None
    return {
        "median": round(float(statistics.median(values)), 4),
        "min": round(float(min(values)), 4),
        "max": round(float(max(values)), 4),
        "ci95": bootstrap_median_ci(values),
        "n": len(values),
    }


def split_warmup(ordered_run_ids: list[str], warmup: int = 1) -> tuple[list[str], list[str]]:
    """Drop the earliest run when there is at least one run left to measure."""
    if warmup <= 0 or len(ordered_run_ids) < 2:
        return [], list(ordered_run_ids)
    drop = min(warmup, len(ordered_run_ids) - 1)
    return list(ordered_run_ids[:drop]), list(ordered_run_ids[drop:])


def summarize_tasks(records: list[dict]) -> dict[str, dict]:
    """Per-task median/min/max over the records passed in (already warm-up-free)."""
    grouped: dict[str, list[dict]] = {}
    for record in records:
        if record.get("state") != "success":
            continue
        grouped.setdefault(record["task_id"], []).append(record)
    summary = {}
    for task_id, rows in grouped.items():
        block: dict = {
            "runtime": next((r.get("runtime") for r in reversed(rows) if r.get("runtime")), ""),
            "rows": rows[-1].get("rows"),
            "n": len(rows),
        }
        for key in STAT_KEYS:
            nums = [float(r[key]) for r in rows if r.get(key) is not None]
            stats = stat_block(nums)
            if stats:
                block[key] = stats
        summary[task_id] = block
    return summary


def evaluate_md5(digests_by_run: dict[str, dict[str, str | None]]) -> dict:
    """Hard gate: every run's hand-written outputs share one MD5, including warm-up.

    ``python_sql_to_s3`` is recorded. A mismatch there does not fail the gate;
    pandas CSV is not on the shared grammar. It does clear ``all_five_match``.
    """
    failures: list[str] = []
    shared: str | None = None
    if not digests_by_run:
        return {"ok": False, "digest": None, "all_five_match": False, "failures": ["no runs"]}
    for run_id, digests in digests_by_run.items():
        missing = [task for task in CUSTOM_TASK_IDS if not digests.get(task)]
        if missing:
            failures.append(f"{run_id}: missing MD5 for {', '.join(missing)}")
            continue
        values = [str(digests[task]) for task in CUSTOM_TASK_IDS]
        if len(set(values)) != 1:
            detail = ", ".join(f"{task}={digests[task]}" for task in CUSTOM_TASK_IDS)
            failures.append(f"{run_id}: {detail}")
            continue
        if shared is None:
            shared = values[0]
        elif values[0] != shared:
            failures.append(f"{run_id}: md5 {values[0]} differs from {shared}")
    all_digests: list[str] = []
    sql_present = True
    for digests in digests_by_run.values():
        if not digests.get("python_sql_to_s3"):
            sql_present = False
        for task in ("python_sql_to_s3", *CUSTOM_TASK_IDS):
            if digests.get(task):
                all_digests.append(str(digests[task]))
    all_five = bool(all_digests) and sql_present and not failures and len(set(all_digests)) == 1
    return {
        "ok": not failures and shared is not None,
        "digest": shared,
        "all_five_match": all_five,
        "failures": failures,
    }


def allocate_run_dir(
    root: Path,
    airflow_version: str,
    date: str,
    slug: str,
    run_ids: list[str],
) -> Path:
    """``results/<version>/<YYYY-MM-DD>-<slug>``, with a numeric suffix on collision.

    Re-reporting the same dag runs overwrites the directory that already
    records those run ids.
    """
    parent = root / airflow_version
    base_name = f"{date}-{slug}"

    def same_runs(path: Path) -> bool:
        env_path = path / "environment.json"
        if not env_path.is_file():
            return not path.exists() or not any(path.iterdir())
        try:
            meta = json.loads(env_path.read_text())
        except json.JSONDecodeError:
            return False
        return meta.get("run_ids") == run_ids

    base = parent / base_name
    if not base.exists() or same_runs(base):
        return base
    suffix = 2
    while True:
        candidate = parent / f"{base_name}-{suffix}"
        if not candidate.exists() or same_runs(candidate):
            return candidate
        suffix += 1


def _fmt(value, digits: int) -> str:
    if value is None:
        return ""
    return f"{float(value):.{digits}f}"


def _spread(block: dict | None, digits: int = 2) -> str:
    if not block:
        return ""
    return f"{block['median']:.{digits}f} ({block['min']:.{digits}f}–{block['max']:.{digits}f})"


def _ci(block: dict | None) -> str:
    if not block or not block.get("ci95"):
        return ""
    lo, hi = block["ci95"]
    return f"{lo:.2f}–{hi:.2f}"


def render_summary_md(meta: dict, task_stats: dict, md5_info: dict) -> str:
    lines = [
        f"# transfer_bench {meta.get('airflow_version')} — {meta.get('date')}-{meta.get('env_slug')}",
        "",
    ]
    if meta.get("ci") or meta.get("env_slug") == "github-actions":
        lines.extend([CI_NOISE_NOTE, ""])
    lines.extend(
        [
            (
                f"Warm-up runs discarded: {meta.get('warmup_discarded')}. "
                f"Statistics use n={meta.get('n')} successful measured run(s) "
                f"out of {meta.get('n_total')} triggered."
            ),
            (
                "The median is the middle value, or the mean of the two middle values when n is even. "
                "The 95% CI is a percentile bootstrap of that median (5000 resamples, fixed seed). "
                "With n this small the interval is wide."
            ),
            "",
        ]
    )
    if md5_info.get("ok"):
        lines.append(f"Byte-identical MD5 gate: pass (`{md5_info.get('digest')}`).")
    else:
        lines.append("Byte-identical MD5 gate: FAIL.")
        for failure in md5_info.get("failures") or []:
            lines.append(f"- {failure}")
    if md5_info.get("all_five_match"):
        lines.append("SqlToS3Operator MD5 matches the hand-written implementations.")
    elif md5_info.get("ok"):
        lines.append(
            "SqlToS3Operator MD5 differs from the hand-written implementations. "
            "It is included in the timings and is not part of the hard gate."
        )
    lines.append("")
    lines.extend(
        [
            "| task | runtime | n | rows | task_s median (min–max) | task_s 95% CI | work_s median (min–max) | cpu_s median (min–max) | overhead_s median | MiB/s median | peak RSS MiB median |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    ranked = sorted(
        task_stats.items(),
        key=lambda item: (item[1].get("task_s") or {}).get("median", math.inf),
    )
    for task_id, block in ranked:
        task_s = block.get("task_s")
        lines.append(
            "| {task} | {runtime} | {n} | {rows} | {task_s} | {ci} | {work} | {cpu} | {overhead} | {mib} | {rss} |".format(
                task=task_id,
                runtime=block.get("runtime") or "",
                n=block.get("n") or 0,
                rows=f"{int(block['rows']):,}" if block.get("rows") is not None else "",
                task_s=_spread(task_s),
                ci=_ci(task_s),
                work=_spread(block.get("work_s")),
                cpu=_spread(block.get("cpu_s")),
                overhead=_spread(block.get("overhead_s")),
                mib=_spread(block.get("mib_per_s"), 1),
                rss=_spread(block.get("peak_rss_mb"), 0),
            )
        )
    lines.extend(["", "## Environment", ""])
    for label, value in _environment_lines(meta):
        lines.append(f"- {label}: {value}")
    warnings = meta.get("warnings") or []
    if warnings:
        lines.extend(["", "## Warnings", ""])
        for warning in warnings:
            lines.append(f"- {warning}")
    lines.append("")
    return "\n".join(lines)


def _environment_lines(meta: dict) -> list[tuple[str, str]]:
    sdk = meta.get("sdk") or {}
    runtimes = meta.get("runtimes") or {}
    git = meta.get("git_sha") or "unknown"
    if meta.get("git_dirty"):
        git = f"{git} (dirty tree)"
    part = meta.get("part_size_mb")
    return [
        ("Airflow", str(meta.get("airflow_version"))),
        ("SDK go", str(sdk.get("go"))),
        ("SDK java", str(sdk.get("java"))),
        ("SDK typescript", str(sdk.get("typescript"))),
        ("Go", str(runtimes.get("go"))),
        ("JDK", str(runtimes.get("jdk"))),
        ("Node", str(runtimes.get("node"))),
        ("CPython", str(runtimes.get("cpython"))),
        ("CPU model", str(meta.get("cpu_model"))),
        ("CPU limit", str(meta.get("cpu_limit"))),
        ("Memory limit", str(meta.get("mem_limit"))),
        ("rows", str(meta.get("rows"))),
        ("fetch_size", str(meta.get("fetch_size"))),
        ("part_size", f"{part} MiB" if part is not None else "None"),
        ("git SHA", git),
        (
            "n",
            (
                f"{meta.get('n')} measured, {meta.get('n_total')} triggered, "
                f"{meta.get('warmup_discarded')} warm-up discarded"
            ),
        ),
        ("env", str(meta.get("env_slug"))),
        ("ci", str(bool(meta.get("ci")))),
    ]


def benchmark_payload(meta: dict, task_stats: dict) -> list[dict]:
    """``customSmallerIsBetter`` entries: median seconds, smaller is better.

    Throughput is omitted here because this file is smaller-is-better. MiB/s
    stays in ``summary.md``. The benchmark name includes the row count so a
    200k-row CI series is not plotted on top of a 1M-row series.
    """
    sdk = meta.get("sdk") or {}
    rows = meta.get("rows")
    extra = (
        f"airflow {meta.get('airflow_version')}; "
        f"sdk go {sdk.get('go')}, java {sdk.get('java')}, typescript {sdk.get('typescript')}; "
        f"rows {rows}; env {meta.get('env_slug')}"
    )
    if meta.get("ci") or meta.get("env_slug") == "github-actions":
        extra += "; GitHub-hosted runner, noisy, not an absolute measurement"
    points = []
    for task_id in TREND_TASKS:
        block = task_stats.get(task_id) or {}
        for metric in ("task_s", "work_s", "cpu_s"):
            stats = block.get(metric)
            if not stats:
                continue
            spread = max(stats["median"] - stats["min"], stats["max"] - stats["median"])
            points.append(
                {
                    "name": f"{task_id} {metric} ({rows} rows)",
                    "unit": "s",
                    "value": stats["median"],
                    "range": f"± {spread:.4f}",
                    "extra": extra,
                }
            )
    return points


def discover_runs(root: Path) -> list[dict]:
    runs = []
    if not root.is_dir():
        return runs
    for env_path in sorted(root.glob("*/*/environment.json")):
        try:
            meta = json.loads(env_path.read_text())
        except json.JSONDecodeError:
            continue
        stats_path = env_path.parent / "stats.json"
        if stats_path.is_file():
            try:
                meta["stats"] = json.loads(stats_path.read_text())
            except json.JSONDecodeError:
                meta["stats"] = {}
        else:
            meta["stats"] = {}
        meta["relpath"] = env_path.parent.relative_to(root).as_posix()
        runs.append(meta)
    return runs


def render_index(runs: list[dict]) -> str:
    lines = [
        "# Published results",
        "",
        "Generated by `scripts/report.py`. Do not edit by hand. `make report`",
        "rewrites this file, and `python3 scripts/report.py --index-only` rebuilds",
        "it from the `environment.json` files already on disk.",
        "",
        "Each run is a directory `results/<airflow-version>/<YYYY-MM-DD>-<env-slug>/`:",
        "",
        "- `environment.json` — Airflow version, Go / Java / TypeScript SDK versions,",
        "  Go / JDK / Node / CPython versions, CPU model, CPU and memory limits,",
        "  rows, fetch_size, part_size, git SHA, and n",
        "- `summary.md` — median, min, max, and a bootstrap 95% CI",
        "- `stats.json` — those numbers, machine-readable",
        "- `results.csv` — every task of every run, including the discarded warm-up",
        "- `runs/<run_id>.json` and `runs/<run_id>.csv` — raw per-run records",
        "- `benchmark.json` — `customSmallerIsBetter` payload (median `task_s`,",
        "  `work_s`, and `cpu_s`). The GitHub Actions workflow is what publishes",
        "  this file to the trend chart. Laptop runs stay in git only.",
        "",
        "The first triggered run is a warm-up. It stays in the raw files and is",
        "excluded from the medians. `n` is the number of measured runs.",
        "",
        CI_NOISE_NOTE,
        "",
        "The Apple M4 sample in the repository README is not in this index.",
        "Different `env` values are different machines. Do not read a column",
        "across environments as a single trend.",
        "",
        "## Runs",
        "",
        "| airflow | date | env | rows | n | git | md5 | path |",
        "|---|---|---|---:|---:|---|---|---|",
    ]
    if not runs:
        lines.append("| | | | | | | | _No published runs yet._ |")
    else:
        for meta in runs:
            git = str(meta.get("git_sha") or "")[:12]
            md5 = meta.get("md5") or {}
            if md5.get("ok"):
                digest = str(md5.get("digest") or "")[:12]
                md5_cell = f"pass {digest}"
            else:
                md5_cell = "fail"
            rel = meta.get("relpath") or ""
            lines.append(
                "| {airflow} | {date} | {env} | {rows} | {n} | `{git}` | {md5} | [{path}]({path}/summary.md) |".format(
                    airflow=meta.get("airflow_version") or "",
                    date=meta.get("date") or "",
                    env=meta.get("env_slug") or "",
                    rows=meta.get("rows") if meta.get("rows") is not None else "",
                    n=meta.get("n") if meta.get("n") is not None else "",
                    git=git,
                    md5=md5_cell,
                    path=rel,
                )
            )
    lines.extend(
        [
            "",
            "## Trend by environment",
            "",
            "Task duration is the median `task_s` in seconds (smaller is better),",
            "after one discarded warm-up. MiB/s is median throughput over `work_s`",
            "(larger is better). SDK columns are go / java / typescript.",
            "Only runs that followed the protocol are included: at least 5 triggered",
            "runs, 1 warm-up discarded, the MD5 gate passed, and a clean git tree.",
            "",
        ]
    )
    protocol = [meta for meta in runs if meta.get("protocol_ok")]
    if not protocol:
        lines.append("_No protocol runs yet._")
        lines.append("")
        return "\n".join(lines)

    by_env: dict[str, list[dict]] = {}
    for meta in protocol:
        by_env.setdefault(str(meta.get("env_slug") or "unknown"), []).append(meta)
    for env in sorted(by_env):
        group = sorted(by_env[env], key=lambda m: (str(m.get("date") or ""), str(m.get("airflow_version") or "")))
        lines.append(f"### `{env}`")
        lines.append("")
        if env == "github-actions":
            lines.append(CI_NOISE_NOTE)
            lines.append("")
        lines.extend(_trend_table(group, "task_s", 2))
        lines.append("")
        lines.append("MiB/s median:")
        lines.append("")
        lines.extend(_trend_table(group, "mib_per_s", 1))
        lines.append("")
    return "\n".join(lines)


def _trend_table(group: list[dict], metric: str, digits: int) -> list[str]:
    header = "| airflow | sdk go/java/ts | date | rows | n | " + " | ".join(
        TREND_LABELS[task] for task in TREND_TASKS
    ) + " |"
    sep = "|---|---|---|---:|---:|" + "|".join("---:" for _ in TREND_TASKS) + "|"
    rows = [header, sep]
    for meta in group:
        sdk = meta.get("sdk") or {}
        sdk_cell = " / ".join(str(sdk.get(name) or "") for name in ("go", "java", "typescript"))
        stats = (meta.get("stats") or {}).get("tasks") or {}
        cells = []
        for task in TREND_TASKS:
            block = (stats.get(task) or {}).get(metric)
            cells.append(_fmt(block.get("median"), digits) if block else "")
        rows.append(
            "| {airflow} | {sdk} | {date} | {nrows} | {n} | {cells} |".format(
                airflow=meta.get("airflow_version") or "",
                sdk=sdk_cell,
                date=meta.get("date") or "",
                nrows=meta.get("rows") if meta.get("rows") is not None else "",
                n=meta.get("n") if meta.get("n") is not None else "",
                cells=" | ".join(cells),
            )
        )
    return rows


def write_index(root: Path) -> str:
    text = render_index(discover_runs(root))
    root.mkdir(parents=True, exist_ok=True)
    (root / "README.md").write_text(text)
    return text
