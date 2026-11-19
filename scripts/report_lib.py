"""Pure helpers for the transfer benchmark report.

No Airflow imports. ``scripts/report.py`` collects rows from the metadata DB
and calls into this module to drop the warm-up run, summarize, and prepend a
changelog entry to ``RESULTS.md``.
"""

from __future__ import annotations

import json
import math
import os
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


_ENTRY_RE = re.compile(r"<!-- bench-entry (.*?) -->", re.DOTALL)


def load_entries(text: str) -> list[dict]:
    """Entries embedded in ``RESULTS.md``. Malformed comments are skipped."""
    entries = []
    for match in _ENTRY_RE.finditer(text or ""):
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            entries.append(payload)
    return entries


def _is_ci_entry(entry: dict) -> bool:
    return entry.get("ci") is True or entry.get("env_slug") == "github-actions"


def has_ci_run(results_md: Path, version: str) -> bool:
    """True when ``RESULTS.md`` already records a GitHub-hosted run of this version.

    A laptop entry for the same Airflow version does not count: the scheduled
    workflow exists to put a ``github-actions`` point on the trend. Raw files
    under ``results/`` are local and are not consulted.
    """
    if not results_md.is_file():
        return False
    for entry in load_entries(results_md.read_text()):
        if str(entry.get("airflow_version") or "") != version:
            continue
        if _is_ci_entry(entry):
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
    lines.extend(_md5_lines(md5_info))
    lines.append("")
    lines.extend(_summary_table(task_stats))
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


def published_entry(meta: dict, task_stats: dict) -> dict:
    """The slice of a run that ``RESULTS.md`` stores and re-renders."""
    sdk = meta.get("sdk") or {}
    return {
        "run_ids": list(meta.get("run_ids") or []),
        "airflow_version": meta.get("airflow_version"),
        "sdk": {
            "go": sdk.get("go"),
            "java": sdk.get("java"),
            "typescript": sdk.get("typescript"),
        },
        "date": meta.get("date"),
        "env_slug": meta.get("env_slug"),
        "ci": bool(meta.get("ci")),
        "cpu_model": meta.get("cpu_model"),
        "cpu_limit": meta.get("cpu_limit"),
        "mem_limit": meta.get("mem_limit"),
        "rows": meta.get("rows"),
        "fetch_size": meta.get("fetch_size"),
        "part_size_mb": meta.get("part_size_mb"),
        "git_sha": meta.get("git_sha"),
        "git_dirty": bool(meta.get("git_dirty")),
        "n": meta.get("n"),
        "n_total": meta.get("n_total"),
        "warmup_discarded": meta.get("warmup_discarded"),
        "runtimes": meta.get("runtimes") or {},
        "md5": meta.get("md5") or {},
        "protocol_ok": bool(meta.get("protocol_ok")),
        "warnings": list(meta.get("warnings") or []),
        "tasks": task_stats,
    }


def upsert_entry(entries: list[dict], new: dict) -> list[dict]:
    """Prepend ``new``. The same dag run ids replace the existing entry in place."""
    key = list(new.get("run_ids") or [])
    if not key:
        raise ValueError("a published entry needs at least one run id")
    for index, existing in enumerate(entries):
        if list(existing.get("run_ids") or []) == key:
            updated = list(entries)
            updated[index] = new
            return updated
    return [new, *entries]


def _sdk_cell(entry: dict) -> str:
    sdk = entry.get("sdk") or {}
    return " / ".join(str(sdk.get(name) or "") for name in ("go", "java", "typescript"))


def _heading(entry: dict) -> str:
    return (
        f"## Airflow {entry.get('airflow_version')} · "
        f"go/java/ts SDK {_sdk_cell(entry)} · "
        f"{entry.get('date')} · {entry.get('env_slug')}"
    )


def _notes_line(entry: dict) -> str:
    if _is_ci_entry(entry):
        return CI_NOISE_NOTE
    return (
        f"Local run ({entry.get('cpu_model')}), scheduler cap {entry.get('cpu_limit')} CPU / "
        f"{entry.get('mem_limit')}, {entry.get('rows')} rows. Not a GitHub-hosted runner and not "
        "comparable with the Apple M4 sample."
    )


def _md5_lines(md5_info: dict) -> list[str]:
    if md5_info.get("ok"):
        lines = [f"Byte-identical MD5 gate: pass (`{md5_info.get('digest')}`)."]
    else:
        lines = ["Byte-identical MD5 gate: FAIL."]
        for failure in md5_info.get("failures") or []:
            lines.append(f"- {failure}")
    if md5_info.get("all_five_match"):
        lines.append("SqlToS3Operator MD5 matches the hand-written implementations.")
    elif md5_info.get("ok"):
        lines.append(
            "SqlToS3Operator MD5 differs from the hand-written implementations. "
            "It is included in the timings and is not part of the hard gate."
        )
    return lines


def _summary_table(task_stats: dict) -> list[str]:
    lines = [
        "| task | runtime | n | rows | task_s median (min–max) | task_s 95% CI | work_s median (min–max) | cpu_s median (min–max) | overhead_s median | MiB/s median | peak RSS MiB median |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
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
    return lines


def _environment_compact(entry: dict) -> str:
    runtimes = entry.get("runtimes") or {}
    sdk = entry.get("sdk") or {}
    git = str(entry.get("git_sha") or "unknown")
    if entry.get("git_dirty"):
        git = f"{git} (dirty tree)"
    part = entry.get("part_size_mb")
    part_text = f"{part} MiB" if part is not None else "unknown"
    return (
        f"Airflow {entry.get('airflow_version')}; "
        f"SDK go {sdk.get('go')}, java {sdk.get('java')}, typescript {sdk.get('typescript')}; "
        f"Go {runtimes.get('go')}; JDK {runtimes.get('jdk')}; Node {runtimes.get('node')}; "
        f"CPython {runtimes.get('cpython')}; CPU {entry.get('cpu_model')}; "
        f"limit {entry.get('cpu_limit')} CPU / {entry.get('mem_limit')}; "
        f"rows {entry.get('rows')}; fetch_size {entry.get('fetch_size')}; part_size {part_text}; "
        f"git `{git}`; env `{entry.get('env_slug')}`; ci {str(bool(entry.get('ci')))}."
    )


def render_entry(entry: dict) -> str:
    """One changelog section. The HTML comment is the idempotency key and the trend source."""
    blob = json.dumps(entry, sort_keys=True, separators=(",", ":"))
    lines = [
        _heading(entry),
        "",
        f"<!-- bench-entry {blob} -->",
        "",
        (
            f"n={entry.get('n')} measured, {entry.get('n_total')} triggered, "
            f"{entry.get('warmup_discarded')} warm-up discarded."
        ),
        "",
        *_md5_lines(entry.get("md5") or {}),
        "",
        *_summary_table(entry.get("tasks") or {}),
        "",
        f"Environment: {_environment_compact(entry)}",
        "",
        f"Notes: {_notes_line(entry)}",
    ]
    warnings = entry.get("warnings") or []
    if warnings:
        lines.extend(["", "Warnings: " + "; ".join(str(item) for item in warnings) + "."])
    return "\n".join(lines)


def _trend_table(group: list[dict], metric: str, digits: int) -> list[str]:
    header = "| airflow | sdk go/java/ts | date | rows | n | " + " | ".join(
        TREND_LABELS[task] for task in TREND_TASKS
    ) + " |"
    sep = "|---|---|---|---:|---:|" + "|".join("---:" for _ in TREND_TASKS) + "|"
    rows = [header, sep]
    for entry in group:
        cells = []
        tasks = entry.get("tasks") or {}
        for task in TREND_TASKS:
            block = (tasks.get(task) or {}).get(metric)
            cells.append(_fmt(block.get("median"), digits) if block else "")
        rows.append(
            "| {airflow} | {sdk} | {date} | {nrows} | {n} | {cells} |".format(
                airflow=entry.get("airflow_version") or "",
                sdk=_sdk_cell(entry),
                date=entry.get("date") or "",
                nrows=entry.get("rows") if entry.get("rows") is not None else "",
                n=entry.get("n") if entry.get("n") is not None else "",
                cells=" | ".join(cells),
            )
        )
    return rows


def render_results(entries: list[dict]) -> str:
    """``RESULTS.md``: trend table, then one section per run, newest first."""
    lines = [
        "# Results",
        "",
        "Changelog of published transfer-bench runs. Newest entry first.",
        "`just report` prepends an entry. Each entry carries an HTML comment with",
        "its Airflow dag run ids; the same ids replace that entry instead of",
        "adding a second one. Do not edit this file by hand.",
        "",
        "Raw per-run CSV, JSON, logs, and the per-run `summary.md` stay in",
        "`results/` on the machine that ran the bench. They are gitignored.",
        "The GitHub Actions workflow commits only this file. It generates",
        "`benchmark.json` for the trend chart and pushes that chart to",
        "`gh-pages`, not to this branch.",
        "",
        "The Apple M4 sample in the README is Aldo's local run. It is not an",
        "entry below. Different environments are different machines. Do not",
        "read the trend table across environments as one series.",
        "",
        CI_NOISE_NOTE,
        "",
        "## Trend",
        "",
        "Median `task_s` (seconds, smaller is better) and peak RSS (MiB), per",
        "release, after one discarded warm-up. SDK columns are go / java /",
        "typescript. Only runs that followed the protocol are included: at least",
        "5 triggered runs, 1 warm-up discarded, the MD5 gate passed, and a clean",
        "git tree.",
        "",
    ]
    protocol = [entry for entry in entries if entry.get("protocol_ok")]
    if not protocol:
        lines.append("_No protocol runs yet._")
        lines.append("")
    else:
        by_env: dict[str, list[dict]] = {}
        for entry in protocol:
            by_env.setdefault(str(entry.get("env_slug") or "unknown"), []).append(entry)
        for env in sorted(by_env):
            group = sorted(
                by_env[env],
                key=lambda item: (str(item.get("date") or ""), str(item.get("airflow_version") or "")),
                reverse=True,
            )
            lines.append(f"### `{env}`")
            lines.append("")
            if env == "github-actions":
                lines.append(CI_NOISE_NOTE)
                lines.append("")
            lines.append("task_s median:")
            lines.append("")
            lines.extend(_trend_table(group, "task_s", 2))
            lines.append("")
            lines.append("peak RSS MiB median:")
            lines.append("")
            lines.extend(_trend_table(group, "peak_rss_mb", 0))
            lines.append("")
    if not entries:
        lines.append("_No published runs yet._")
        lines.append("")
    else:
        for entry in entries:
            lines.append(render_entry(entry))
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_text_atomic(path: Path, text: str) -> None:
    """Replace ``path`` when the directory is writable, even if the file is not.

    ``results/`` is shared with the Airflow user. Truncating a file owned by
    someone else needs write permission on that file; replacing it only needs
    write permission on the directory.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text)
    os.replace(temporary, path)


def write_text_shared(path: Path, text: str) -> None:
    """Write ``RESULTS.md``.

    A bind-mounted file cannot be replaced via a sibling temp file. Fall back
    to truncating the mounted file when the directory is not writable.
    """
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(text)
        os.replace(temporary, path)
    except OSError:
        if temporary.exists():
            try:
                temporary.unlink()
            except OSError:
                pass
        with path.open("w", encoding="utf-8") as handle:
            handle.write(text)


def write_results(path: Path, entries: list[dict]) -> str:
    text = render_results(entries)
    write_text_shared(path, text)
    return text
