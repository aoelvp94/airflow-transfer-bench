#!/usr/bin/env python3
"""Choose the Airflow version a benchmark workflow should run.

``workflow_dispatch`` always runs the requested version (default 3.4.0) with
the requested SDK versions (default 1.0.0).

``schedule`` looks up the newest stable apache-airflow on PyPI that is at
least 3.4.0 (the first release this bench tracks). It runs only when
``RESULTS.md`` has no GitHub-hosted entry for that version yet. A laptop
entry for that version does not suppress the CI run. SDK versions stay at
the workflow defaults; the JSON also reports the newest SDK versions it
could see, so a person can dispatch those explicitly.

Prints one JSON object on stdout. Progress goes to stderr.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from report_lib import has_ci_run, latest_stable, pypi_stable_versions  # noqa: E402

DEFAULTS = {
    "airflow_version": "3.4.0",
    "go_sdk_version": "1.0.0",
    "java_sdk_version": "1.0.0",
    "ts_sdk_version": "1.0.0",
    "rows": 200_000,
    "runs": 5,
    "supervisor_schema_version": "2026-10-30",
}
_VERSION = re.compile(r"^\d+\.\d+\.\d+$")
_SCHEMA = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_USER_AGENT = "airflow-transfer-bench"


def fetch_json(url: str) -> dict:
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode())


def fetch_text(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read().decode()


def latest_go_sdk() -> str | None:
    try:
        payload = fetch_json("https://proxy.golang.org/github.com/apache/airflow/go-sdk/@latest")
    except Exception:
        return None
    version = str(payload.get("Version") or "")
    return version[1:] if version.startswith("v") else version or None


def latest_java_sdk() -> str | None:
    try:
        text = fetch_text("https://repo1.maven.org/maven2/org/apache/airflow/airflow-sdk/maven-metadata.xml")
    except Exception:
        return None
    match = re.search(r"<release>([^<]+)</release>", text) or re.search(r"<latest>([^<]+)</latest>", text)
    return match.group(1).strip() if match else None


def latest_ts_sdk() -> str | None:
    try:
        payload = fetch_json("https://registry.npmjs.org/@apache-airflow/ts-sdk/latest")
    except Exception:
        return None
    version = payload.get("version")
    return str(version) if version else None


def latest_sdks() -> dict[str, str | None]:
    return {"go": latest_go_sdk(), "java": latest_java_sdk(), "typescript": latest_ts_sdk()}


def check_version(name: str, value: str) -> str:
    if not _VERSION.fullmatch(value or ""):
        raise SystemExit(f"{name}={value!r} is not an x.y.z version")
    return value


def decide(
    *,
    mode: str,
    results_md: Path,
    airflow_version: str,
    go_sdk_version: str,
    java_sdk_version: str,
    ts_sdk_version: str,
    rows: int,
    runs: int,
    supervisor_schema_version: str,
    pypi_payload: dict | None,
    sdk_latest: dict | None,
    minimum: str = "3.4.0",
) -> dict:
    if not _SCHEMA.fullmatch(supervisor_schema_version):
        raise SystemExit(f"supervisor schema {supervisor_schema_version!r} is not YYYY-MM-DD")
    if runs < 5:
        raise SystemExit("runs must be at least 5 (one warm-up is discarded)")
    if rows < 1:
        raise SystemExit("rows must be positive")
    for name, value in (
        ("go_sdk_version", go_sdk_version),
        ("java_sdk_version", java_sdk_version),
        ("ts_sdk_version", ts_sdk_version),
    ):
        check_version(name, value)

    pypi_latest = None
    if pypi_payload is not None:
        pypi_latest = latest_stable(pypi_stable_versions(pypi_payload), minimum=minimum)

    if mode == "schedule":
        if pypi_latest is None:
            raise SystemExit(f"no stable apache-airflow >={minimum} on PyPI")
        version = pypi_latest
        if has_ci_run(results_md, version):
            should_run = False
            reason = f"RESULTS.md already has a github-actions entry for Airflow {version}"
        else:
            should_run = True
            reason = f"no github-actions entry for Airflow {version}"
    elif mode == "workflow_dispatch":
        version = check_version("airflow_version", airflow_version)
        should_run = True
        reason = "workflow_dispatch"
    else:
        raise SystemExit(f"unknown mode {mode!r}")

    return {
        "mode": mode,
        "should_run": should_run,
        "reason": reason,
        "airflow_version": version,
        "go_sdk_version": go_sdk_version,
        "java_sdk_version": java_sdk_version,
        "ts_sdk_version": ts_sdk_version,
        "rows": rows,
        "runs": runs,
        "supervisor_schema_version": supervisor_schema_version,
        "pypi_latest": pypi_latest,
        "latest_sdks": sdk_latest or {},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("schedule", "workflow_dispatch"))
    parser.add_argument("--results-md", type=Path, default=Path("RESULTS.md"))
    parser.add_argument("--airflow-version", default=DEFAULTS["airflow_version"])
    parser.add_argument("--go-sdk-version", default=DEFAULTS["go_sdk_version"])
    parser.add_argument("--java-sdk-version", default=DEFAULTS["java_sdk_version"])
    parser.add_argument("--ts-sdk-version", default=DEFAULTS["ts_sdk_version"])
    parser.add_argument("--rows", type=int, default=DEFAULTS["rows"])
    parser.add_argument("--runs", type=int, default=DEFAULTS["runs"])
    parser.add_argument("--supervisor-schema-version", default=DEFAULTS["supervisor_schema_version"])
    parser.add_argument("--min-version", default="3.4.0")
    args = parser.parse_args()

    payload = None
    if args.mode == "schedule" or args.airflow_version:
        try:
            payload = fetch_json("https://pypi.org/pypi/apache-airflow/json")
        except Exception as exc:
            if args.mode == "schedule":
                raise SystemExit(f"could not read PyPI: {exc}") from exc
            print(f"PyPI lookup failed ({exc}); continuing with the requested version", file=sys.stderr)
    sdks = latest_sdks()
    for name, version in sdks.items():
        print(f"latest {name} sdk: {version or 'unknown'}", file=sys.stderr)
    decision = decide(
        mode=args.mode,
        results_md=args.results_md,
        airflow_version=args.airflow_version,
        go_sdk_version=args.go_sdk_version,
        java_sdk_version=args.java_sdk_version,
        ts_sdk_version=args.ts_sdk_version,
        rows=args.rows,
        runs=args.runs,
        supervisor_schema_version=args.supervisor_schema_version,
        pypi_payload=payload,
        sdk_latest=sdks,
        minimum=args.min_version,
    )
    if decision["latest_sdks"] and args.mode == "schedule":
        print(
            "scheduled runs pin SDK versions to the workflow defaults; "
            "dispatch the workflow to benchmark a newer SDK",
            file=sys.stderr,
        )
    json.dump(decision, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
