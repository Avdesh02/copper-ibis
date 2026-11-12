#!/usr/bin/env python3
"""Print the real error behind a failed job run.

``bundle run`` reports only "Some iterations failed" for a for_each task. The actual
exception lives on the nested iteration run, several API hops down. This walks the
chain and prints it.

Usage:  python scripts/show_run_error.py <run_id> [--profile wind]
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess

ANSI = re.compile(r"\x1b\[[0-9;]*m")


def api(path: str, profile: str) -> dict:
    proc = subprocess.run(
        ["databricks", "api", "get", path, "-p", profile], capture_output=True, text=True
    )
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {"_raw": proc.stdout + proc.stderr}


def show_output(run_id: int, profile: str, label: str) -> None:
    out = api(f"/api/2.2/jobs/runs/get-output?run_id={run_id}", profile)
    error = out.get("error")
    trace = ANSI.sub("", out.get("error_trace") or "")
    logs = ANSI.sub("", out.get("logs") or "")

    print(f"\n=== {label} (run {run_id}) ===")
    if error:
        print(f"ERROR: {error}")
    if trace:
        print(trace[-2500:])
    elif logs:
        print(logs[-2000:])
    if not (error or trace or logs):
        print("(no output captured)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id", type=int)
    parser.add_argument("--profile", default="wind")
    parser.add_argument("--max-iterations", type=int, default=2)
    args = parser.parse_args()

    run = api(f"/api/2.2/jobs/runs/get?run_id={args.run_id}", args.profile)

    for task in run.get("tasks", []):
        state = task.get("state", {}).get("result_state")
        key = task["task_key"]
        print(f"task {key}: {state}")
        if state not in ("FAILED", "TIMEDOUT"):
            continue

        task_run_id = task.get("run_id")
        nested = api(f"/api/2.2/jobs/runs/get?run_id={task_run_id}", args.profile)
        iterations = nested.get("iterations") or []

        if iterations:
            failed = [i for i in iterations if i.get("state", {}).get("result_state") == "FAILED"]
            for it in failed[: args.max_iterations]:
                show_output(it["run_id"], args.profile, f"{key} iteration")
        else:
            show_output(task_run_id, args.profile, key)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
