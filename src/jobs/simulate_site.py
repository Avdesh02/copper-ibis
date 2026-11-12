"""Test-data simulation for one site.

Generates physically plausible sensor telemetry plus turbine master-data change events
and writes them as JSON into the landing Volume, where Auto Loader picks them up.

Run once per site by the orchestrator's for_each fan-out, so ``--site`` is the per-item
value ``{{input}}``. Writing per-site files also means two sites never contend for the
same output path.

Injects a configurable share of anomalies so the alert path has something to detect and
the quarantine path has something to catch.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


# Make `turbine_pipeline` importable.
#
# A serverless spark_python_task runs this file through exec(), so __file__ is not
# defined and the working directory is not the bundle root. The source root is
# therefore located explicitly: the task's entry path appears in sys.argv[0], and the
# deployed layout always puts the package under <bundle root>/src.
def _add_package_root() -> None:
    candidates: list[Path] = []
    try:
        candidates.append(Path(__file__).resolve().parents[1])
    except NameError:
        pass
    if sys.argv and sys.argv[0]:
        entry = Path(sys.argv[0]).resolve()
        candidates.extend([entry.parent.parent, *entry.parents])
    cwd = Path.cwd()
    candidates.extend([cwd / "src", cwd, *cwd.parents])

    for base in candidates:
        for root in (base, base / "src"):
            if (root / "turbine_pipeline").is_dir():
                if str(root) not in sys.path:
                    sys.path.insert(0, str(root))
                return


_add_package_root()

from turbine_pipeline.lib import physics  # noqa: E402
from turbine_pipeline.lib.sites import get_site  # noqa: E402


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def build_sensor_events(
    site_id: str,
    rows: int,
    anomaly_rate: float,
    run_id: str,
    start: datetime | None = None,
    seed: int | None = None,
) -> list[dict]:
    """Generate ``rows`` sensor events for one site.

    Pure: no I/O, so it can be exercised directly in tests.
    """
    rng = random.Random(seed)
    site = get_site(site_id)
    start = start or datetime.now(timezone.utc) - timedelta(minutes=rows)

    events: list[dict] = []
    for i in range(rows):
        turbine = site.turbines[i % len(site.turbines)]
        ts = start + timedelta(seconds=30 * i)

        wind = max(
            0.0,
            rng.gauss(physics.expected_wind_speed(ts.hour, site.wind_factor), 2.5),
        )
        power_kw = physics.power_output_kw(wind, turbine.capacity_mw)
        load = power_kw / (turbine.capacity_mw * 1000.0)

        oil_temp = physics.oil_temperature_c(site.temp_base_c, load)
        rpm = physics.blade_rpm(wind)
        friction = physics.gear_friction(load)

        is_anomaly = rng.random() < anomaly_rate
        if is_anomaly:
            # Push one metric past its critical bound so an alert must fire.
            which = rng.choice(("oil_temperature", "blade_velocity", "gear_friction"))
            if which == "oil_temperature":
                oil_temp = round(oil_temp + rng.uniform(25.0, 45.0), 2)
            elif which == "blade_velocity":
                rpm = round(rpm + rng.uniform(10.0, 18.0), 2)
            else:
                friction = round(min(friction + rng.uniform(0.3, 0.5), 1.5), 4)

        events.append(
            {
                "event_id": f"{run_id}-{site_id}-{i:06d}",
                "turbine_id": turbine.turbine_id,
                "site_id": site_id,
                "event_ts": _iso(ts),
                "seq": i,
                "wind_speed_mps": round(wind, 2),
                "readings": [
                    {
                        "metric": "oil_temperature",
                        "value": oil_temp,
                        "unit": "celsius",
                        "quality": "SUSPECT" if is_anomaly else "GOOD",
                    },
                    {"metric": "blade_velocity", "value": rpm, "unit": "rpm", "quality": "GOOD"},
                    {
                        "metric": "gear_friction",
                        "value": friction,
                        "unit": "coef",
                        "quality": "GOOD",
                    },
                    {
                        "metric": "power_output",
                        "value": power_kw,
                        "unit": "kw",
                        "quality": "GOOD",
                    },
                ],
            }
        )
    return events


def build_turbine_changes(site_id: str, run_id: str, change_seq_base: int) -> list[dict]:
    """Master-data change events for the site's turbines, feeding the SCD2 dimension."""
    site = get_site(site_id)
    now = datetime.now(timezone.utc)
    return [
        {
            "turbine_id": t.turbine_id,
            "site_id": site_id,
            "state": t.state,
            "capacity_mw": t.capacity_mw,
            "status": t.status,
            "hub_height_m": t.hub_height_m,
            "rotor_diameter_m": t.rotor_diameter_m,
            "commission_date": t.commission_date,
            "operation": "UPSERT",
            "change_seq": change_seq_base + idx,
            "changed_at": _iso(now),
        }
        for idx, t in enumerate(site.turbines)
    ]


def build_malformed(site_id: str, tag: str, count: int) -> list[dict]:
    """Records that do not match the declared schema.

    Without these the rescued-data quarantine table would stay permanently empty and
    that branch would never be exercised.
    """
    return [
        {
            "event_id": f"{tag}-malformed-{n}",
            "turbine_id": None,
            "site_id": site_id,
            "event_ts": "not-a-timestamp",
            "unexpected_field": {"nested": "value"},
        }
        for n in range(count)
    ]


def _write_jsonl(records: list[dict], target_dir: str, filename: str) -> str:
    """Write newline-delimited JSON, creating the directory if needed."""
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    path = str(Path(target_dir) / filename)
    with open(path, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate test telemetry for one site")
    parser.add_argument("--site", required=True, help="site id, e.g. rajasthan")
    parser.add_argument("--catalog", required=True)
    # Passed in rather than hardcoded: dev deployments prefix schema names.
    parser.add_argument("--raw-schema", default="raw")
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--anomaly-rate", type=float, default=0.05)
    parser.add_argument("--run-id", default="local")
    parser.add_argument(
        "--malformed", type=int, default=2, help="malformed records, to exercise quarantine"
    )
    parser.add_argument(
        "--backfill-rows",
        type=int,
        default=50,
        help=(
            "historical rows written to the backfill directory. The pipeline's one-shot "
            "backfill append flow reads that path and fails if it is missing, so this "
            "also guarantees the directory exists."
        ),
    )
    parser.add_argument("--seed", type=int, default=None)
    args, _unknown = parser.parse_known_args(argv)

    base = f"/Volumes/{args.catalog}/{args.raw_schema}/landing"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    tag = f"{args.site}-{args.run_id}-{stamp}-{uuid.uuid4().hex[:8]}"

    events = build_sensor_events(
        args.site, args.rows, args.anomaly_rate, args.run_id, seed=args.seed
    )
    records = events + build_malformed(args.site, tag, args.malformed)

    sensors_path = _write_jsonl(records, f"{base}/sensors/{args.site}", f"sensors-{tag}.json")
    changes = build_turbine_changes(args.site, args.run_id, change_seq_base=1)
    turbines_path = _write_jsonl(changes, f"{base}/turbines", f"turbines-{tag}.json")

    # Historical seed for the pipeline's one-shot backfill append flow. Written only
    # when the site has nothing there yet, so repeat runs do not re-seed history.
    backfill_dir = Path(f"{base}/backfill")
    backfill_path = "(already seeded)"
    existing = list(backfill_dir.glob(f"backfill-{args.site}-*.json")) if backfill_dir.exists() else []
    if args.backfill_rows > 0 and not existing:
        history_start = datetime.now(timezone.utc) - timedelta(days=1)
        history = build_sensor_events(
            args.site,
            args.backfill_rows,
            anomaly_rate=0.0,
            run_id=f"backfill-{args.run_id}",
            start=history_start,
            seed=args.seed,
        )
        backfill_path = _write_jsonl(
            history, str(backfill_dir), f"backfill-{args.site}-{tag}.json"
        )

    print(f"site={args.site}")
    print(f"  sensor events  : {len(events)} (+{args.malformed} malformed) -> {sensors_path}")
    print(f"  turbine changes: {len(changes)} -> {turbines_path}")
    print(f"  backfill seed  : {backfill_path}")

    # Hand the row count downstream when running on Databricks.
    try:
        dbutils.jobs.taskValues.set(key=f"rows_{args.site}", value=len(events))  # noqa: F821
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    # A serverless spark_python_task runs this file through exec(), where even
    # SystemExit(0) is reported as a task failure. So exit explicitly only when the
    # work actually failed, and otherwise just fall off the end.
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
