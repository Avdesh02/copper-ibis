#!/usr/bin/env python3
"""Create the Unity Catalog catalog for a bundle target.

A DAB cannot do this in this workspace. Default Storage is enabled on the account, and
the catalogs REST API rejects creation without a managed location:

    Metastore storage root URL does not exist. Default Storage is enabled in your
    account. (400 INVALID_STATE)

That applies both to ``databricks catalogs create`` and to a DAB ``catalogs:`` resource.
``CREATE CATALOG`` over SQL does work, so this script runs that against a SQL warehouse.

Idempotent: if the catalog already exists it does nothing. The warehouse is only started
when work is actually needed, and is stopped again afterwards unless it was already
running or ``--keep-warehouse`` is passed.

Usage:
    python scripts/bootstrap_catalog.py --target dev
    python scripts/bootstrap_catalog.py --target prod --profile wind
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time

CATALOGS = {
    "dev": "wind_dev_copper_ibis",
    # The shared CI environment lives in the dev catalog under its own `ci_` schema
    # prefix, so it needs no catalog of its own.
    "ci": "wind_dev_copper_ibis",
    "prod": "wind_prod_copper_ibis",
}

COMMENT = "Wind turbine telemetry platform ({target}); created by scripts/bootstrap_catalog.py."


def run_cli(args: list[str], profile: str | None) -> tuple[int, str]:
    """Run a databricks CLI command, returning (exit code, combined output).

    ``profile`` is omitted when empty so CI can authenticate purely from
    DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET; passing a
    profile name that has no entry in .databrickscfg would fail there.
    """
    cmd = ["databricks", *args]
    if profile:
        cmd += ["-p", profile]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def catalog_exists(name: str, profile: str | None) -> bool:
    code, out = run_cli(["catalogs", "get", name, "-o", "json"], profile)
    return code == 0 and f'"{name}"' in out


def pick_warehouse(profile: str | None) -> tuple[str, str]:
    """Return (warehouse_id, state), preferring one that is already running."""
    code, out = run_cli(["warehouses", "list", "-o", "json"], profile)
    if code != 0:
        raise SystemExit(f"could not list SQL warehouses:\n{out}")

    try:
        warehouses = json.loads(out)
    except json.JSONDecodeError:
        raise SystemExit(f"unexpected warehouses response:\n{out[:400]}") from None
    if not warehouses:
        raise SystemExit("no SQL warehouse available; one is required to create a catalog")

    for wh in warehouses:
        if wh.get("state") == "RUNNING":
            return wh["id"], "RUNNING"
    return warehouses[0]["id"], warehouses[0].get("state", "UNKNOWN")


def execute_sql(statement: str, warehouse_id: str, profile: str | None) -> None:
    payload = json.dumps(
        {"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "50s"}
    )
    code, out = run_cli(["api", "post", "/api/2.0/sql/statements", "--json", payload], profile)
    if code != 0:
        raise SystemExit(f"SQL call failed for: {statement}\n{out}")

    try:
        state = json.loads(out).get("status", {}).get("state")
    except json.JSONDecodeError:
        raise SystemExit(f"unexpected SQL response:\n{out[:400]}") from None

    if state != "SUCCEEDED":
        raise SystemExit(f"statement did not succeed ({state}): {statement}\n{out}")
    print(f"  ok: {statement}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=sorted(CATALOGS), default="dev")
    parser.add_argument(
        "--profile",
        default="wind",
        help='CLI profile; pass "" in CI to authenticate from environment variables',
    )
    parser.add_argument("--keep-warehouse", action="store_true",
                        help="leave the SQL warehouse running when finished")
    args = parser.parse_args(argv)

    catalog = CATALOGS[args.target]
    print(f"Bootstrapping catalog {catalog} (target={args.target})")

    if catalog_exists(catalog, args.profile):
        print(f"  {catalog} already exists - nothing to do")
        return 0

    warehouse_id, state = pick_warehouse(args.profile)
    started_by_us = state != "RUNNING"

    if started_by_us:
        print(f"  starting SQL warehouse {warehouse_id} (was {state})")
        code, out = run_cli(["warehouses", "start", warehouse_id], args.profile)
        if code != 0:
            raise SystemExit(f"could not start warehouse:\n{out}")
        time.sleep(5)

    try:
        execute_sql(
            f"CREATE CATALOG IF NOT EXISTS {catalog} "
            f"COMMENT '{COMMENT.format(target=args.target)}'",
            warehouse_id,
            args.profile,
        )
    finally:
        if started_by_us and not args.keep_warehouse:
            print(f"  stopping SQL warehouse {warehouse_id}")
            run_cli(["warehouses", "stop", warehouse_id], args.profile)

    if not catalog_exists(catalog, args.profile):
        raise SystemExit(f"{catalog} still missing after CREATE CATALOG")

    print(f"  {catalog} ready")
    print("\nNext: databricks bundle deploy --target " + args.target)
    return 0


if __name__ == "__main__":
    sys.exit(main())
