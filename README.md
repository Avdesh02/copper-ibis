# Wind Turbine Telemetry Platform

A parameterized, multi-site wind turbine telemetry platform on Databricks: one Lakeflow
Spark Declarative Pipeline (SDP), a chained orchestrator job, a test-data simulator,
unit tests, and GitHub Actions CI/CD — all deployed from a Databricks Asset Bundle.

**Every Databricks object carries the `_copper_ibis` suffix**, matching the git folder
name, so nothing collides with other work in the shared workspace.

---

## Architecture

```
          simulator job                        SDP pipeline
                │                    ┌──────────────────────────────┐
                ▼                    │                              │
   /Volumes/<catalog>/raw/landing    │  BRONZE                      │
     ├── sensors/<site>/  ──────────▶│   bronze_sensor_raw          │
     ├── backfill/        ──────────▶│     ↑ 2 APPEND FLOWS         │
     └── turbines/        ──────────▶│   bronze_turbine_cdc         │
                                     │         │                    │
                                     │  SILVER ▼                    │
                                     │   silver_sensor_readings     │  streaming flow
                                     │   silver_sensor_quarantine   │  rescued rows
                                     │   dim_turbine (SCD2)         │  AUTO CDC
                                     │         │                    │
                                     │  GOLD   ▼                    │
                                     │   gold_reading_enriched      │  stream-static join
                                     │   gold_site_window_agg       │  MATERIALIZED VIEW
                                     │   gold_threshold_alerts      │  per-site thresholds
                                     │   gold_turbine_current_state │  MATERIALIZED VIEW
                                     └──────────────────────────────┘
```

Each Lakeflow feature is used where it is genuinely the right tool:

| Feature | Where | Why there |
|---|---|---|
| **Auto Loader** | both bronze tables | Incremental file ingest from a UC Volume. |
| **Append flow** (fan-in) | `bronze_sensor_raw` | Two sources, one target: a continuous `live` flow and a one-shot `backfill` flow (`once=True`). A UNION would be wrong — each flow tracks its own progress. |
| **Auto CDC → SCD2** | `dim_turbine` | Turbine attributes change (TURB-003 goes in and out of maintenance), so history matters. Produces `__START_AT` / `__END_AT`. |
| **Streaming flow** | `silver_sensor_readings` | Continuous append-only transformation with expectations. |
| **Materialized view** | `gold_site_window_agg`, `gold_turbine_current_state` | Aggregates over a streaming source **must** be MVs: streaming tables are append-only and never recompute when late data lands. |
| **Expectations + quarantine** | silver | Structural violations drop; value violations warn; unparseable rows route to a quarantine table instead of vanishing. |

---

## Site parameterization — static *and* dynamic

Five sites, one per state: `rajasthan` (TURB-001), `tamil_nadu` (TURB-002),
`maharashtra` (TURB-003), `gujarat` (TURB-004), `karnataka` (TURB-005).

### Static — fixed at deploy time

| Where | What |
|---|---|
| `databricks.yml` `variables:` | catalog, schema prefix, window duration, rows per site, notification email |
| pipeline `configuration:` | site roster, landing paths, target schemas, **per-site alert thresholds** |
| job `parameters:` defaults | env, catalog, schema names, anomaly rate |

Pipeline configuration values are **always strings**, so the site list is a
comma-separated string that `lib/config.py` parses and unit tests cover.

### Dynamic — resolved per run

| Reference | Used for |
|---|---|
| `{{job.start_time.iso_date}}` | `processing_date` default |
| `{{job.id}}`, `{{job.run_id}}`, `{{job.name}}` | run audit, traceable output filenames |
| `{{job.trigger.type}}`, `{{job.repair_count}}` | audit: scheduled vs manual, repaired runs |
| `{{task.name}}` | per-task logging |
| `{{input}}` | **the per-site value inside each `for_each` iteration** |
| `{{tasks.publish_site_config.values.active_sites}}` | the fan-out list, set at run time |
| `{{tasks.<key>.result_state}}` | upstream outcomes in the audit row |

The roster is static, but **which sites actually run is dynamic**:
`publish_site_config` writes the `ops.site_config_copper_ibis` control table and
publishes only the enabled sites as a task value, which the fan-out iterates.

> **Why a control table?** A `pipeline_task` accepts only `pipeline_id` and
> `full_refresh` — it cannot carry per-run parameters. Static values therefore travel
> through pipeline `configuration:`, and anything that must change without a redeploy
> travels through the control table.

---

## Orchestrator task graph

```
publish_site_config          seeds the control table; publishes active_sites
        ▼
simulate_site_data           for_each over active_sites, concurrency 5, site = {{input}}
        ▼
run_sdp_pipeline             run_if: ALL_DONE — one bad site must not block ingest
        ▼
validate_site_outputs        for_each; asserts gold rows exist per site
        ▼
publish_run_summary          run_if: ALL_DONE — always records the outcome
```

---

## Repository layout

```
databricks.yml                     bundle: variables and dev/prod targets
resources/
  schemas.yml                      UC schemas + landing volume
  wind_turbine_sdp.pipeline.yml    the SDP pipeline + STATIC parameters
  orchestrator.job.yml             chained workflow (static + dynamic parameters)
  simulator.job.yml                standalone test-data generation
src/turbine_pipeline/
  lib/                             pure, importable, unit-tested logic
    sites.py                       site and turbine roster
    physics.py                     power curve, diurnal and location factors
    config.py                      parses string config; threshold resolution
    schemas.py                     explicit JSON schemas
    transforms.py                  DataFrame -> DataFrame transformations
  transformations/                 thin @dp wrappers (the pipeline's glob target)
src/jobs/                          job task scripts
scripts/
  bootstrap_catalog.py             creates the catalog (a DAB cannot - see below)
  run_spark_tests.py               Windows-only Spark test runner
  show_run_error.py                prints the real error behind a failed run
tests/                             pytest suite
```

Transformation logic lives in `lib/` and the `@dp`-decorated modules stay thin, so the
real logic is importable and testable without a pipeline.

---

## Deploying

### Prerequisites
- Databricks CLI v1.24+, authenticated (`databricks auth profiles`)
- A SQL warehouse (used only by the catalog bootstrap)

### One-off per environment

```bash
python scripts/bootstrap_catalog.py --target dev
```

> **Why this step exists.** This workspace has Default Storage enabled, and the
> catalogs API rejects creation without a managed location
> (`400 INVALID_STATE: Metastore storage root URL does not exist`) — both via
> `databricks catalogs create` and via a DAB `catalogs:` resource. `CREATE CATALOG`
> over SQL does work, so the script runs that. It is idempotent and stops the warehouse
> again afterwards.

### Deploy and run

```bash
databricks bundle validate --strict --target dev
databricks bundle deploy --target dev

databricks bundle run wind_turbine_simulator   --target dev   # generate test data
databricks bundle run wind_turbine_orchestrator --target dev  # full chain
```

Production is defined in the same bundle and deploys through the manual
`cd-prod.yml` workflow.

---

## Testing

```bash
pytest                                # pure-Python tests + bundle convention checks
python scripts/run_spark_tests.py     # Spark transformation tests (Windows)
ruff check src tests scripts
```

- `test_physics.py`, `test_config.py` — pure Python, no Spark.
- `test_bundle_naming.py` — asserts every object name ends in `_copper_ibis`, that
  pipeline config is string-only, that the edition supports Auto CDC, and that every
  task has compute. A convention only written down drifts; this fails CI instead.
- `test_transforms.py` — the real transformation logic against a local Spark session.

> **Windows note.** PySpark workers cannot run inside a pytest process on Windows: the
> worker socket resets and jobs fail with `SocketException: Connection reset` /
> `WinError 10038`. Reproduced on pytest 8.3 **and** 9.2, so it is not a pytest
> regression — the same code passes in a plain Python process. Those tests therefore
> skip under pytest on Windows and run via `scripts/run_spark_tests.py`, which executes
> the very same test classes. On Linux CI they run under pytest normally.

---

## CI/CD

| Workflow | Trigger | Does |
|---|---|---|
| `ci.yml` | PR, push to main | ruff, pytest, `bundle validate --strict` on every target |
| `cd-ci.yml` | merge to main | bootstrap catalog, deploy the shared `ci` environment |
| `cd-prod.yml` | manual only | confirmation input, tests, approval gate, deploy prod |

### Targets

| Target | Catalog | Schema prefix | Who deploys |
|---|---|---|---|
| `dev` | `wind_dev_copper_ibis` | `dev_<user>_` | a developer, from their machine |
| `ci` | `wind_dev_copper_ibis` | `ci_` | GitHub Actions, on merge to main |
| `prod` | `wind_prod_copper_ibis` | *(none)* | GitHub Actions, manual dispatch |

`dev` uses `mode: development`, which prefixes every object with the deploying
identity — ideal for per-developer sandboxes that never collide. `ci` deliberately
uses `mode: production` instead: under development mode a CI deploy would prefix
objects with the *service principal's* name and stand up a whole second stack rather
than updating the shared one. Production mode keeps those names stable. `ci` still
points at the dev catalog and its schedule stays paused, since only `prod` unpauses
it.

### Authentication

CI authenticates as a dedicated service principal using OAuth M2M (client credentials).
No personal token and no secret is ever committed.

| Item | Value |
|---|---|
| Service principal | `wind_turbine_ci_copper_ibis` |
| Application (client) ID | `d4dc6166-ad2a-4490-a4e7-37cce791f5a9` |
| Workspace host | `https://dbc-0090b005-ba2d.cloud.databricks.com` |

Its access is declared in the bundle (`var.ci_service_principal`), so every deploy
re-grants `CAN_MANAGE` on the jobs and pipeline rather than depending on manual
one-off grants. It also holds catalog privileges (`USE_CATALOG`, `CREATE_SCHEMA`,
`CREATE_TABLE`, `CREATE_VOLUME`, `MODIFY`, `SELECT`, `READ_VOLUME`, `WRITE_VOLUME`)
and `CAN_USE` on the SQL warehouse the catalog bootstrap needs.

#### Issuing the client secret

Generate the secret and paste it **straight into GitHub** — never into a terminal, a
file, a chat window, or anywhere it would be captured in history.

1. Databricks **Account console → User management → Service principals →
   `wind_turbine_ci_copper_ibis` → Secrets → Generate secret**. Copy the value; it is
   shown exactly once.
2. GitHub → repo **Settings → Secrets and variables → Actions → New repository
   secret**, and add:

   | Name | Value |
   |---|---|
   | `DATABRICKS_HOST` | `https://dbc-0090b005-ba2d.cloud.databricks.com` |
   | `DATABRICKS_CLIENT_ID` | `d4dc6166-ad2a-4490-a4e7-37cce791f5a9` |
   | `DATABRICKS_CLIENT_SECRET` | the generated secret |

3. For prod approvals, create a `production` GitHub Environment with required
   reviewers — `cd-prod.yml` gates on it.

If a secret is ever exposed, revoke it immediately in the same Secrets tab and
generate a new one; rotating costs nothing and a leaked secret grants deploy access to
the whole catalog.

---

## Operational notes

- **Full refresh destroys streaming state** across every site. Avoid it unless you
  intend to reprocess from scratch.
- **Serverless cold start** takes a few minutes on a pipeline's first run — not a hang.
- **`stored_as_scd_type`** is the integer `2` for Type 2 but the string `"1"` for
  Type 1.
- **Never set `cloudFiles.schemaLocation`** — the pipeline manages it.
- **A streaming table cannot become a materialized view in place** (or vice versa); the
  table has to be dropped first.
- Debug a failed run with `python scripts/show_run_error.py <run_id>` — `bundle run`
  reports only "Some iterations failed" for a `for_each` task, while the real exception
  sits on the nested iteration run.
