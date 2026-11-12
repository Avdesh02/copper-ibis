"""Enforce the `_copper_ibis` naming convention across the bundle.

Every Databricks object this repo creates must carry the git-folder suffix so it never
collides with other work in the shared workspace. A convention that is only written down
drifts; this test fails CI the moment something is added without it.

The YAML is parsed directly rather than shelling out to the CLI, so the test runs
without workspace credentials.
"""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml", reason="PyYAML not installed")

REPO = Path(__file__).resolve().parents[1]
RESOURCES = REPO / "resources"
SUFFIX = "_copper_ibis"


def _load(name: str) -> dict:
    with open(RESOURCES / name, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _resources(kind: str) -> dict:
    """Collect every resource of one kind across all resource files."""
    found: dict[str, dict] = {}
    for path in sorted(RESOURCES.glob("*.yml")):
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh) or {}
        found.update((doc.get("resources") or {}).get(kind) or {})
    return found


class TestNamingConvention:
    def test_jobs_carry_the_suffix(self):
        jobs = _resources("jobs")
        assert jobs, "expected at least one job"
        for key, job in jobs.items():
            assert job["name"].endswith(SUFFIX), f"job {key} is named {job['name']!r}"

    def test_pipelines_carry_the_suffix(self):
        pipelines = _resources("pipelines")
        assert pipelines, "expected at least one pipeline"
        for key, pipeline in pipelines.items():
            assert pipeline["name"].endswith(SUFFIX), f"pipeline {key} is named {pipeline['name']!r}"

    def test_event_log_table_carries_the_suffix(self):
        pipeline = _resources("pipelines")["wind_turbine_sdp"]
        assert pipeline["event_log"]["name"].endswith(SUFFIX)

    def test_catalog_variable_defaults_carry_the_suffix(self):
        with open(REPO / "databricks.yml", encoding="utf-8") as fh:
            root = yaml.safe_load(fh)

        assert root["variables"]["catalog"]["default"].endswith(SUFFIX)
        for target_name, target in root["targets"].items():
            catalog = (target.get("variables") or {}).get("catalog")
            assert catalog, f"target {target_name} does not pin a catalog"
            assert catalog.endswith(SUFFIX), f"target {target_name} uses catalog {catalog!r}"

    def test_bundle_name_carries_the_suffix(self):
        with open(REPO / "databricks.yml", encoding="utf-8") as fh:
            root = yaml.safe_load(fh)
        assert root["bundle"]["name"].endswith(SUFFIX)


class TestPipelineWiring:
    """Guard the settings that are easy to break and expensive to debug."""

    PIPELINE = _resources("pipelines")["wind_turbine_sdp"]

    def test_edition_supports_auto_cdc(self):
        # SCD Type 2 / Auto CDC is rejected on CORE and PRO.
        assert self.PIPELINE["edition"] == "ADVANCED"

    def test_is_serverless(self):
        assert self.PIPELINE["serverless"] is True

    def test_is_not_continuous(self):
        # A continuous pipeline retries failed updates indefinitely.
        assert self.PIPELINE["continuous"] is False

    def test_uses_schema_not_deprecated_target(self):
        assert "schema" in self.PIPELINE
        assert "target" not in self.PIPELINE

    def test_every_configuration_value_is_a_string(self):
        """Pipeline configuration is string-only; a bare number fails at deploy."""
        for key, value in self.PIPELINE["configuration"].items():
            assert isinstance(value, str), f"{key} is {type(value).__name__}, not str"

    def test_site_list_matches_the_roster(self):
        from turbine_pipeline.lib.sites import ALL_SITE_IDS

        configured = self.PIPELINE["configuration"]["turbine.sites"].split(",")
        assert sorted(configured) == sorted(ALL_SITE_IDS)


class TestOrchestratorParameters:
    JOB = _resources("jobs")["wind_turbine_orchestrator"]

    def _params(self) -> dict[str, str]:
        return {p["name"]: str(p.get("default", "")) for p in self.JOB["parameters"]}

    def test_uses_dynamic_references_for_run_scoped_values(self):
        params = self._params()
        assert params["processing_date"] == "{{job.start_time.iso_date}}"
        assert params["run_id"] == "{{job.run_id}}"

    def test_fan_out_reads_the_upstream_task_value(self):
        tasks = {t["task_key"]: t for t in self.JOB["tasks"]}
        fan_out = tasks["simulate_site_data"]["for_each_task"]
        assert fan_out["inputs"] == "{{tasks.publish_site_config.values.active_sites}}"
        assert fan_out["task"]["spark_python_task"]["parameters"].count("{{input}}") == 1

    def test_pipeline_runs_even_if_a_site_fails(self):
        tasks = {t["task_key"]: t for t in self.JOB["tasks"]}
        assert tasks["run_sdp_pipeline"]["run_if"] == "ALL_DONE"

    def test_audit_always_runs(self):
        tasks = {t["task_key"]: t for t in self.JOB["tasks"]}
        assert tasks["publish_run_summary"]["run_if"] == "ALL_DONE"

    def test_every_task_has_compute(self):
        """A task without a cluster or environment fails bundle validation."""

        def check(task):
            if "for_each_task" in task:
                check(task["for_each_task"]["task"])
                return
            if "pipeline_task" in task:
                return  # pipelines bring their own compute
            assert any(
                k in task
                for k in ("environment_key", "job_cluster_key", "existing_cluster_id", "new_cluster")
            ), f"task {task['task_key']} has no compute"

        for task in self.JOB["tasks"]:
            check(task)
