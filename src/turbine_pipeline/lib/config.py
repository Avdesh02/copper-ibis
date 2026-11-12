"""Pipeline configuration parsing.

Databricks pipeline ``configuration:`` values are **always strings**, so every typed or
structured value has to be encoded and parsed back here. Keeping that parsing in one
pure module means it can be unit-tested without Spark.

Key layout (set in ``resources/wind_turbine_sdp.pipeline.yml``)::

    turbine.catalog                                    -> str
    turbine.sites                                      -> comma-separated site ids
    turbine.landing.sensors_path                       -> str
    turbine.window_duration                            -> str, e.g. "5 minutes"
    turbine.threshold.<site>.<metric>.<bound>          -> float, site override
    turbine.threshold.default.<metric>.<bound>         -> float, fallback

``<bound>`` is one of ``warn_min``, ``warn_max``, ``critical_min``, ``critical_max``.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_SITE_KEY = "default"
BOUNDS = ("warn_min", "warn_max", "critical_min", "critical_max")


@dataclass(frozen=True)
class Thresholds:
    """Alert bounds for one metric at one site. ``None`` means unbounded."""

    warn_min: float | None = None
    warn_max: float | None = None
    critical_min: float | None = None
    critical_max: float | None = None

    def classify(self, value: float | None) -> str:
        """Severity for a reading: CRITICAL, WARN or OK.

        Critical is checked first so an extreme value is never reported as a mere
        warning. A ``None`` reading cannot be judged and is treated as OK.
        """
        if value is None:
            return "OK"
        if self.critical_max is not None and value > self.critical_max:
            return "CRITICAL"
        if self.critical_min is not None and value < self.critical_min:
            return "CRITICAL"
        if self.warn_max is not None and value > self.warn_max:
            return "WARN"
        if self.warn_min is not None and value < self.warn_min:
            return "WARN"
        return "OK"


def parse_site_list(raw: str | None) -> list[str]:
    """Split a comma-separated site list, ignoring blanks and stray whitespace."""
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def _to_float(raw: str | None) -> float | None:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def resolve_thresholds(conf: dict[str, str], site_id: str, metric: str) -> Thresholds:
    """Thresholds for one site and metric, falling back to the defaults per bound.

    Fallback is per-bound rather than all-or-nothing, so a site can override just its
    ``warn_max`` and still inherit the default critical bounds.
    """
    values: dict[str, float | None] = {}
    for bound in BOUNDS:
        site_val = _to_float(conf.get(f"turbine.threshold.{site_id}.{metric}.{bound}"))
        if site_val is None:
            site_val = _to_float(conf.get(f"turbine.threshold.{DEFAULT_SITE_KEY}.{metric}.{bound}"))
        values[bound] = site_val
    return Thresholds(**values)


def get_conf(spark, key: str, default: str | None = None) -> str | None:
    """Read a pipeline configuration value, tolerating absence.

    ``spark.conf.get`` raises when a key is missing in some runtimes and returns ``None``
    in others; this normalises both to ``default``.
    """
    try:
        value = spark.conf.get(key)
    except Exception:
        return default
    return default if value is None else value


def load_conf(spark, keys: list[str]) -> dict[str, str]:
    """Collect the given configuration keys into a plain dict (absent keys omitted)."""
    out: dict[str, str] = {}
    for key in keys:
        value = get_conf(spark, key)
        if value is not None:
            out[key] = value
    return out


def threshold_conf_keys(site_ids: list[str], metrics: list[str]) -> list[str]:
    """Every threshold key worth reading, for the sites and metrics in play."""
    keys = []
    for site in [DEFAULT_SITE_KEY, *site_ids]:
        for metric in metrics:
            keys.extend(f"turbine.threshold.{site}.{metric}.{bound}" for bound in BOUNDS)
    return keys
