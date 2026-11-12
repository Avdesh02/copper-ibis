"""Gold threshold alerts — streaming table, thresholds parameterized per site.

This is where per-site parameterization becomes visible in the data: the same reading
can be OK at a hot desert site and a WARN at a temperate one.

Thresholds are resolved from pipeline configuration, which is static at deploy time, so
they fold into the query plan as literals rather than costing a join.
"""

from pyspark import pipelines as dp

from turbine_pipeline.lib.config import (
    get_conf,
    load_conf,
    parse_site_list,
    resolve_thresholds,
    threshold_conf_keys,
)
from turbine_pipeline.lib.transforms import (
    ALERTABLE_METRICS,
    attach_thresholds,
    classify_severity,
    select_alerts,
)

SITE_IDS = parse_site_list(get_conf(spark, "turbine.sites"))  # noqa: F821

_conf = load_conf(spark, threshold_conf_keys(SITE_IDS, list(ALERTABLE_METRICS)))  # noqa: F821

# {site_id: {metric: Thresholds}} resolved once, at pipeline-definition time.
SITE_THRESHOLDS = {
    site_id: {
        metric: resolve_thresholds(_conf, site_id, metric) for metric in ALERTABLE_METRICS
    }
    for site_id in SITE_IDS
}


SILVER_SCHEMA = get_conf(spark, "turbine.schema.silver", "silver")  # noqa: F821
GOLD_SCHEMA = get_conf(spark, "turbine.schema.gold", "gold")  # noqa: F821


@dp.table(
    name=f"{GOLD_SCHEMA}.gold_threshold_alerts_copper_ibis",
    comment="Readings that breached a per-site WARN or CRITICAL threshold.",
    cluster_by=["site_id", "severity"],
)
def gold_threshold_alerts_copper_ibis():
    readings = spark.readStream.table(f"{SILVER_SCHEMA}.silver_sensor_readings_copper_ibis")  # noqa: F821
    with_bounds = attach_thresholds(readings, SITE_THRESHOLDS)
    return select_alerts(classify_severity(with_bounds))
