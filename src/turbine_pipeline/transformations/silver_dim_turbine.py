"""Turbine dimension — AUTO CDC (change data flow) producing SCD Type 2 history.

Turbine attributes genuinely change: TURB-003 moves in and out of maintenance, capacity
is re-rated. Keeping history means a reading can be interpreted against the turbine as
it was at the time, not only as it is now.

Auto CDC writes ``__START_AT`` / ``__END_AT``; the current row per key is the one with
``__END_AT IS NULL``. SCD Type 2 requires pipeline edition ADVANCED.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from turbine_pipeline.lib.config import get_conf

BRONZE_SCHEMA = get_conf(spark, "turbine.schema.bronze", "bronze")  # noqa: F821
SILVER_SCHEMA = get_conf(spark, "turbine.schema.silver", "silver")  # noqa: F821


@dp.temporary_view(name="turbine_changes_clean")
def turbine_changes_clean():
    """Pre-filter the raw feed before CDC.

    ``create_auto_cdc_flow`` takes a table or view *name*, never a DataFrame, so any
    filtering has to happen in a view like this one.
    """
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.bronze_turbine_cdc_copper_ibis")  # noqa: F821
        .filter(F.col("turbine_id").isNotNull())
        .filter(F.col("change_seq").isNotNull())
        .withColumn("changed_at", F.to_timestamp("changed_at"))
    )


dp.create_streaming_table(
    name=f"{SILVER_SCHEMA}.dim_turbine_copper_ibis",
    comment="SCD Type 2 turbine dimension; current rows have __END_AT IS NULL.",
    cluster_by=["turbine_id"],
)

dp.create_auto_cdc_flow(
    target=f"{SILVER_SCHEMA}.dim_turbine_copper_ibis",
    source="turbine_changes_clean",
    keys=["turbine_id"],
    # Monotonic per turbine, so ordering is deterministic even if two changes share a
    # timestamp.
    sequence_by=F.col("change_seq"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    stored_as_scd_type=2,  # int 2 for Type 2 (Type 1 would be the string "1")
    except_column_list=["operation", "_rescued_data", "_ingested_at", "_source_file"],
)
