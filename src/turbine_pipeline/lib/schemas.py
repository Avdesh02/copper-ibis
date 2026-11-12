"""Explicit schemas for the landing-zone JSON.

Declaring these rather than inferring them keeps Auto Loader deterministic and lets the
rescued-data column catch anything that does not fit, instead of silently widening the
schema.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

RESCUED_COLUMN = "_rescued_data"

# Metrics carried in every sensor event.
SENSOR_METRICS = ("oil_temperature", "blade_velocity", "gear_friction", "power_output")

_READING = StructType(
    [
        StructField("metric", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("quality", StringType(), True),
    ]
)

# Sensor telemetry: one event per turbine per tick, readings nested as an array so the
# payload stays narrow when metrics are added.
SENSOR_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("turbine_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("seq", IntegerType(), True),
        StructField("wind_speed_mps", DoubleType(), True),
        StructField("readings", ArrayType(_READING), True),
        StructField(RESCUED_COLUMN, StringType(), True),
    ]
)

# Turbine master-data change feed, consumed by Auto CDC into an SCD2 dimension.
TURBINE_CDC_SCHEMA = StructType(
    [
        StructField("turbine_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("state", StringType(), True),
        StructField("capacity_mw", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("hub_height_m", DoubleType(), True),
        StructField("rotor_diameter_m", DoubleType(), True),
        StructField("commission_date", StringType(), True),
        StructField("operation", StringType(), True),  # UPSERT | DELETE
        StructField("change_seq", IntegerType(), True),
        StructField("changed_at", StringType(), True),
        StructField(RESCUED_COLUMN, StringType(), True),
    ]
)
