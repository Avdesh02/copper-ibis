"""Wind turbine physics.

Ported from the original ``04_batch_simulator.py`` so the generated test data stays
physically plausible. Pure Python: no Spark, no I/O, fully unit-tested.
"""

from __future__ import annotations

import math

# Standard three-stage power curve (metres per second).
CUT_IN_MPS = 3.0
RATED_MPS = 12.0
CUT_OUT_MPS = 25.0


def power_output_kw(wind_speed_mps: float, capacity_mw: float) -> float:
    """Power produced at a given wind speed, in kW.

    Below cut-in the rotor does not turn; above cut-out the turbine furls for safety.
    Between cut-in and rated, output follows the cube law. Above rated (and below
    cut-out) the turbine holds its rated output.
    """
    if capacity_mw <= 0:
        raise ValueError(f"capacity_mw must be positive, got {capacity_mw}")

    rated_kw = capacity_mw * 1000.0

    if wind_speed_mps < CUT_IN_MPS or wind_speed_mps >= CUT_OUT_MPS:
        return 0.0
    if wind_speed_mps >= RATED_MPS:
        return round(rated_kw, 2)

    fraction = (wind_speed_mps - CUT_IN_MPS) / (RATED_MPS - CUT_IN_MPS)
    return round(rated_kw * fraction**3, 2)


def diurnal_factor(hour: int) -> float:
    """Time-of-day wind multiplier; wind picks up through the afternoon.

    Peaks at noon, floors at 0.3 so night-time wind never reaches zero.
    """
    if not 0 <= hour <= 23:
        raise ValueError(f"hour must be 0-23, got {hour}")
    return max(0.3, 0.5 + 0.7 * math.sin((hour - 6) * math.pi / 12))


def expected_wind_speed(hour: int, wind_factor: float, base_mps: float = 8.0) -> float:
    """Mean wind speed for a site at a given hour, before random variation."""
    return base_mps * wind_factor * diurnal_factor(hour)


def blade_rpm(wind_speed_mps: float) -> float:
    """Rotor speed, roughly linear in wind speed up to the rated point."""
    if wind_speed_mps < CUT_IN_MPS or wind_speed_mps >= CUT_OUT_MPS:
        return 0.0
    return round(min(wind_speed_mps / RATED_MPS, 1.0) * 18.0, 2)


def oil_temperature_c(temp_base_c: float, load_fraction: float) -> float:
    """Gearbox oil temperature: ambient plus heat from the load."""
    load_fraction = max(0.0, min(1.0, load_fraction))
    return round(temp_base_c + 35.0 * load_fraction, 2)


def gear_friction(load_fraction: float) -> float:
    """Dimensionless friction coefficient, rising with load."""
    load_fraction = max(0.0, min(1.0, load_fraction))
    return round(0.25 + 0.35 * load_fraction, 4)
