"""Physics tests — pure Python, no Spark required."""

from __future__ import annotations

import pytest

from turbine_pipeline.lib import physics


class TestPowerOutput:
    def test_zero_below_cut_in(self):
        assert physics.power_output_kw(0.0, 2.0) == 0.0
        assert physics.power_output_kw(physics.CUT_IN_MPS - 0.01, 2.0) == 0.0

    def test_zero_at_and_above_cut_out(self):
        """A furled turbine produces nothing, however hard the wind blows."""
        assert physics.power_output_kw(physics.CUT_OUT_MPS, 2.0) == 0.0
        assert physics.power_output_kw(60.0, 2.0) == 0.0

    def test_rated_output_at_and_above_rated_speed(self):
        assert physics.power_output_kw(physics.RATED_MPS, 2.5) == pytest.approx(2500.0)
        assert physics.power_output_kw(20.0, 2.5) == pytest.approx(2500.0)

    def test_monotonic_between_cut_in_and_rated(self):
        speeds = [3.5, 5.0, 7.0, 9.0, 11.0, 12.0]
        outputs = [physics.power_output_kw(s, 3.0) for s in speeds]
        assert outputs == sorted(outputs)
        assert outputs[0] < outputs[-1]

    def test_scales_with_capacity(self):
        small = physics.power_output_kw(8.0, 1.5)
        large = physics.power_output_kw(8.0, 3.0)
        assert large == pytest.approx(small * 2, rel=1e-6)

    def test_rejects_non_positive_capacity(self):
        with pytest.raises(ValueError):
            physics.power_output_kw(10.0, 0.0)


class TestDiurnalFactor:
    def test_floor_is_respected(self):
        assert all(physics.diurnal_factor(h) >= 0.3 for h in range(24))

    def test_afternoon_windier_than_pre_dawn(self):
        assert physics.diurnal_factor(12) > physics.diurnal_factor(3)

    def test_rejects_out_of_range_hour(self):
        for bad in (-1, 24):
            with pytest.raises(ValueError):
                physics.diurnal_factor(bad)


class TestDerivedSignals:
    def test_blade_rpm_zero_outside_operating_range(self):
        assert physics.blade_rpm(1.0) == 0.0
        assert physics.blade_rpm(30.0) == 0.0

    def test_blade_rpm_caps_at_rated(self):
        assert physics.blade_rpm(12.0) == physics.blade_rpm(20.0)

    def test_oil_temperature_rises_with_load(self):
        cold = physics.oil_temperature_c(30.0, 0.0)
        hot = physics.oil_temperature_c(30.0, 1.0)
        assert cold == 30.0
        assert hot > cold

    def test_load_fraction_is_clamped(self):
        """Out-of-range load must not produce absurd temperatures."""
        assert physics.oil_temperature_c(30.0, 5.0) == physics.oil_temperature_c(30.0, 1.0)
        assert physics.oil_temperature_c(30.0, -2.0) == physics.oil_temperature_c(30.0, 0.0)

    def test_gear_friction_rises_with_load(self):
        assert physics.gear_friction(1.0) > physics.gear_friction(0.0)


class TestExpectedWindSpeed:
    def test_windier_site_sees_more_wind(self):
        windy = physics.expected_wind_speed(12, wind_factor=1.3)
        calm = physics.expected_wind_speed(12, wind_factor=0.9)
        assert windy > calm
