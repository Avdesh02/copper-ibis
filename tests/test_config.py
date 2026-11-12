"""Configuration-parsing tests — pure Python, no Spark required.

These guard the string-encoded pipeline configuration, where a typo would otherwise
surface only as a silently missing threshold at runtime.
"""

from __future__ import annotations

from turbine_pipeline.lib import config
from turbine_pipeline.lib.config import Thresholds


class TestParseSiteList:
    def test_parses_simple_list(self):
        assert config.parse_site_list("a,b,c") == ["a", "b", "c"]

    def test_strips_whitespace_and_blanks(self):
        assert config.parse_site_list(" a , ,b,  ") == ["a", "b"]

    def test_empty_and_none_give_empty_list(self):
        assert config.parse_site_list("") == []
        assert config.parse_site_list(None) == []

    def test_single_site(self):
        assert config.parse_site_list("rajasthan") == ["rajasthan"]


class TestResolveThresholds:
    CONF = {
        "turbine.threshold.default.oil_temperature.warn_max": "85.0",
        "turbine.threshold.default.oil_temperature.critical_max": "95.0",
        "turbine.threshold.default.oil_temperature.warn_min": "5.0",
        # Rajasthan runs hot, so its warn bound is raised but critical is inherited.
        "turbine.threshold.rajasthan.oil_temperature.warn_max": "92.0",
    }

    def test_uses_default_when_no_site_override(self):
        t = config.resolve_thresholds(self.CONF, "karnataka", "oil_temperature")
        assert t.warn_max == 85.0
        assert t.critical_max == 95.0

    def test_site_override_wins(self):
        t = config.resolve_thresholds(self.CONF, "rajasthan", "oil_temperature")
        assert t.warn_max == 92.0

    def test_fallback_is_per_bound_not_all_or_nothing(self):
        """Overriding one bound must not discard the inherited ones."""
        t = config.resolve_thresholds(self.CONF, "rajasthan", "oil_temperature")
        assert t.warn_max == 92.0
        assert t.critical_max == 95.0
        assert t.warn_min == 5.0

    def test_missing_metric_yields_all_none(self):
        t = config.resolve_thresholds(self.CONF, "rajasthan", "unknown_metric")
        assert t == Thresholds()

    def test_malformed_value_treated_as_absent(self):
        conf = {"turbine.threshold.default.m.warn_max": "not-a-number"}
        assert config.resolve_thresholds(conf, "s", "m").warn_max is None

    def test_blank_value_treated_as_absent(self):
        conf = {"turbine.threshold.default.m.warn_max": "  "}
        assert config.resolve_thresholds(conf, "s", "m").warn_max is None


class TestClassify:
    T = Thresholds(warn_min=5.0, warn_max=85.0, critical_min=-10.0, critical_max=95.0)

    def test_ok_inside_bounds(self):
        assert self.T.classify(50.0) == "OK"

    def test_warn_above_warn_max(self):
        assert self.T.classify(90.0) == "WARN"

    def test_critical_above_critical_max(self):
        assert self.T.classify(100.0) == "CRITICAL"

    def test_critical_takes_precedence_over_warn(self):
        """A value past critical also exceeds warn; it must report CRITICAL."""
        assert self.T.classify(1000.0) == "CRITICAL"

    def test_warn_below_warn_min(self):
        assert self.T.classify(0.0) == "WARN"

    def test_critical_below_critical_min(self):
        assert self.T.classify(-50.0) == "CRITICAL"

    def test_boundary_values_are_ok(self):
        """Bounds are exclusive: exactly at the limit is not yet a breach."""
        assert self.T.classify(85.0) == "OK"
        assert self.T.classify(5.0) == "OK"

    def test_none_is_ok(self):
        assert self.T.classify(None) == "OK"

    def test_unbounded_thresholds_never_alert(self):
        assert Thresholds().classify(1e9) == "OK"


class TestConfKeyHelpers:
    def test_threshold_keys_cover_default_and_sites(self):
        keys = config.threshold_conf_keys(["rajasthan"], ["oil_temperature"])
        assert "turbine.threshold.default.oil_temperature.warn_max" in keys
        assert "turbine.threshold.rajasthan.oil_temperature.critical_min" in keys
        assert len(keys) == 2 * 1 * len(config.BOUNDS)


class _FakeConf:
    def __init__(self, data, raise_on_missing=False):
        self._data = data
        self._raise = raise_on_missing

    def get(self, key):
        if key in self._data:
            return self._data[key]
        if self._raise:
            raise Exception("no such key")
        return None


class _FakeSpark:
    def __init__(self, data, raise_on_missing=False):
        self.conf = _FakeConf(data, raise_on_missing)


class TestGetConf:
    def test_reads_present_key(self):
        assert config.get_conf(_FakeSpark({"k": "v"}), "k") == "v"

    def test_missing_key_returns_default(self):
        assert config.get_conf(_FakeSpark({}), "k", "fallback") == "fallback"

    def test_raising_runtime_returns_default(self):
        """Some runtimes raise instead of returning None; both must behave the same."""
        spark = _FakeSpark({}, raise_on_missing=True)
        assert config.get_conf(spark, "k", "fallback") == "fallback"

    def test_load_conf_omits_absent_keys(self):
        spark = _FakeSpark({"a": "1"})
        assert config.load_conf(spark, ["a", "b"]) == {"a": "1"}
