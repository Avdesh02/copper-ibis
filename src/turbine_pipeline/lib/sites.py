"""Site and turbine roster.

Ported from the original ``static/turbine_info.json``. A *site* is one Indian state,
holding exactly one turbine. Site ids are the parameterization key used throughout the
bundle, the job fan-out and the landing-zone layout.
"""

from __future__ import annotations

from dataclasses import dataclass, field

MODEL_SUFFIX = "copper_ibis"


@dataclass(frozen=True)
class Turbine:
    turbine_id: str
    site_id: str
    state: str
    capacity_mw: float
    commission_date: str
    status: str
    hub_height_m: float
    rotor_diameter_m: float


@dataclass(frozen=True)
class Site:
    site_id: str
    state: str
    # Multiplies base wind speed. Rajasthan and Gujarat are the windiest.
    wind_factor: float
    # Mean ambient temperature, drives the oil-temperature baseline.
    temp_base_c: float
    turbines: list[Turbine] = field(default_factory=list)


_TURBINES = [
    Turbine("TURB-001", "rajasthan", "Rajasthan", 2.1, "2022-03-15", "active", 100.0, 80.0),
    Turbine("TURB-002", "tamil_nadu", "Tamil Nadu", 3.0, "2021-11-20", "active", 120.0, 100.0),
    Turbine("TURB-003", "maharashtra", "Maharashtra", 1.5, "2022-06-05", "maintenance", 80.0, 60.0),
    Turbine("TURB-004", "gujarat", "Gujarat", 2.5, "2020-12-10", "active", 95.0, 75.0),
    Turbine("TURB-005", "karnataka", "Karnataka", 3.2, "2023-01-25", "active", 110.0, 110.0),
]

# wind_factor / temp_base_c carried over from the original simulator's lookup tables.
_SITE_ENV = {
    "rajasthan": (1.3, 42.0),
    "tamil_nadu": (1.1, 35.0),
    "maharashtra": (0.9, 33.0),
    "gujarat": (1.2, 38.0),
    "karnataka": (1.0, 30.0),
}

SITES: dict[str, Site] = {
    t.site_id: Site(
        site_id=t.site_id,
        state=t.state,
        wind_factor=_SITE_ENV[t.site_id][0],
        temp_base_c=_SITE_ENV[t.site_id][1],
        turbines=[t],
    )
    for t in _TURBINES
}

ALL_SITE_IDS = sorted(SITES)


def get_site(site_id: str) -> Site:
    """Look up a site, failing loudly on an unknown id."""
    try:
        return SITES[site_id]
    except KeyError:
        raise KeyError(f"unknown site_id {site_id!r}; known sites: {ALL_SITE_IDS}") from None


def turbines_for_site(site_id: str) -> list[Turbine]:
    return list(get_site(site_id).turbines)


def suffixed(name: str) -> str:
    """Append the git-folder naming suffix, without doubling it."""
    return name if name.endswith(f"_{MODEL_SUFFIX}") else f"{name}_{MODEL_SUFFIX}"
