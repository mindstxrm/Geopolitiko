"""
World Monitor layer catalog and view presets.
Mirrors patterns from koala73/worldmonitor source:
- src/config/map-layer-definitions.ts: LAYER_REGISTRY, def(), VARIANT_LAYER_ORDER
- src/utils/urlState.ts: TIME_RANGES, VIEW_VALUES, parseMapUrlState, buildMapUrl
Single source of truth for layer ids, labels, live flag, icons, and map view presets.
"""

# Layer: (id, fallback_label, is_live, description, icon)
# Order matches reference: Iran Attacks, Intel Hotspots, Conflict Zones, Military Bases, Nuclear,
# Gamma Irradiators, Spaceports, Undersea Cables, Pipelines, AI Data Centers, Military Activity, etc.
WORLD_MONITOR_LAYERS = [
    ("iranAttacks", "Iran Attacks", True, "News: Iran + attacks/strikes/missiles/drones", ""),
    ("hotspots", "Intel Hotspots", True, "High risk-score countries", ""),
    ("conflicts", "Conflict Zones", True, "Military movement / conflict events", ""),
    ("bases", "Military Bases", False, "Military bases", ""),
    ("nuclear", "Nuclear Sites", False, "Nuclear facilities", ""),
    ("gamma_irradiators", "Gamma Irradiators", False, "Industrial irradiator facilities", ""),
    ("spaceports", "Spaceports", False, "Launch and space facilities", ""),
    ("undersea_cables", "Undersea Cables", False, "Submarine cable landing points", ""),
    ("pipelines", "Pipelines", False, "Key pipeline nodes and compressor stations", ""),
    ("ai_datacenters", "AI Data Centers", False, "Major AI/cloud data centers", ""),
    ("military", "Military Activity", True, "Naval, border incidents, exercises", ""),
    ("waterways", "Strategic Waterways", False, "Chokepoints", ""),
    ("weather", "Weather Alerts", False, "Climate / vulnerability", ""),
    ("economic", "Economic Centers", False, "Macro / food inflation stress", ""),
    ("outages", "Internet Outages", False, "Critical infrastructure", ""),
    ("natural", "Natural Events", False, "Natural disaster / climate risk", ""),
    ("sanctions", "Sanctions", False, "Sanctioned countries", ""),
]

# Time range URL value -> days. Their urlState.ts: TIME_RANGES = ['1h','6h','24h','48h','7d','all']
# We use day granularity; 1h/6h/24h -> 1d, 48h -> 2d.
WORLD_MONITOR_TIME_RANGES = {
    "1h": 1,
    "6h": 1,
    "1d": 1,
    "24h": 1,
    "2d": 2,
    "48h": 2,
    "7d": 7,
    "30d": 30,
    "all": 365,
}

# View preset key -> (lat, lon, zoom). Their urlState.ts: VIEW_VALUES = ['global','america','mena','eu','asia','latam','africa','oceania']
WORLD_MONITOR_VIEW_PRESETS = {
    "global": (20.0, 0.0, 1.5),
    "america": (39.0, -98.0, 3.0),
    "mena": (25.0, 45.0, 3.0),
    "eu": (50.0, 10.0, 3.0),
    "asia": (25.0, 100.0, 2.0),
    "latam": (20.0, -60.0, 2.0),
    "africa": (0.0, 20.0, 2.0),
    "oceania": (25.0, 135.0, 2.0),
}

# Human-readable labels for view presets (for toolbar)
WORLD_MONITOR_VIEW_LABELS = {
    "global": "Global",
    "america": "Americas",
    "mena": "MENA",
    "eu": "Europe",
    "asia": "Asia",
    "latam": "Latin America",
    "africa": "Africa",
    "oceania": "Oceania",
}


def get_world_monitor_layer_ids():
    return [row[0] for row in WORLD_MONITOR_LAYERS]


def get_world_monitor_live_layer_ids():
    return {row[0] for row in WORLD_MONITOR_LAYERS if row[2]}


def get_world_monitor_layer_labels():
    return {row[0]: row[1] for row in WORLD_MONITOR_LAYERS}


def get_world_monitor_layer_icons():
    """Optional icon per layer (from their LAYER_REGISTRY). Empty string = no icon."""
    return {row[0]: row[4] for row in WORLD_MONITOR_LAYERS if len(row) > 4 and row[4]}


def get_world_monitor_time_days(time_range_key):
    """Return days for time range. Their TIME_RANGES: 1h,6h,24h,48h,7d,all. We normalize 1h/24h->1, 48h->2."""
    key = (time_range_key or "").strip().lower()
    days = WORLD_MONITOR_TIME_RANGES.get(key, 1)
    return max(1, min(365, days)) if days else 1


def get_view_preset_coords(view_key):
    """Return (lat, lon, zoom) for view param; None if unknown. Mirrors their parseMapUrlState(view)."""
    key = (view_key or "").strip().lower()
    return WORLD_MONITOR_VIEW_PRESETS.get(key)


def clamp_zoom(zoom, min_z=1, max_z=10):
    return max(min_z, min(max_z, float(zoom)))


def clamp_lat(lat):
    return max(-90.0, min(90.0, float(lat)))


def clamp_lon(lon):
    return max(-180.0, min(180.0, float(lon)))
