"""GEPI (Escalation Pressure Index) channel definitions.
Each channel pulls structured signals from specific data sources.
"""
from typing import Dict, List, Any

GEPI_CHANNELS = [
    "military_activity",
    "sanctions_activity",
    "hostile_rhetoric",
    "domestic_unrest",
    "diplomacy_breakdown",
    "supply_chain_tension",
]

# Data source mapping: channel -> (table/query description, date_column, value_extraction)
CHANNEL_CONFIG: Dict[str, Dict[str, Any]] = {
    "military_activity": {
        "sources": ["border_incidents", "military_movement", "conflict_events"],
        "description": "Conflict incidents, military movements, border incidents",
    },
    "sanctions_activity": {
        "sources": ["sanctions_registry"],
        "description": "New sanctions, designations",
    },
    "hostile_rhetoric": {
        "sources": ["articles"],  # sentiment/topics proxy; can extend with rhetoric table
        "description": "Rhetoric sentiment from articles (proxy)",
    },
    "domestic_unrest": {
        "sources": ["protest_tracking"],
        "description": "Protests, domestic unrest events",
    },
    "diplomacy_breakdown": {
        "sources": ["treaties"],
        "description": "Treaty breakdowns, withdrawal signals",
    },
    "supply_chain_tension": {
        "sources": ["chokepoints", "chokepoint_flows"],
        "description": "Chokepoint risk, supply chain disruption",
    },
}

GEPI_WEIGHTS = {
    "military_activity": 0.30,
    "sanctions_activity": 0.20,
    "hostile_rhetoric": 0.15,
    "domestic_unrest": 0.10,
    "diplomacy_breakdown": 0.15,
    "supply_chain_tension": 0.10,
}

GEPI_WEIGHTS_VERSION = "1.0"
