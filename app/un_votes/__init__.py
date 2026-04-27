"""UN vote analytics for geopolitical indices (GPI)."""
from .schema import init_gpi_un_tables, migrate_un_votes_to_gpi_raw

__all__ = ["init_gpi_un_tables", "migrate_un_votes_to_gpi_raw"]
