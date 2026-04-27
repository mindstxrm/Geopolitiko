"""Run institutional models: GEPI, CDEI, SFI, GEG, alignment, fragility, escalation."""
from datetime import datetime


def run_institutional_models(as_of: str | None = None) -> dict:
    """Run all institutional model computations. Returns summary."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    out = {}
    from app.institutional_models.gepi_compute import run_gepi
    gepi = run_gepi(as_of)
    out["gepi"] = gepi
    from app.institutional_models.cdei_compute import compute_cdei
    out["cdei_count"] = compute_cdei(as_of)
    from app.institutional_models.sfi_compute import compute_sfi
    out["sfi_count"] = compute_sfi(as_of)
    from app.institutional_models.geg_compute import run_geg
    n1, n2 = run_geg(as_of)
    out["geg_events"] = n1
    out["geg_links"] = n2
    from app.institutional_models.alignment_compute import compute_tbcs, compute_multi_layer
    out["tbcs_count"] = compute_tbcs(as_of)
    out["alignment_multi_count"] = compute_multi_layer(as_of)
    from app.institutional_models.fragility_compute import compute_fragility
    out["fragility_count"] = compute_fragility(as_of)
    from app.institutional_models.escalation_compute import run_backtest
    out["escalation_backtest"] = run_backtest()
    return out
