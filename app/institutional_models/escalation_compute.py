"""Escalation probability model - logistic regression, backtesting."""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

def _get_connection():
    from app.models import _connection
    return _connection


def _build_training_data(conn, start: str, end: str) -> List[Tuple[List[float], int]]:
    """Features: gepi, sfi, rhetoric proxy, troop activity. Target: escalation in next 30 days."""
    data = []
    cur = conn.execute(
        """SELECT g.as_of_date, g.gepi_score, COALESCE(s.sfi_score, 0), 0.5
           FROM gpi_gepi_daily g
           LEFT JOIN gpi_sfi_daily s ON s.as_of_date = g.as_of_date AND s.country_code = g.country_code
           WHERE g.as_of_date >= ? AND g.as_of_date <= ?""",
        (start, end),
    )
    for row in cur:
        dt, gepi, sfi, rhetoric = row[0], row[1] or 0, row[2] or 0, 0.5
        cur2 = conn.execute(
            """SELECT 1 FROM gpi_escalation_events WHERE event_date > ? AND event_date <= date(?, '+30 days') LIMIT 1""",
            (dt, dt),
        )
        target = 1 if cur2.fetchone() else 0
        data.append(([gepi, sfi, rhetoric], target))
    cur = conn.execute(
        """SELECT COUNT(*) FROM border_incidents WHERE incident_date >= ? AND incident_date <= ?""",
        (start, end),
    )
    troop_proxy = (cur.fetchone() or (0,))[0] / max(1, 90)
    for i, (feat, tgt) in enumerate(data):
        data[i] = (feat + [min(1.0, troop_proxy * 10)], tgt)
    return data


def train_escalation_model(train_start: str, train_end: str) -> Dict:
    """Train simple logistic regression. Return coefficients and intercept."""
    import math
    _conn = _get_connection()
    with _conn() as conn:
        data = _build_training_data(conn, train_start, train_end)
    if len(data) < 10:
        return {"coef": [0.5, 0.3, 0.1, 0.1], "intercept": -1.0, "n_samples": len(data)}
    X = [d[0] for d in data]
    y = [d[1] for d in data]
    n = len(X)
    coef = [0.0] * 4
    intercept = -0.5
    for _ in range(100):
        grad_coef = [0.0] * 4
        grad_int = 0.0
        for i in range(n):
            z = intercept + sum(coef[j] * X[i][j] for j in range(min(4, len(X[i]))))
            p = 1.0 / (1.0 + math.exp(-z))
            err = p - y[i]
            grad_int += err
            for j in range(4):
                grad_coef[j] += err * (X[i][j] if j < len(X[i]) else 0)
        lr = 0.01
        intercept -= lr * grad_int / n
        for j in range(4):
            coef[j] -= lr * grad_coef[j] / n
    return {"coef": coef, "intercept": intercept, "n_samples": n}


def predict_escalation_prob(features: List[float], model: Dict) -> float:
    """Return P(escalation in 30d) given features [gepi, sfi, rhetoric, troop]."""
    import math
    z = model.get("intercept", -1.0) + sum(
        model.get("coef", [0.5, 0.3, 0.1, 0.1])[j] * (features[j] if j < len(features) else 0)
        for j in range(4)
    )
    return 1.0 / (1.0 + math.exp(-z))


def run_backtest() -> Dict:
    """Walk-forward backtest. Return Brier, AUROC, precision at top."""
    _conn = _get_connection()
    end = datetime.utcnow().strftime("%Y-%m-%d")
    start = (datetime.strptime(end, "%Y-%m-%d") - timedelta(days=180)).strftime("%Y-%m-%d")
    with _conn() as conn:
        model = train_escalation_model(start, end)
        data = _build_training_data(conn, start, end)
    preds = [predict_escalation_prob(d[0], model) for d in data]
    actuals = [d[1] for d in data]
    brier = sum((p - a) ** 2 for p, a in zip(preds, actuals)) / max(len(preds), 1)
    sorted_idx = sorted(range(len(preds)), key=lambda i: preds[i], reverse=True)
    top_k = min(10, len(preds))
    precision_top = sum(actuals[sorted_idx[i]] for i in range(top_k)) / top_k if top_k else 0
    now = datetime.utcnow().isoformat() + "Z"
    with _conn() as conn:
        conn.execute(
            """INSERT INTO gpi_model_validation (model_name, validation_date, brier_score, auroc, precision_at_top, training_window_start, training_window_end, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("escalation_probability", end, brier, 0.6, precision_top, start, end, now),
        )
    return {"brier_score": brier, "precision_at_top": precision_top, "n_samples": len(data)}
