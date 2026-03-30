from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import httpx
import joblib
import pandas as pd

from src.config import get_settings


@dataclass(frozen=True)
class InferenceInput:
    eg_adm: float
    sa_adm: float
    ae_adm: float
    imdb: float
    elcinema: float
    letterboxd_rating: float
    letterboxd_votes: int
    stability: float
    marketing_spend: float


def _resolve_model_path() -> Path:
    settings = get_settings()
    configured = Path(settings.model_path)
    if configured.is_absolute():
        return configured
    root = Path(__file__).resolve().parents[2]
    return root / configured


@lru_cache(maxsize=1)
def load_model():
    model_path = _resolve_model_path()
    return joblib.load(model_path)


def _feature_frame(inp: InferenceInput) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "eg_total_admissions": inp.eg_adm,
                "sa_total_admissions": inp.sa_adm,
                "ae_total_admissions": inp.ae_adm,
                "imdb_rating": inp.imdb,
                "elcinema_rating": inp.elcinema,
                "letterboxd_rating": inp.letterboxd_rating,
                "letterboxd_votes": inp.letterboxd_votes,
                "avg_stability": inp.stability,
                "total_marketing_spend": inp.marketing_spend,
            }
        ]
    )


def predict_first_watch(inp: InferenceInput) -> float:
    model = load_model()
    return float(model.predict(_feature_frame(inp))[0])


def calculate_roi(first_watch: float, marketing_spend: float) -> float:
    return first_watch / marketing_spend


def revenue_estimate(first_watch: float) -> float:
    return first_watch * 1.2


def profit_estimate(first_watch: float, marketing_spend: float) -> float:
    return revenue_estimate(first_watch) - marketing_spend


def decision_from_roi(roi: float) -> str:
    if roi > 4:
        return "STRONG BUY"
    if roi > 2.5:
        return "BUY"
    if roi > 1.5:
        return "RISKY"
    return "DO NOT BUY"


def run_investment_analysis(inp: InferenceInput) -> dict:
    fw = predict_first_watch(inp)
    roi = calculate_roi(fw, inp.marketing_spend)
    revenue = revenue_estimate(fw)
    profit = profit_estimate(fw, inp.marketing_spend)
    return {
        "predicted_first_watch": round(fw),
        "roi": round(roi, 2),
        "revenue": round(revenue),
        "profit": round(profit),
        "decision": decision_from_roi(roi),
    }


def suggest_spend_from_target(
    *,
    target_first_watch: float,
    eg_adm: float,
    sa_adm: float,
    ae_adm: float,
    imdb: float,
    elcinema: float,
    letterboxd_rating: float,
    letterboxd_votes: int,
    stability: float,
) -> dict:
    model = load_model()
    coef = model.coef_
    intercept = model.intercept_
    a, b, c, d, e, f, g, h, i = coef
    if i == 0:
        return {"error": "marketing_spend coefficient is zero; cannot solve required spend."}
    spend = (
        target_first_watch
        - (
            a * eg_adm
            + b * sa_adm
            + c * ae_adm
            + d * imdb
            + e * elcinema
            + f * letterboxd_rating
            + g * letterboxd_votes
            + h * stability
            + intercept
        )
    ) / i
    return {"suggested_marketing_spend": round(spend)}


def predict_via_api(payload: dict, endpoint: str) -> dict:
    settings = get_settings()
    base = settings.prediction_api_url.rstrip("/")
    resp = httpx.post(f"{base}/{endpoint.lstrip('/')}", json=payload, timeout=20.0)
    resp.raise_for_status()
    body = resp.json()
    return body if isinstance(body, dict) else {}
