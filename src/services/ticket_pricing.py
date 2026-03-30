"""
Ticket price baselines and admissions estimation.

Egypt: ticket price is derived from Filmyard gross ÷ admissions (most accurate).
Other markets: carefully researched baseline prices based on operator pricing data.
Prices are stored in config/ticket_prices.json and can be updated via the dashboard.
"""
from __future__ import annotations

import json
import os
from collections.abc import Iterable
from pathlib import Path

_CONFIG_PATH = Path(__file__).parents[2] / "config" / "ticket_prices.json"

# Default fallback prices (used if config file is missing)
_DEFAULT_PRICES: dict[str, float] = {
    "EG": 180.0,  # EGP — fallback; real price derived from Filmyard when available
    "SA": 40.0,   # SAR — AMC / Muvi / VOX, avg ~38–45
    "AE": 55.0,   # AED — multiplex avg ~48–60; aligns ~$15 USD/ticket at peg (BOM reports USD)
    "KW": 3.5,    # KWD — ~3–4
    "BH": 3.5,    # BHD — ~3–4
    "QA": 42.0,   # QAR — ~35–50
    "OM": 2.5,    # OMR — ~2–3
    "JO": 6.0,    # JOD — ~5–7
    # LB excluded — LBP instability makes estimation unreliable
}

_CURRENCY_LABELS: dict[str, str] = {
    "EG": "EGP", "SA": "SAR", "AE": "AED", "KW": "KWD",
    "BH": "BHD", "QA": "QAR", "OM": "OMR", "JO": "JOD",
}


def load_prices() -> dict[str, float]:
    """Load ticket prices from config file, falling back to defaults."""
    try:
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            data = json.load(f)
        # Merge with defaults so any missing keys use defaults
        merged = dict(_DEFAULT_PRICES)
        merged.update({k.upper(): float(v) for k, v in data.items()})
        return merged
    except Exception:
        return dict(_DEFAULT_PRICES)


def save_prices(prices: dict[str, float]) -> None:
    """Persist ticket prices to config file."""
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump({k.upper(): v for k, v in prices.items()}, f, indent=2)


# Module-level cache — refreshed by calling load_prices() explicitly
BASELINE_TICKET_PRICES: dict[str, float] = load_prices()

# Box Office Mojo reports many territories in USD while ticket baselines are local currency.
# Pegged: AED, SAR, BHD (approx). Floating: set via env and refresh periodically for estimates only.
_AED_PER_USD: float = 3.6725
_SAR_PER_USD: float = 3.75
# Local currency units per 1 USD (multiply BOM USD gross for ticket math).
_EGP_PER_USD: float = float(os.getenv("EGP_PER_USD", "50.5"))
_KWD_PER_USD: float = float(os.getenv("KWD_PER_USD", "0.308"))
_BHD_PER_USD: float = float(os.getenv("BHD_PER_USD", "0.376"))
_QAR_PER_USD: float = float(os.getenv("QAR_PER_USD", "3.64"))
_OMR_PER_USD: float = float(os.getenv("OMR_PER_USD", "0.385"))
_JOD_PER_USD: float = float(os.getenv("JOD_PER_USD", "0.709"))


def gross_in_local_currency_for_estimate(
    gross: float,
    *,
    currency: str | None,
    market_code: str | None,
) -> float:
    """Express gross in the same currency as ticket baselines for division.

    When ``currency`` is USD and the market baseline is local (e.g. EGP for Egypt), multiply
    by an approximate **local per USD** rate so admissions ≈ gross_local ÷ ticket_price.
    Override rates with env: ``EGP_PER_USD``, ``KWD_PER_USD``, etc.
    """
    if gross <= 0:
        return gross
    c = (currency or "").strip().upper()
    m = (market_code or "").strip().upper()
    if c != "USD":
        return gross
    if m == "AE":
        return gross * _AED_PER_USD
    if m == "SA":
        return gross * _SAR_PER_USD
    if m == "EG":
        return gross * _EGP_PER_USD
    if m == "KW":
        return gross * _KWD_PER_USD
    if m == "BH":
        return gross * _BHD_PER_USD
    if m == "QA":
        return gross * _QAR_PER_USD
    if m == "OM":
        return gross * _OMR_PER_USD
    if m == "JO":
        return gross * _JOD_PER_USD
    return gross


def sum_gross_in_ticket_currency(rows: Iterable[dict], market_code: str) -> float:
    """Sum ``period_gross_local`` per row after USD→local conversion for ``market_code``."""
    m = (market_code or "").strip().upper()
    total = 0.0
    for r in rows:
        g = float(r.get("period_gross_local") or 0)
        if g <= 0:
            continue
        cur = (r.get("currency") or "").strip() or None
        total += gross_in_local_currency_for_estimate(g, currency=cur, market_code=m)
    return total


def ticket_currency_code(market_code: str) -> str | None:
    """Currency code matching :func:`estimate_admissions` baseline for this market."""
    return _CURRENCY_LABELS.get((market_code or "").strip().upper())


def estimate_admissions(
    gross_local: float,
    market_code: str,
    ticket_price_override: float | None = None,
    *,
    gross_currency: str | None = None,
) -> int | None:
    """Estimate admissions from gross and market ticket baseline.

    Args:
        gross_local: Period (or cumulative) gross as stored on the evidence row.
        market_code: ISO country code (e.g. "SA", "AE").
        ticket_price_override: If provided (e.g. Egypt price derived from Filmyard),
            use this instead of the market baseline.
        gross_currency: ISO code for gross_local (e.g. USD for BOM). When USD and the
            baseline is local (EGP, AED, SAR, KWD, …), gross is converted before
            dividing by ticket price.
    """
    if gross_local <= 0:
        return None
    prices = load_prices()
    price = ticket_price_override if ticket_price_override is not None else prices.get(
        (market_code or "").upper()
    )
    if price is None or price <= 0:
        return None
    gross_for_price = gross_in_local_currency_for_estimate(
        float(gross_local),
        currency=gross_currency,
        market_code=market_code,
    )
    return int(round(gross_for_price / price))


def price_basis_label(market_code: str, derived_price: float | None = None) -> str:
    """Return a short label describing the basis of the ticket price used."""
    code = (market_code or "").upper()
    currency = _CURRENCY_LABELS.get(code, "")
    if derived_price is not None and code == "EG":
        return f"Filmyard-derived ~{derived_price:,.0f} EGP"
    prices = load_prices()
    price = prices.get(code)
    if price is None:
        return "no price data"
    return f"~{price:g} {currency}"
