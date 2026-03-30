"""
Commercial Decision Engine
===========================
Additive Layer 3 — evaluates potential new films for acquisition/release.

Reads from existing tables:
    Film, ReconciledEvidence, RatingsMetric, MarketingInput, OutcomeTarget

Reuses from Layer 1:
    release_intelligence.build_market_run_profile  (run-shape, holds, stability)

Writes to: nothing. Purely computational — returns a decision payload.

Pipeline
--------
 1. Build lightweight historical film profiles  (from DB, cached per session)
 2. Find comparable films                       (similarity search)
 3. Compute demand potential                    (ratings + buzz + comparable perf)
 4. Assess marketing dependency                 (run-shape analysis of comparables)
 5. Generate market forecasts                   (floor / base / stretch)
 6. Evaluate target realism                     (target vs forecast)
 7. Assess spend adequacy                       (spend vs comparable benchmarks)
 8. Assess risk                                 (multi-factor)
 9. Compute final decision score + label
10. Generate analyst summary
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    Film, MarketingInput, OutcomeTarget, RatingsMetric, ReconciledEvidence,
)
from src.services.release_intelligence import (
    CORE_MARKETS,
    build_market_run_profile,
)

# ── Constants ──────────────────────────────────────────────────────────────────

MARKET_NAMES = {"EG": "Egypt", "AE": "UAE", "SA": "Saudi Arabia"}

# Rough FX to USD for spend normalisation
FX_TO_USD: dict[str, float] = {
    "EGP": 1 / 50.0,
    "AED": 1 / 3.67,
    "SAR": 1 / 3.75,
    "KWD": 1 / 0.31,
    "BHD": 1 / 0.38,
    "QAR": 1 / 3.64,
    "OMR": 1 / 0.38,
    "JOD": 1 / 0.71,
    "USD": 1.0,
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _safe_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# ── Data Structures ────────────────────────────────────────────────────────────

@dataclass
class PotentialFilm:
    """User-provided inputs for a film being evaluated."""
    title: str = ""
    genre: str = ""
    country_of_origin: str = ""
    expected_imdb_rating: float | None = None
    expected_elcinema_rating: float | None = None
    movie_meter_rank: int | None = None          # BOM Movie Meter (lower = more buzz)
    marketing_spend: dict[str, float] = field(default_factory=dict)   # {market_code: local_currency}
    spend_currencies: dict[str, str] = field(default_factory=dict)    # {market_code: currency_code}
    total_marketing_spend_usd: float = 0.0
    first_watch_target: dict[str, float] = field(default_factory=dict)  # {market_code: admissions}
    total_first_watch_target: float = 0.0


@dataclass
class HistoricalFilmProfile:
    """Lightweight analytical profile of a historical film."""
    film_id: str
    title: str
    release_year: int | None = None
    imdb_rating: float | None = None
    imdb_votes: int = 0
    elcinema_rating: float | None = None
    total_admissions: dict[str, float] = field(default_factory=dict)
    opening_admissions: dict[str, float] = field(default_factory=dict)
    market_shares: dict[str, float] = field(default_factory=dict)
    run_shapes: dict[str, str] = field(default_factory=dict)
    stability_scores: dict[str, float] = field(default_factory=dict)
    total_mena_admissions: float = 0.0
    historical_spend_usd: float = 0.0           # from MarketingInput if available
    historical_target: float = 0.0              # from OutcomeTarget if available


@dataclass
class ComparableFilm:
    """A historical film identified as comparable."""
    profile: HistoricalFilmProfile
    similarity_score: float = 0.0
    match_reasons: list[str] = field(default_factory=list)


# ── Step 1: Build Historical Profiles ──────────────────────────────────────────

def build_historical_profiles(session: Session) -> list[HistoricalFilmProfile]:
    """
    Build lightweight profiles for every historical film in the database.
    Uses build_market_run_profile from release_intelligence for consistency.
    """
    # Bulk queries
    films = {str(f.id): f for f in session.execute(select(Film)).scalars().all()}

    all_evidence = session.execute(select(ReconciledEvidence)).scalars().all()
    evidence_by_film: dict[str, list] = {}
    for ev in all_evidence:
        evidence_by_film.setdefault(str(ev.film_id), []).append(ev)

    all_ratings = session.execute(select(RatingsMetric)).scalars().all()
    ratings_by_film: dict[str, list] = {}
    for r in all_ratings:
        ratings_by_film.setdefault(str(r.film_id), []).append(r)

    all_spend = session.execute(select(MarketingInput)).scalars().all()
    spend_by_film: dict[str, list] = {}
    for s in all_spend:
        spend_by_film.setdefault(str(s.film_id), []).append(s)

    all_targets = session.execute(select(OutcomeTarget)).scalars().all()
    targets_by_film: dict[str, list] = {}
    for t in all_targets:
        targets_by_film.setdefault(str(t.film_id), []).append(t)

    profiles: list[HistoricalFilmProfile] = []

    for film_id_str, film in films.items():
        profile = HistoricalFilmProfile(
            film_id=film_id_str,
            title=film.canonical_title or "",
            release_year=film.release_year,
        )

        # ── Ratings ────────────────────────────────────────────────────────
        for r in ratings_by_film.get(film_id_str, []):
            src = (r.source_name or "").lower()
            rv = _safe_float(r.rating_value)
            vc = r.vote_count or 0
            if rv <= 0:
                continue
            if "imdb" in src and (profile.imdb_rating is None or vc > profile.imdb_votes):
                profile.imdb_rating = rv
                profile.imdb_votes = vc
            elif ("elcinema" in src or "el cinema" in src) and profile.elcinema_rating is None:
                profile.elcinema_rating = rv

        # ── Market data via build_market_run_profile ───────────────────────
        evidence = evidence_by_film.get(film_id_str, [])
        by_market: dict[str, list[dict]] = {}
        for ev in evidence:
            cc = ev.country_code or "??"
            by_market.setdefault(cc, []).append({
                "country_code": cc,
                "period_key": ev.period_key,
                "period_start_date": ev.period_start_date.isoformat() if ev.period_start_date else None,
                "period_end_date": ev.period_end_date.isoformat() if ev.period_end_date else None,
                "period_gross_local": float(ev.period_gross_local or 0),
                "currency": ev.currency,
                "granularity": ev.record_granularity,
                "semantics": ev.record_semantics,
                "admissions_actual": float(ev.admissions_actual) if ev.admissions_actual is not None else None,
                "admissions_estimated": float(ev.admissions_estimated) if ev.admissions_estimated is not None else None,
            })

        total_mena = 0.0
        for cc in CORE_MARKETS:
            rows = by_market.get(cc, [])
            if not rows:
                continue
            mrp = build_market_run_profile(film_id_str, cc, rows)
            profile.total_admissions[cc] = mrp.tracked_total_admissions
            profile.opening_admissions[cc] = mrp.opening_admissions
            profile.run_shapes[cc] = mrp.run_shape_label
            profile.stability_scores[cc] = mrp.stability
            total_mena += mrp.tracked_total_admissions

        profile.total_mena_admissions = total_mena
        if total_mena > 0:
            for cc in CORE_MARKETS:
                profile.market_shares[cc] = profile.total_admissions.get(cc, 0) / total_mena

        # ── Historical spend (convert to USD) ──────────────────────────────
        for s in spend_by_film.get(film_id_str, []):
            local = _safe_float(s.spend_local)
            ccy = (s.spend_currency or "USD").upper()
            profile.historical_spend_usd += local * FX_TO_USD.get(ccy, 1 / 50.0)

        # ── Historical target ──────────────────────────────────────────────
        for t in targets_by_film.get(film_id_str, []):
            if (t.target_label or "") == "first_watch_target":
                profile.historical_target += _safe_float(t.target_value)

        profiles.append(profile)

    return profiles


# ── Step 2: Comparable Film Search ─────────────────────────────────────────────

def _performance_tier(total_mena: float) -> int:
    """Map total MENA admissions to a 1-5 tier."""
    if total_mena >= 500_000:
        return 5
    if total_mena >= 250_000:
        return 4
    if total_mena >= 100_000:
        return 3
    if total_mena >= 30_000:
        return 2
    return 1


def _estimated_tier(potential: PotentialFilm) -> int:
    """Estimate 1-5 performance tier from available signals."""
    signals: list[int] = []
    if potential.expected_imdb_rating:
        if potential.expected_imdb_rating >= 7.5:
            signals.append(5)
        elif potential.expected_imdb_rating >= 7.0:
            signals.append(4)
        elif potential.expected_imdb_rating >= 6.5:
            signals.append(3)
        elif potential.expected_imdb_rating >= 5.5:
            signals.append(2)
        else:
            signals.append(1)
    if potential.movie_meter_rank and potential.movie_meter_rank > 0:
        if potential.movie_meter_rank <= 50:
            signals.append(5)
        elif potential.movie_meter_rank <= 200:
            signals.append(4)
        elif potential.movie_meter_rank <= 500:
            signals.append(3)
        elif potential.movie_meter_rank <= 1000:
            signals.append(2)
        else:
            signals.append(1)
    return round(statistics.mean(signals)) if signals else 3


def _compute_similarity(
    potential: PotentialFilm,
    historical: HistoricalFilmProfile,
) -> tuple[float, list[str]]:
    """Score how comparable a historical film is to the potential film (0-1)."""
    dims: list[tuple[str, float, float]] = []   # (name, weight, score)
    reasons: list[str] = []

    # ── Ratings similarity (weight 0.40) ──────────────────────────────────
    rating_sims: list[float] = []
    if potential.expected_imdb_rating and historical.imdb_rating:
        diff = abs(potential.expected_imdb_rating - historical.imdb_rating)
        sim = _clamp(1.0 - diff / 3.0)
        rating_sims.append(sim)
        if sim > 0.70:
            reasons.append(f"Similar IMDb ({historical.imdb_rating:.1f})")
    if potential.expected_elcinema_rating and historical.elcinema_rating:
        diff = abs(potential.expected_elcinema_rating - historical.elcinema_rating)
        rating_sims.append(_clamp(1.0 - diff / 3.0))
    if rating_sims:
        dims.append(("ratings", 0.40, statistics.mean(rating_sims)))

    # ── Market balance similarity (weight 0.30) ──────────────────────────
    total_target = sum(potential.first_watch_target.values()) or 1.0
    pot_shares = {cc: potential.first_watch_target.get(cc, 0) / total_target for cc in CORE_MARKETS}
    if not any(pot_shares.values()):
        pot_shares = {"EG": 0.45, "AE": 0.30, "SA": 0.25}

    hist_shares = historical.market_shares
    if hist_shares:
        dot = sum(pot_shares.get(cc, 0) * hist_shares.get(cc, 0) for cc in CORE_MARKETS)
        mag_p = math.sqrt(sum(v ** 2 for v in pot_shares.values()) or 1e-9)
        mag_h = math.sqrt(sum(v ** 2 for v in hist_shares.values()) or 1e-9)
        cos_sim = _clamp(dot / (mag_p * mag_h))
        dims.append(("market_balance", 0.30, cos_sim))
        if cos_sim > 0.85:
            dominant = max(hist_shares, key=hist_shares.get)
            reasons.append(f"Similar market mix ({MARKET_NAMES.get(dominant, dominant)}-led)")

    # ── Performance tier similarity (weight 0.30) ────────────────────────
    if historical.total_mena_admissions > 0:
        est = _estimated_tier(potential)
        hist = _performance_tier(historical.total_mena_admissions)
        tier_sim = _clamp(1.0 - abs(est - hist) / 3.0)
        dims.append(("performance_tier", 0.30, tier_sim))
        if tier_sim > 0.80:
            reasons.append(f"Similar performance tier ({historical.total_mena_admissions:,.0f} MENA admissions)")

    if not dims:
        return 0.0, []

    total_w = sum(w for _, w, _ in dims)
    score = sum(w * s for _, w, s in dims) / total_w if total_w > 0 else 0.0
    return round(score, 3), reasons


def find_comparable_films(
    potential: PotentialFilm,
    profiles: list[HistoricalFilmProfile],
    top_n: int = 5,
) -> list[ComparableFilm]:
    """Find the top-N most comparable historical films."""
    candidates: list[ComparableFilm] = []
    for p in profiles:
        if p.total_mena_admissions <= 0:
            continue
        score, reasons = _compute_similarity(potential, p)
        if score > 0.10:
            candidates.append(ComparableFilm(profile=p, similarity_score=score, match_reasons=reasons))

    candidates.sort(key=lambda c: c.similarity_score, reverse=True)
    return candidates[:top_n]


# ── Step 3: Demand Potential ───────────────────────────────────────────────────

def compute_demand_potential(
    potential: PotentialFilm,
    comparables: list[ComparableFilm],
    all_profiles: list[HistoricalFilmProfile],
) -> float:
    """
    0-1 score representing natural audience demand independent of marketing.
    Blends ratings signal, buzz signal, and comparable film performance.
    """
    components: list[tuple[str, float, float]] = []

    # 1. Ratings signal (weight 0.30)
    r_scores: list[float] = []
    if potential.expected_imdb_rating:
        r_scores.append(_clamp((potential.expected_imdb_rating - 4.0) / 5.0))
    if potential.expected_elcinema_rating:
        r_scores.append(_clamp((potential.expected_elcinema_rating - 4.0) / 5.0))
    if r_scores:
        components.append(("ratings", 0.30, statistics.mean(r_scores)))

    # 2. Buzz signal (weight 0.15)
    if potential.movie_meter_rank and potential.movie_meter_rank > 0:
        buzz = _clamp(1.0 - potential.movie_meter_rank / 1000.0)
        components.append(("buzz", 0.15, buzz))

    # 3. Comparable performance (weight 0.40)
    if comparables:
        all_mena = sorted(p.total_mena_admissions for p in all_profiles if p.total_mena_admissions > 0)
        if all_mena:
            comp_scores: list[tuple[float, float]] = []
            for comp in comparables:
                below = sum(1 for a in all_mena if a <= comp.profile.total_mena_admissions)
                pct = below / len(all_mena)
                comp_scores.append((pct, comp.similarity_score))
            total_sw = sum(w for _, w in comp_scores)
            if total_sw > 0:
                perf = sum(s * w for s, w in comp_scores) / total_sw
                components.append(("comparable_perf", 0.40, _clamp(perf)))

    # 4. Ratings consistency with comparables (weight 0.15)
    if comparables and potential.expected_imdb_rating:
        comp_ratings = [c.profile.imdb_rating for c in comparables if c.profile.imdb_rating]
        if comp_ratings:
            avg_r = statistics.mean(comp_ratings)
            consistency = 1.0 - abs(avg_r - potential.expected_imdb_rating) / 5.0
            components.append(("rating_consistency", 0.15, _clamp(consistency)))

    if not components:
        return 0.0
    total_w = sum(w for _, w, _ in components)
    return round(_clamp(sum(w * s for _, w, s in components) / total_w), 3)


# ── Step 4: Marketing Dependency ───────────────────────────────────────────────

def compute_marketing_dependency(comparables: list[ComparableFilm]) -> float:
    """
    0-1 score. High = comparable films are marketing-driven (front-loaded).
    Low = comparable films succeed on word-of-mouth.
    """
    indicators: list[tuple[float, float]] = []
    for comp in comparables:
        w = comp.similarity_score
        for shape in comp.profile.run_shapes.values():
            if "front_loaded" in shape:
                indicators.append((0.85, w))
            elif "anomalous" in shape or "irregular" in shape:
                indicators.append((0.65, w))
            elif "standard_decay" in shape:
                indicators.append((0.50, w))
            elif "clean_build_then_decay" in shape or "resilient" in shape:
                indicators.append((0.25, w))
            elif "build_then_decay" in shape:
                indicators.append((0.30, w))
            else:
                indicators.append((0.50, w))

    if not indicators:
        return 0.50
    total_w = sum(w for _, w in indicators)
    return round(_clamp(sum(s * w for s, w in indicators) / total_w), 3) if total_w > 0 else 0.50


# ── Step 5: Forecast ──────────────────────────────────────────────────────────

def _rating_adjustment(potential: PotentialFilm, comparables: list[ComparableFilm]) -> float:
    """
    If the potential film's expected rating is better/worse than the average
    comparable, nudge the forecast proportionally.  Returns a multiplier.
    """
    if not potential.expected_imdb_rating or not comparables:
        return 1.0
    comp_ratings = [c.profile.imdb_rating for c in comparables if c.profile.imdb_rating]
    if not comp_ratings:
        return 1.0
    avg = statistics.mean(comp_ratings)
    diff = potential.expected_imdb_rating - avg
    # ±1 rating point = ±15% adjustment
    return _clamp(1.0 + diff * 0.15, 0.50, 1.80)


def compute_forecast(
    comparables: list[ComparableFilm],
    potential: PotentialFilm,
) -> dict[str, dict[str, int]]:
    """Generate floor / base / stretch admissions per market from comparables."""
    rating_factor = _rating_adjustment(potential, comparables)
    forecasts: dict[str, dict[str, int]] = {}
    mena_f, mena_b, mena_s = 0.0, 0.0, 0.0

    for cc in CORE_MARKETS:
        comp_data: list[tuple[float, float]] = []   # (admissions, sim_weight)
        for comp in comparables:
            adm = comp.profile.total_admissions.get(cc, 0)
            if adm > 0:
                comp_data.append((adm, comp.similarity_score))

        if not comp_data:
            forecasts[cc] = {"floor": 0, "base": 0, "stretch": 0}
            continue

        total_w = sum(w for _, w in comp_data)
        base = sum(a * w for a, w in comp_data) / total_w * rating_factor

        # Uncertainty: wider band when fewer/weaker comparables
        n = len(comp_data)
        avg_sim = total_w / n if n > 0 else 0
        uncertainty = _clamp(0.12 + (1.0 - avg_sim) * 0.25 + max(0, (3 - n)) * 0.08)

        floor = base * (1.0 - uncertainty)
        stretch = base * (1.0 + uncertainty)

        forecasts[cc] = {
            "floor": round(max(floor, 0)),
            "base": round(max(base, 0)),
            "stretch": round(max(stretch, 0)),
        }
        mena_f += max(floor, 0)
        mena_b += max(base, 0)
        mena_s += max(stretch, 0)

    forecasts["MENA_TOTAL"] = {
        "floor": round(mena_f),
        "base": round(mena_b),
        "stretch": round(mena_s),
    }
    return forecasts


# ── Step 6: Target Realism ────────────────────────────────────────────────────

def assess_target_realism(
    forecast: dict[str, dict[str, int]],
    potential: PotentialFilm,
) -> str:
    """Compare the user's first-watch target against the forecast."""
    target = potential.total_first_watch_target
    if not target:
        target = sum(potential.first_watch_target.values())
    if target <= 0:
        return "Not Specified"

    mena = forecast.get("MENA_TOTAL", {})
    floor = mena.get("floor", 0)
    base = mena.get("base", 0)
    stretch = mena.get("stretch", 0)

    if base <= 0:
        return "Insufficient Forecast Data"

    if target <= floor:
        return "Conservative"
    if target <= base:
        return "Realistic"
    if target <= stretch:
        return "Ambitious"
    return "Unrealistic"


# ── Step 7: Spend Adequacy ────────────────────────────────────────────────────

def assess_spend_adequacy(
    potential: PotentialFilm,
    forecast: dict[str, dict[str, int]],
    marketing_dependency: float,
) -> str:
    """Classify whether planned marketing spend is appropriate."""
    # Compute total spend in USD
    total_usd = potential.total_marketing_spend_usd
    if total_usd <= 0:
        # Try to compute from per-market spend
        for cc, local_amt in potential.marketing_spend.items():
            ccy = potential.spend_currencies.get(cc, "USD").upper()
            total_usd += local_amt * FX_TO_USD.get(ccy, 1 / 50.0)

    if total_usd <= 0:
        return "Not Specified"

    mena_base = forecast.get("MENA_TOTAL", {}).get("base", 0)
    if mena_base <= 0:
        return "Insufficient Forecast Data"

    # Cost per first-watch benchmark
    # Marketing-driven films need ~$0.80-1.20 per FW; WOM films ~$0.30-0.50
    cost_per_fw = total_usd / mena_base
    benchmark = 0.35 + marketing_dependency * 0.65   # $0.35 – $1.00

    ratio = cost_per_fw / benchmark if benchmark > 0 else 1.0

    if ratio < 0.45:
        return "Under-supported"
    if ratio < 0.80:
        return "Lean but Adequate"
    if ratio <= 1.50:
        return "Efficient"
    if ratio <= 2.50:
        return "High Spend but Justified"
    return "Over-invested"


# ── Step 8: Risk Assessment ───────────────────────────────────────────────────

def assess_risk(
    potential: PotentialFilm,
    comparables: list[ComparableFilm],
    demand_potential: float,
    target_realism: str,
    marketing_dependency: float,
    confidence_score: float,
) -> tuple[str, float]:
    """Multi-factor risk assessment. Returns (label, score 0-1)."""
    factors: list[float] = []

    # 1. Ratings vs target mismatch
    if potential.expected_imdb_rating and potential.total_first_watch_target:
        if potential.expected_imdb_rating < 6.0 and potential.total_first_watch_target > 200_000:
            factors.append(0.80)
        elif potential.expected_imdb_rating < 6.5 and potential.total_first_watch_target > 400_000:
            factors.append(0.70)
        elif potential.expected_imdb_rating >= 7.0:
            factors.append(0.20)
        else:
            factors.append(0.40)

    # 2. Comparable strength
    if len(comparables) < 2:
        factors.append(0.80)
    elif len(comparables) < 4:
        avg_sim = statistics.mean(c.similarity_score for c in comparables)
        factors.append(0.65 if avg_sim < 0.40 else 0.40)
    else:
        factors.append(0.25)

    # 3. Target realism
    target_risk = {
        "Conservative": 0.10, "Realistic": 0.25, "Ambitious": 0.55,
        "Unrealistic": 0.90, "Not Specified": 0.45, "Insufficient Forecast Data": 0.60,
    }
    factors.append(target_risk.get(target_realism, 0.50))

    # 4. Market concentration risk
    total_t = sum(potential.first_watch_target.values()) or potential.total_first_watch_target or 0
    if total_t > 0 and potential.first_watch_target:
        max_share = max(potential.first_watch_target.values()) / total_t
        factors.append(0.55 if max_share > 0.70 else 0.25)

    # 5. Volatile comparable patterns
    volatile_count = sum(
        1 for c in comparables
        for shape in c.profile.run_shapes.values()
        if any(tag in shape for tag in ("irregular", "anomalous", "volatile", "sparse"))
    )
    if volatile_count > 0:
        factors.append(min(0.80, 0.30 + volatile_count * 0.12))

    # 6. Inverse demand potential
    factors.append(_clamp(1.0 - demand_potential))

    # 7. Low confidence
    factors.append(_clamp(1.0 - confidence_score))

    risk_score = statistics.mean(factors) if factors else 0.50

    if risk_score >= 0.70:
        label = "Very High Risk"
    elif risk_score >= 0.50:
        label = "High Risk"
    elif risk_score >= 0.35:
        label = "Moderate Risk"
    else:
        label = "Low Risk"

    return label, round(risk_score, 3)


# ── Step 9: Confidence ────────────────────────────────────────────────────────

def compute_confidence(
    potential: PotentialFilm,
    comparables: list[ComparableFilm],
    all_profiles: list[HistoricalFilmProfile],
) -> tuple[float, str]:
    """How reliable is this analysis? Returns (score, label)."""
    factors: list[float] = []

    # Comparable count
    n = len(comparables)
    factors.append(1.0 if n >= 5 else 0.7 if n >= 3 else 0.4 if n >= 1 else 0.1)

    # Average similarity
    if comparables:
        factors.append(statistics.mean(c.similarity_score for c in comparables))

    # Input completeness
    completeness = 0.0
    if potential.expected_imdb_rating:
        completeness += 0.30
    if potential.expected_elcinema_rating:
        completeness += 0.15
    if potential.movie_meter_rank:
        completeness += 0.15
    if potential.total_marketing_spend_usd > 0 or potential.marketing_spend:
        completeness += 0.15
    if potential.total_first_watch_target > 0 or potential.first_watch_target:
        completeness += 0.15
    if potential.genre:
        completeness += 0.10
    factors.append(completeness)

    # Historical database depth
    n_films = sum(1 for p in all_profiles if p.total_mena_admissions > 0)
    factors.append(0.90 if n_films >= 20 else 0.70 if n_films >= 10 else 0.45 if n_films >= 5 else 0.20)

    score = _clamp(statistics.mean(factors)) if factors else 0.0

    if score >= 0.70:
        label = "High"
    elif score >= 0.45:
        label = "Medium"
    else:
        label = "Low"

    return round(score, 3), label


# ── Step 10: Final Decision ───────────────────────────────────────────────────

def compute_final_decision(
    demand_potential: float,
    risk_score: float,
    target_realism: str,
    spend_adequacy: str,
    confidence_score: float,
) -> tuple[float, str]:
    """Weighted composite decision score + label."""
    target_s = {
        "Conservative": 0.90, "Realistic": 0.80, "Ambitious": 0.50,
        "Unrealistic": 0.15, "Not Specified": 0.45, "Insufficient Forecast Data": 0.35,
    }.get(target_realism, 0.40)

    spend_s = {
        "Under-supported": 0.30, "Lean but Adequate": 0.60, "Efficient": 0.85,
        "High Spend but Justified": 0.70, "Over-invested": 0.40,
        "Not Specified": 0.50, "Insufficient Forecast Data": 0.35,
    }.get(spend_adequacy, 0.50)

    score = _clamp(
        demand_potential * 0.30
        + (1.0 - risk_score) * 0.25
        + target_s * 0.20
        + spend_s * 0.10
        + confidence_score * 0.15
    )

    if score >= 0.75:
        label = "Strong Acquisition Case"
    elif score >= 0.60:
        label = "Good but Selective"
    elif score >= 0.45:
        label = "Spend-Sensitive Opportunity"
    elif score >= 0.30:
        label = "Risky Project"
    else:
        label = "Weak Commercial Case"

    return round(score, 3), label


# ── Step 11: Analyst Summary ──────────────────────────────────────────────────

def generate_analyst_summary(
    potential: PotentialFilm,
    comparables: list[ComparableFilm],
    demand_potential: float,
    marketing_dependency: float,
    forecast: dict[str, dict[str, int]],
    target_realism: str,
    spend_adequacy: str,
    risk_label: str,
    final_label: str,
    confidence_label: str,
) -> str:
    """Generate a concise analyst-style summary."""
    parts: list[str] = []

    # Overall assessment
    label_text = {
        "Strong Acquisition Case": f'"{potential.title}" presents a strong commercial opportunity in MENA.',
        "Good but Selective": f'"{potential.title}" shows selective commercial promise — execution-dependent.',
        "Spend-Sensitive Opportunity": f'"{potential.title}" can work commercially but is highly dependent on marketing investment.',
        "Risky Project": f'"{potential.title}" carries significant commercial risk based on available comparables.',
        "Weak Commercial Case": f'"{potential.title}" shows limited commercial viability based on current analysis.',
    }
    parts.append(label_text.get(final_label, f'"{potential.title}" — assessment pending.'))

    # Comparable context
    if comparables:
        top = comparables[0]
        reason_str = top.match_reasons[0] if top.match_reasons else "overall profile"
        parts.append(f"Most comparable film: {top.profile.title} ({reason_str}).")

    # Forecast base
    mena_base = forecast.get("MENA_TOTAL", {}).get("base", 0)
    if mena_base > 0:
        parts.append(f"MENA forecast base: {mena_base:,} first watches.")

    # Demand narrative
    if demand_potential >= 0.65:
        parts.append("Natural audience demand is strong.")
    elif demand_potential >= 0.40:
        parts.append("Audience demand is moderate — marketing support will be important.")
    else:
        parts.append("Natural demand indicators are weak — heavy marketing needed.")

    # Marketing dependency
    if marketing_dependency >= 0.65:
        parts.append("Comparable films were primarily marketing-driven — expect front-loaded performance.")
    elif marketing_dependency <= 0.35:
        parts.append("Comparables suggest word-of-mouth potential — the run should build after opening.")

    # Target realism
    if target_realism == "Unrealistic":
        parts.append("Warning: the first-watch target exceeds what comparable films have achieved.")
    elif target_realism == "Ambitious":
        parts.append("The target is ambitious — requires outperforming most comparable films.")
    elif target_realism == "Conservative":
        parts.append("The target is conservative and highly achievable based on comparables.")

    # Spend
    if spend_adequacy == "Under-supported":
        parts.append("Marketing spend appears insufficient relative to the target.")
    elif spend_adequacy == "Over-invested":
        parts.append("Marketing spend is high relative to expected returns — ROI risk.")

    # Risk
    if risk_label in ("High Risk", "Very High Risk"):
        parts.append(f"Risk: {risk_label}. Proceed with caution.")

    # Confidence
    if confidence_label == "Low":
        parts.append("Confidence is low due to limited data — treat projections as directional only.")

    return " ".join(parts)


# ── Main Orchestrator ─────────────────────────────────────────────────────────

def evaluate_potential_film(
    session: Session,
    potential: PotentialFilm,
) -> dict[str, Any]:
    """
    Main entry point.  Evaluates a potential new film and returns the full
    decision payload (safe to serialise as JSON).
    """
    try:
        return _evaluate_inner(session, potential)
    except Exception:   # noqa: BLE001
        # Never crash the UI
        return {
            "error": "Evaluation failed — check inputs and historical data.",
            "demand_potential_score": 0.0,
            "final_decision_score": 0.0,
            "final_decision_label": "Insufficient Data",
        }


def _evaluate_inner(session: Session, potential: PotentialFilm) -> dict[str, Any]:
    # Step 1
    all_profiles = build_historical_profiles(session)

    # Step 2
    comparables = find_comparable_films(potential, all_profiles, top_n=5)

    # Step 3
    demand_potential = compute_demand_potential(potential, comparables, all_profiles)

    # Step 4
    marketing_dependency = compute_marketing_dependency(comparables)

    # Step 5
    forecast = compute_forecast(comparables, potential)

    # Step 6
    target_realism = assess_target_realism(forecast, potential)

    # Step 9 (confidence first — needed by risk)
    confidence_score, confidence_label = compute_confidence(potential, comparables, all_profiles)

    # Step 7
    spend_adequacy = assess_spend_adequacy(potential, forecast, marketing_dependency)

    # Step 8
    risk_label, risk_score = assess_risk(
        potential, comparables, demand_potential,
        target_realism, marketing_dependency, confidence_score,
    )

    # Step 10
    final_score, final_label = compute_final_decision(
        demand_potential, risk_score, target_realism, spend_adequacy, confidence_score,
    )

    # Step 11
    summary = generate_analyst_summary(
        potential, comparables, demand_potential, marketing_dependency,
        forecast, target_realism, spend_adequacy, risk_label, final_label, confidence_label,
    )

    # Assemble output
    return {
        "demand_potential_score": demand_potential,
        "marketing_dependency_score": marketing_dependency,
        "comparable_films": [
            {
                "title": c.profile.title,
                "release_year": c.profile.release_year,
                "similarity": round(c.similarity_score, 3),
                "reason": "; ".join(c.match_reasons) if c.match_reasons else "overall profile",
                "imdb_rating": c.profile.imdb_rating,
                "total_mena_admissions": round(c.profile.total_mena_admissions),
                "market_shares": {k: round(v, 3) for k, v in c.profile.market_shares.items()},
                "run_shapes": c.profile.run_shapes,
            }
            for c in comparables
        ],
        "forecast": forecast,
        "target_realism": target_realism,
        "spend_adequacy": spend_adequacy,
        "risk_level": risk_label,
        "risk_score": risk_score,
        "confidence_score": confidence_score,
        "confidence_label": confidence_label,
        "final_decision_score": final_score,
        "final_decision_label": final_label,
        "analyst_summary": summary,
        "_meta": {
            "historical_films_with_data": sum(1 for p in all_profiles if p.total_mena_admissions > 0),
            "comparables_found": len(comparables),
            "input_completeness": _input_completeness(potential),
        },
    }


def _input_completeness(p: PotentialFilm) -> float:
    """How complete are the user inputs? (0-1)"""
    score = 0.0
    if p.expected_imdb_rating:
        score += 0.25
    if p.expected_elcinema_rating:
        score += 0.15
    if p.movie_meter_rank:
        score += 0.15
    if p.total_marketing_spend_usd > 0 or p.marketing_spend:
        score += 0.15
    if p.total_first_watch_target > 0 or p.first_watch_target:
        score += 0.15
    if p.genre:
        score += 0.10
    if p.title:
        score += 0.05
    return round(_clamp(score), 2)
