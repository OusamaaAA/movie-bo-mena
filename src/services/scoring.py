from dataclasses import dataclass
from decimal import Decimal

from src.models import MarketingInput, OutcomeTarget, RatingsMetric, ReconciledEvidence

DEFAULT_WEIGHTS = {
    "historical_performance": 0.24,
    "market_breadth": 0.12,
    "audience_pull": 0.16,
    "ratings_sentiment": 0.14,
    "popularity_interest": 0.14,
    "marketing_context": 0.10,
    "target_alignment": 0.10,
}


@dataclass
class ScoreBreakdown:
    overall: float
    components: dict[str, float]
    explanation: str


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def calculate_marketing_score(
    reconciled: list[ReconciledEvidence],
    ratings: list[RatingsMetric],
    marketing: list[MarketingInput],
    targets: list[OutcomeTarget],
    weights: dict[str, float] | None = None,
) -> ScoreBreakdown:
    w = weights or DEFAULT_WEIGHTS

    title_weekly = [
        r for r in reconciled if r.record_scope == "title" and r.record_semantics == "title_period_gross"
    ]
    cumulative = [
        r for r in reconciled if r.record_scope == "title" and r.record_semantics == "title_cumulative_total"
    ]
    gross_values = [float(r.period_gross_local or 0) for r in title_weekly if r.period_gross_local is not None]
    markets = {r.country_code for r in title_weekly if r.country_code}
    avg_rating = (
        sum(float(r.rating_value or 0) for r in ratings if r.rating_value) / max(1, len([r for r in ratings if r.rating_value]))
    )
    avg_pop_rank = (
        sum(float(r.popularity_rank or 0) for r in ratings if r.popularity_rank) / max(1, len([r for r in ratings if r.popularity_rank]))
    )
    spend = sum(float(m.spend_local or 0) for m in marketing if m.spend_local)
    target = sum(float(t.target_value or 0) for t in targets if t.target_value)

    historical_performance = _clamp((sum(gross_values) / 1_000_000.0) * 15)
    market_breadth = _clamp(len(markets) * 18)
    cumulative_has_data = any((r.cumulative_gross_local is not None) for r in cumulative)
    audience_pull = _clamp(
        (sum(gross_values[:5]) / 500_000.0) * 22 if gross_values else (55 if cumulative_has_data else 20)
    )
    ratings_sentiment = _clamp((avg_rating / 10.0) * 100)
    popularity_interest = _clamp(100 - (avg_pop_rank / 200.0) * 100) if avg_pop_rank else 45
    marketing_context = _clamp((spend / 500_000.0) * 100 if spend else 40)
    target_alignment = _clamp((sum(gross_values) / target) * 100) if target > 0 else 50

    components = {
        "historical_performance": historical_performance,
        "market_breadth": market_breadth,
        "audience_pull": audience_pull,
        "ratings_sentiment": ratings_sentiment,
        "popularity_interest": popularity_interest,
        "marketing_context": marketing_context,
        "target_alignment": target_alignment,
    }
    overall = sum(components[key] * w.get(key, 0.0) for key in components)
    explanation = (
        f"Score {overall:.1f}/100 is driven by historical gross strength ({historical_performance:.1f}), "
        f"market footprint ({market_breadth:.1f}), and ratings/popularity ({ratings_sentiment:.1f}/{popularity_interest:.1f}). "
        "Marketing spend and target alignment adjust confidence in acquisition upside."
    )
    return ScoreBreakdown(overall=overall, components=components, explanation=explanation)

