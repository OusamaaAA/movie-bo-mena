from datetime import date
import re

from rapidfuzz import fuzz
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from src.models import (
    MarketingInput,
    NormalizedEvidence,
    OutcomeTarget,
    RawEvidence,
    RatingsMetric,
    ReconciledEvidence,
    ReviewQueue,
)
from src.services.text_utils import normalize_title_cross_language
from src.sources.boxofficemojo.parser import coerce_bom_release_weekend_url, normalize_bom_title_url


def _elcinema_work_id_from_raw(source_url: str | None, source_entity_id: str | None) -> str | None:
    for blob in (source_url or "", source_entity_id or ""):
        m = re.search(r"elcinema\.com/(?:en/)?work/(\d+)", blob, flags=re.I)
        if m:
            return m.group(1)
    se = (source_entity_id or "").strip()
    if re.fullmatch(r"\d{4,12}", se):
        return se
    if "_" in se:
        head = se.split("_", 1)[0]
        if re.fullmatch(r"\d{4,12}", head):
            return head
    return None


class ReportRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def source_coverage(self, film_id: str) -> list[tuple[str, int]]:
        stmt = (
            select(NormalizedEvidence.source_name, func.count(NormalizedEvidence.id))
            .where(NormalizedEvidence.film_id == film_id)
            .group_by(NormalizedEvidence.source_name)
            .order_by(func.count(NormalizedEvidence.id).desc())
        )
        return list(self.session.execute(stmt).all())

    def raw_by_source(self, film_id: str, source_name: str, limit: int = 100) -> list[RawEvidence]:
        stmt = (
            select(RawEvidence)
            .join(NormalizedEvidence, NormalizedEvidence.raw_evidence_id == RawEvidence.id)
            .where(NormalizedEvidence.film_id == film_id, RawEvidence.source_name == source_name)
            .order_by(RawEvidence.created_at.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def market_signals(self, film_id: str, limit: int = 200) -> list[RawEvidence]:
        stmt = (
            select(RawEvidence)
            .join(NormalizedEvidence, NormalizedEvidence.raw_evidence_id == RawEvidence.id)
            .where(
                NormalizedEvidence.film_id == film_id,
                RawEvidence.record_scope != "title",
            )
            .order_by(RawEvidence.created_at.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def market_signals_by_title_hints(self, title_hints: list[str], limit: int = 200) -> list[RawEvidence]:
        hints = [h.strip() for h in title_hints if h and h.strip()]
        if not hints:
            return []
        predicates = [RawEvidence.film_title_raw.ilike(f"%{hint}%") for hint in hints]
        stmt = (
            select(RawEvidence)
            .where(
                RawEvidence.record_scope != "title",
                or_(*predicates),
            )
            .order_by(RawEvidence.created_at.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def reconciled(self, film_id: str, limit: int = 200) -> list[ReconciledEvidence]:
        stmt = (
            select(ReconciledEvidence)
            .where(ReconciledEvidence.film_id == film_id)
            .order_by(ReconciledEvidence.period_start_date.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def latest_ratings(self, film_id: str) -> list[RatingsMetric]:
        max_date = self.session.execute(
            select(func.max(RatingsMetric.metric_date)).where(RatingsMetric.film_id == film_id)
        ).scalar_one_or_none()
        if not max_date:
            return []
        stmt = select(RatingsMetric).where(RatingsMetric.film_id == film_id, RatingsMetric.metric_date == max_date)
        return list(self.session.execute(stmt).scalars().all())

    def marketing_inputs(self, film_id: str) -> list[MarketingInput]:
        return list(self.session.execute(select(MarketingInput).where(MarketingInput.film_id == film_id)).scalars().all())

    def outcome_targets(self, film_id: str) -> list[OutcomeTarget]:
        return list(self.session.execute(select(OutcomeTarget).where(OutcomeTarget.film_id == film_id)).scalars().all())

    def open_review_items(self, limit: int = 200) -> list[ReviewQueue]:
        stmt = (
            select(ReviewQueue)
            .where(ReviewQueue.status == "open")
            .order_by(ReviewQueue.created_at.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def source_explorer(self, source_name: str, start_date: date | None, end_date: date | None, limit: int = 300) -> list[RawEvidence]:
        stmt = select(RawEvidence).where(RawEvidence.source_name == source_name)
        if start_date:
            stmt = stmt.where(RawEvidence.created_at >= start_date)
        if end_date:
            stmt = stmt.where(RawEvidence.created_at <= end_date)
        stmt = stmt.order_by(RawEvidence.created_at.desc()).limit(limit)
        return list(self.session.execute(stmt).scalars().all())

    def raw_evidence_for_film(self, film_id: str, limit: int = 800) -> list[RawEvidence]:
        stmt = (
            select(RawEvidence)
            .join(NormalizedEvidence, NormalizedEvidence.raw_evidence_id == RawEvidence.id)
            .where(NormalizedEvidence.film_id == film_id)
            .order_by(RawEvidence.created_at.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def stored_acquisition_source_ids(self, film_id: str) -> dict[str, list[str]]:
        """
        IDs and canonical source pages already linked to a film (for acquisition refresh).
        elCinema work IDs feed /en/work/{id}/boxoffice; BOM release URLs feed /release/rl…/weekend/.
        """
        work_ids: list[str] = []
        release_urls: list[str] = []
        title_urls: list[str] = []
        seen_w: set[str] = set()
        seen_r: set[str] = set()
        seen_t: set[str] = set()

        for raw in self.raw_evidence_for_film(film_id):
            w = _elcinema_work_id_from_raw(raw.source_url, raw.source_entity_id)
            if w and w not in seen_w:
                seen_w.add(w)
                work_ids.append(w)

            ru = coerce_bom_release_weekend_url(raw.source_url) or coerce_bom_release_weekend_url(
                raw.source_entity_id,
            )
            if ru and ru not in seen_r:
                seen_r.add(ru)
                release_urls.append(ru)

            tu = normalize_bom_title_url(raw.source_url) or normalize_bom_title_url(raw.source_entity_id)
            if tu and tu not in seen_t:
                seen_t.add(tu)
                title_urls.append(tu)

        return {
            "elcinema_work_ids": work_ids,
            "bom_release_urls": release_urls,
            "bom_title_urls": title_urls,
        }

    def egypt_avg_ticket_price(self) -> float | None:
        """
        Derive a market-wide average Egypt ticket price from ALL Filmyard EG records
        that have both gross and admissions.  Returns the median gross÷admissions ratio.
        """
        rows = self.session.execute(
            select(RawEvidence.period_gross_local, RawEvidence.admissions_actual)
            .where(
                RawEvidence.source_name == "Filmyard",
                RawEvidence.country_code == "EG",
                RawEvidence.period_gross_local > 0,
                RawEvidence.admissions_actual.isnot(None),
                RawEvidence.admissions_actual > 0,
            )
        ).all()
        if not rows:
            return None
        prices = sorted(float(gross) / float(adm) for gross, adm in rows if float(adm) > 0)
        if not prices:
            return None
        n = len(prices)
        mid = n // 2
        return prices[mid] if n % 2 == 1 else (prices[mid - 1] + prices[mid]) / 2

    def title_evidence_matches(
        self,
        query: str,
        *,
        limit: int = 10,
        sample_size: int = 2000,
        release_year_hint: int | None = None,
    ) -> list[dict]:
        """
        User-facing acquisition lookup helper:
        find recent title-level evidence that looks like `query` across Arabic/English variants.
        """
        if not query or not query.strip():
            return []
        q_cross = normalize_title_cross_language(query)

        rows = self.session.execute(
            select(RawEvidence, NormalizedEvidence.film_id)
            .outerjoin(NormalizedEvidence, NormalizedEvidence.raw_evidence_id == RawEvidence.id)
            .where(RawEvidence.record_scope == "title")
            .order_by(RawEvidence.created_at.desc())
            .limit(sample_size)
        ).all()

        scored: list[tuple[float, RawEvidence, str | None]] = []
        for raw, film_id in rows:
            cand_cross = normalize_title_cross_language(raw.film_title_raw or "")
            score = fuzz.ratio(q_cross, cand_cross) / 100.0
            if release_year_hint and raw.release_year_hint:
                score += 0.06 if release_year_hint == raw.release_year_hint else -0.05
            score = max(0.0, min(1.0, score))
            scored.append((score, raw, film_id))

        scored.sort(key=lambda x: x[0], reverse=True)
        out = []
        for score, raw, film_id in scored[:limit]:
            out.append(
                {
                    "match_score": score,
                    "film_id": film_id,
                    "raw_evidence_id": raw.id,
                    "title": raw.film_title_raw,
                    "release_year_hint": raw.release_year_hint,
                    "source_name": raw.source_name,
                    "country_code": raw.country_code,
                    "period_key": raw.period_key,
                    "period_granularity": raw.record_granularity,
                    "period_semantics": raw.record_semantics,
                    "admissions_actual": float(raw.admissions_actual) if raw.admissions_actual is not None else None,
                    "period_gross_local": float(raw.period_gross_local or 0),
                    "currency": raw.currency,
                    "freshness_note": raw.notes,
                }
            )
        return out

