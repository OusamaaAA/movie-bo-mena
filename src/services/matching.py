from __future__ import annotations

import hashlib
from dataclasses import dataclass

from rapidfuzz import fuzz
from sqlalchemy import String, cast, select
from sqlalchemy.orm import Session

from src.models import Film, FilmAlias, ReviewQueue
from src.services.text_utils import contains_arabic, normalize_title, normalize_title_cross_language


@dataclass
class MatchResult:
    film_id: str | None
    confidence: float
    needs_review: bool
    reason: str


class TitleMatcher:
    """Closer to the effective Apps Script behavior than the Cursor scaffold.

    Conservative identity rules:
    - exact alias match first (same normalization)
    - normalized match second (transliteration-aware)
    - fuzzy match third
    - year bonus / penalty always applied in fuzzy+normalized stages
    """

    AUTO_THRESHOLD = 0.92
    REVIEW_THRESHOLD = 0.75
    YEAR_BONUS = 0.06
    YEAR_PENALTY = 0.05
    MAX_REVIEW_CANDIDATES = 5
    NEW_FILM_EVIDENCE_THRESHOLD = 0.84

    def __init__(self, session: Session) -> None:
        self.session = session
        # One load per ingest batch: avoids N full-table film_aliases scans (was freezing Streamlit backfills).
        self._aliases_cache: list[FilmAlias] | None = None
        self._film_by_id: dict[str, Film | None] = {}

    def _get_aliases(self) -> list[FilmAlias]:
        if self._aliases_cache is None:
            self._aliases_cache = list(self.session.execute(select(FilmAlias)).scalars().all())
        return self._aliases_cache

    def _register_new_alias(self, row: FilmAlias) -> None:
        if self._aliases_cache is not None:
            self._aliases_cache.append(row)

    def _get_film(self, film_id: str) -> Film | None:
        key = str(film_id)
        if key not in self._film_by_id:
            stmt = select(Film).where(cast(Film.id, String) == key).limit(1)
            self._film_by_id[key] = self.session.execute(stmt).scalar_one_or_none()
        return self._film_by_id[key]

    def _short_source_entity_id(self, value: str | None) -> str | None:
        if not value:
            return None
        if len(value) <= 64:
            return value
        # Deterministic shortening: keep prefix, add sha1 tail.
        tail = hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:8]
        return f"{value[:52]}#{tail}"

    def _upsert_alias(
        self,
        *,
        film_id: str,
        alias_text: str,
        alias_language: str | None,
        alias_type: str,
        confidence: float,
        source_entity_id: str | None,
    ) -> None:
        normalized_alias = normalize_title(alias_text)
        alias_language_norm = alias_language
        source_short = self._short_source_entity_id(source_entity_id)
        # Seed row from ensure_film is flushed there so SELECT sees it; avoid flush-on-every-row.
        # DB unique constraint is (film_id, normalized_alias) — not alias_type.
        # A seed alias and a source_title for the same string must merge, not INSERT twice.
        existing = self.session.execute(
            select(FilmAlias).where(
                cast(FilmAlias.film_id, String) == str(film_id),
                FilmAlias.normalized_alias == normalized_alias,
            )
        ).scalar_one_or_none()
        if existing:
            if confidence and float(confidence) > float(existing.confidence or 0):
                existing.confidence = confidence
            if source_short and not existing.source:
                existing.source = source_short
            if alias_language_norm and not existing.alias_language:
                existing.alias_language = alias_language_norm
            # Evidence-backed title wins over seed/canonical rows for the same normalized string.
            if alias_type == "source_title":
                existing.alias_type = "source_title"
            # Persist updates so later SELECTs (same transaction) see the new alias_type.
            self.session.flush()
            return
        row = FilmAlias(
            film_id=film_id,
            alias_text=alias_text,
            normalized_alias=normalized_alias,
            alias_language=alias_language_norm,
            alias_type=alias_type,
            confidence=confidence,
            source=source_short,
        )
        self.session.add(row)
        self._register_new_alias(row)
        self.session.flush()

    def _evidence_confidence(self, parser_confidence: float | None, source_confidence: float | None) -> float:
        pc = float(parser_confidence or 0.0)
        sc = float(source_confidence or 0.0)
        if pc <= 0 and sc <= 0:
            return 0.0
        return max(0.0, min(1.0, (pc + sc) / 2.0))

    def _score_cross_language(
        self,
        normalized_cross: str,
        alias: FilmAlias,
        release_year_hint: int | None,
    ) -> float:
        alias_cross = normalize_title_cross_language(alias.alias_text)
        score = fuzz.ratio(normalized_cross, alias_cross) / 100.0
        film = self._get_film(alias.film_id)
        if release_year_hint and film and film.release_year:
            score += self.YEAR_BONUS if release_year_hint == film.release_year else -self.YEAR_PENALTY
        return max(0.0, min(1.0, score))

    def match_or_queue(
        self,
        raw_title: str,
        release_year_hint: int | None = None,
        raw_evidence_id: str | None = None,
        record_scope: str = 'title',
        *,
        raw_title_ar: str | None = None,
        source_entity_id: str | None = None,
        parser_confidence: float | None = None,
        source_confidence: float | None = None,
    ) -> MatchResult:
        if record_scope != 'title':
            return MatchResult(film_id=None, confidence=0.0, needs_review=False, reason='not_applicable_non_title_scope')

        normalized_exact = normalize_title(raw_title)
        normalized_cross = normalize_title_cross_language(raw_title)
        aliases = self._get_aliases()

        # 1) Exact alias match first (same script/normalization).
        exact_candidates: list[tuple[FilmAlias, float]] = []
        for alias in aliases:
            if alias.normalized_alias == normalized_exact:
                score = 1.0
                film = self._get_film(alias.film_id)
                if release_year_hint and film and film.release_year:
                    score += self.YEAR_BONUS if release_year_hint == film.release_year else -self.YEAR_PENALTY
                    score = max(0.0, min(1.0, score))
                exact_candidates.append((alias, score))
        if exact_candidates:
            exact_candidates.sort(key=lambda x: x[1], reverse=True)
            best_alias, best_score = exact_candidates[0]
            if len(exact_candidates) > 1 and exact_candidates[1][1] >= self.REVIEW_THRESHOLD:
                self._queue_review(raw_title, release_year_hint, raw_evidence_id, best_alias.film_id, best_score, 'ambiguous_exact_alias_match')
                return MatchResult(film_id=None, confidence=best_score, needs_review=True, reason='queued_ambiguous_exact_match')

            film_id = best_alias.film_id
            self._upsert_alias(
                film_id=film_id,
                alias_text=raw_title,
                alias_language='ar' if contains_arabic(raw_title) else 'en',
                alias_type='source_title',
                confidence=best_score,
                source_entity_id=source_entity_id,
            )
            if raw_title_ar:
                self._upsert_alias(
                    film_id=film_id,
                    alias_text=raw_title_ar,
                    alias_language='ar',
                    alias_type='source_title',
                    confidence=best_score,
                    source_entity_id=source_entity_id,
                )
            return MatchResult(film_id=film_id, confidence=best_score, needs_review=False, reason='exact_alias_match')

        # 2) Normalized match second (transliteration-aware equality).
        normalized_candidates: list[tuple[FilmAlias, float]] = []
        for alias in aliases:
            alias_cross = normalize_title_cross_language(alias.alias_text)
            if alias_cross == normalized_cross:
                score = 1.0
                film = self._get_film(alias.film_id)
                if release_year_hint and film and film.release_year:
                    score += self.YEAR_BONUS if release_year_hint == film.release_year else -self.YEAR_PENALTY
                    score = max(0.0, min(1.0, score))
                normalized_candidates.append((alias, score))
        if normalized_candidates:
            normalized_candidates.sort(key=lambda x: x[1], reverse=True)
            best_alias, best_score = normalized_candidates[0]
            # If we find multiple equally-normalized aliases, require review.
            if len({a.film_id for a, _ in normalized_candidates}) > 1 and best_score >= self.REVIEW_THRESHOLD:
                self._queue_review(
                    raw_title,
                    release_year_hint,
                    raw_evidence_id,
                    best_alias.film_id,
                    best_score,
                    'ambiguous_normalized_alias_match',
                )
                return MatchResult(film_id=None, confidence=best_score, needs_review=True, reason='queued_ambiguous_normalized_match')

            film_id = best_alias.film_id
            self._upsert_alias(
                film_id=film_id,
                alias_text=raw_title,
                alias_language='ar' if contains_arabic(raw_title) else 'en',
                alias_type='source_title',
                confidence=1.0,
                source_entity_id=source_entity_id,
            )
            if raw_title_ar:
                self._upsert_alias(
                    film_id=film_id,
                    alias_text=raw_title_ar,
                    alias_language='ar',
                    alias_type='source_title',
                    confidence=1.0,
                    source_entity_id=source_entity_id,
                )
            return MatchResult(film_id=film_id, confidence=1.0, needs_review=False, reason='normalized_alias_match')

        # 3) Fuzzy candidates (cross-language).
        evidence_conf = self._evidence_confidence(parser_confidence, source_confidence)
        scored = sorted(
            ((alias, self._score_cross_language(normalized_cross, alias, release_year_hint)) for alias in aliases),
            key=lambda x: x[1],
            reverse=True,
        )
        top = scored[: self.MAX_REVIEW_CANDIDATES]
        if top and top[0][1] >= self.AUTO_THRESHOLD:
            best_alias, best_score = top[0]
            film_id = best_alias.film_id
            self._upsert_alias(
                film_id=film_id,
                alias_text=raw_title,
                alias_language='ar' if contains_arabic(raw_title) else 'en',
                alias_type='source_title',
                confidence=best_score,
                source_entity_id=source_entity_id,
            )
            if raw_title_ar:
                self._upsert_alias(
                    film_id=film_id,
                    alias_text=raw_title_ar,
                    alias_language='ar',
                    alias_type='source_title',
                    confidence=best_score,
                    source_entity_id=source_entity_id,
                )
            return MatchResult(film_id=film_id, confidence=best_score, needs_review=False, reason='auto_match')
        if top and top[0][1] >= self.REVIEW_THRESHOLD:
            best_alias, best_score = top[0]
            tie = len(top) > 1 and abs(top[0][1] - top[1][1]) <= 0.03
            reason = "ambiguous_fuzzy_title_match" if tie else "low_confidence_title_match"

            # If evidence is strong enough, don't send non-ambiguous low-title-similarity
            # titles to the review queue; auto-create the film and attach observed aliases.
            if not tie and evidence_conf >= self.NEW_FILM_EVIDENCE_THRESHOLD:
                film = self.ensure_film(raw_title, release_year_hint)
                self._upsert_alias(
                    film_id=film.id,
                    alias_text=raw_title,
                    alias_language="ar" if contains_arabic(raw_title) else "en",
                    alias_type="source_title",
                    confidence=evidence_conf,
                    source_entity_id=source_entity_id,
                )
                if raw_title_ar:
                    self._upsert_alias(
                        film_id=film.id,
                        alias_text=raw_title_ar,
                        alias_language="ar",
                        alias_type="source_title",
                        confidence=evidence_conf,
                        source_entity_id=source_entity_id,
                    )
                return MatchResult(
                    film_id=film.id,
                    confidence=evidence_conf,
                    needs_review=False,
                    reason="auto_created_new_film_from_weak_title_match",
                )

            self._queue_review(raw_title, release_year_hint, raw_evidence_id, best_alias.film_id, best_score, reason)
            return MatchResult(film_id=None, confidence=best_score, needs_review=True, reason="queued_for_review")

        # 4) If we don't have a good match, conservatively auto-create a film only when evidence confidence is high.
        best_score = top[0][1] if top else 0.0
        if evidence_conf >= self.NEW_FILM_EVIDENCE_THRESHOLD and raw_title and len(raw_title.strip()) >= 3:
            film = self.ensure_film(raw_title, release_year_hint)
            self._upsert_alias(
                film_id=film.id,
                alias_text=raw_title,
                alias_language='ar' if contains_arabic(raw_title) else 'en',
                alias_type='source_title',
                confidence=evidence_conf,
                source_entity_id=source_entity_id,
            )
            if raw_title_ar:
                self._upsert_alias(
                    film_id=film.id,
                    alias_text=raw_title_ar,
                    alias_language='ar',
                    alias_type='source_title',
                    confidence=evidence_conf,
                    source_entity_id=source_entity_id,
                )
            return MatchResult(film_id=film.id, confidence=evidence_conf, needs_review=False, reason='auto_created_new_film')

        self._queue_review(raw_title, release_year_hint, raw_evidence_id, None, best_score, 'no_strong_candidate')
        return MatchResult(film_id=None, confidence=best_score, needs_review=True, reason='queued_no_candidate')

    def _queue_review(
        self,
        raw_title: str,
        release_year_hint: int | None,
        raw_evidence_id: str | None,
        candidate_film_id: str | None,
        candidate_score: float,
        reason: str,
    ) -> None:
        self.session.add(
            ReviewQueue(
                raw_evidence_id=raw_evidence_id,
                film_title_raw=raw_title,
                release_year_hint=release_year_hint,
                candidate_film_id=candidate_film_id,
                candidate_score=candidate_score,
                reason=reason,
            )
        )

    def ensure_film(self, title: str, release_year: int | None) -> Film:
        normalized = normalize_title(title)
        film = self.session.execute(select(Film).where(Film.normalized_title == normalized, Film.release_year == release_year)).scalar_one_or_none()
        if film:
            return film
        film = Film(canonical_title=title, normalized_title=normalized, release_year=release_year)
        self.session.add(film)
        self.session.flush()
        self._film_by_id[str(film.id)] = film
        seed = FilmAlias(
            film_id=film.id,
            alias_text=title,
            normalized_alias=normalized,
            alias_language='ar' if contains_arabic(title) else 'en',
            alias_type='seed',
            source='matcher',
        )
        self.session.add(seed)
        self.session.flush()
        self._register_new_alias(seed)
        return film

    def attach_observed_titles(
        self,
        *,
        film_id: str,
        raw_title: str,
        raw_title_ar: str | None = None,
        source_entity_id: str | None = None,
        confidence: float = 1.0,
        alias_type: str = "source_title",
    ) -> None:
        """Attach source-observed titles as aliases (idempotent)."""
        self._upsert_alias(
            film_id=film_id,
            alias_text=raw_title,
            alias_language="ar" if contains_arabic(raw_title) else "en",
            alias_type=alias_type,
            confidence=confidence,
            source_entity_id=source_entity_id,
        )
        if raw_title_ar:
            self._upsert_alias(
                film_id=film_id,
                alias_text=raw_title_ar,
                alias_language="ar",
                alias_type=alias_type,
                confidence=confidence,
                source_entity_id=source_entity_id,
            )
