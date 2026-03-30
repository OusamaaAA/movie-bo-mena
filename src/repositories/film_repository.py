from rapidfuzz import fuzz
from sqlalchemy import Select, String, cast, or_, select
from sqlalchemy.orm import Session

from src.models import Film, FilmAlias
from src.services.text_utils import normalize_title, normalize_title_cross_language


class FilmRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def search(self, query: str, limit: int = 20) -> list[Film]:
        if not query or not query.strip():
            return []
        q_exact = normalize_title(query)
        nq = f"%{q_exact}%"
        stmt: Select[tuple[Film]] = (
            select(Film)
            .outerjoin(FilmAlias, FilmAlias.film_id == Film.id)
            .where(
                or_(
                    Film.normalized_title.ilike(nq),
                    FilmAlias.normalized_alias.ilike(nq),
                )
            )
            .limit(limit)
        )
        candidates = list(self.session.execute(stmt).scalars().unique().all())
        if not candidates:
            return []

        # Re-rank with transliteration-aware cross normalization.
        q_cross = normalize_title_cross_language(query)
        film_ids = [str(f.id) for f in candidates]
        aliases = list(
            self.session.execute(
                select(FilmAlias).where(cast(FilmAlias.film_id, String).in_(film_ids))
            ).scalars().all()
        )
        best_score_by_film: dict[str, float] = {fid: 0.0 for fid in film_ids}
        for a in aliases:
            cand_cross = normalize_title_cross_language(a.alias_text)
            score = fuzz.ratio(q_cross, cand_cross) / 100.0
            aid = str(a.film_id)
            best_score_by_film[aid] = max(best_score_by_film.get(aid, 0.0), score)

        candidates.sort(key=lambda f: best_score_by_film.get(str(f.id), 0.0), reverse=True)
        return candidates[:limit]

    def search_rows(self, query: str, limit: int = 20) -> list[dict]:
        """Detached-safe film search output for UI pages."""
        films = self.search(query, limit=limit)
        return [
            {
                "id": str(f.id),
                "canonical_title": f.canonical_title,
                "release_year": f.release_year,
            }
            for f in films
        ]

    def get(self, film_id: str) -> Film | None:
        # DB `films.id` is UUID in Supabase; compare as text to avoid UUID/varchar mismatch
        # when ORM metadata uses string ids.
        stmt = select(Film).where(cast(Film.id, String) == str(film_id)).limit(1)
        return self.session.execute(stmt).scalar_one_or_none()

    def create_if_missing(self, title: str, release_year: int | None = None) -> Film:
        normalized = normalize_title(title)
        stmt = select(Film).where(Film.normalized_title == normalized, Film.release_year == release_year)
        found = self.session.execute(stmt).scalar_one_or_none()
        if found:
            return found
        film = Film(canonical_title=title, normalized_title=normalized, release_year=release_year)
        self.session.add(film)
        self.session.flush()
        self.session.add(
            FilmAlias(
                film_id=film.id,
                alias_text=title,
                normalized_alias=normalized,
                alias_language="und",
                alias_type="canonical_seed",
                source="manual",
            )
        )
        return film

