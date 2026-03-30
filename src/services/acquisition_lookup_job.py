from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import re
from typing import Any
from urllib.parse import quote_plus, unquote

import httpx

from src.config import get_settings
from rapidfuzz import fuzz
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from src.models import Film, FilmAlias, LookupJob, NormalizedEvidence, RatingsMetric, RawEvidence, ReviewQueue
from src.repositories.film_repository import FilmRepository
from src.repositories.report_repository import ReportRepository
from src.services.ingestion_service import ingest_source, rebuild_reconciled_for_film
from src.services.json_utils import make_json_safe
from src.services.matching import TitleMatcher
from src.services.report_builder import build_film_report
from src.services.text_utils import contains_arabic, normalize_title, normalize_title_cross_language
from src.sources.boxofficemojo.client import BomClient
from src.sources.boxofficemojo.ingest import run_bom_weekly
from src.sources.boxofficemojo.parser import (
    normalize_bom_release_url,
    normalize_bom_title_url,
    parse_bom_search_results,
    parse_release_page_evidence,
    parse_release_page_header,
    parse_title_page_candidate,
)
from src.sources.common import ExtractedRecord
from src.sources.elcinema.client import ElCinemaClient
from src.sources.elcinema.ingest import run_elcinema_title_released_markets
from src.sources.elcinema.parser import parse_current_chart, parse_elcinema_search_results, parse_elcinema_work_metadata, parse_title_boxoffice
from src.sources.imdb.client import ImdbClient
from src.services.resolved_title_scoring import (
    RELEASE_TITLE_FALLBACK_MIN,
    row_accept_with_parent,
    score_candidate_hit,
    score_resolved_title,
    title_matches_query,
)
from src.sources.filmyard.ingest import run_filmyard_daily
from src.sources.imdb.ingest import run_imdb_daily


STRONG_THRESHOLD = 0.88
REVIEW_LOW_THRESHOLD = 0.70
AUTO_RESOLVE_MARGIN = 0.04

# Post–work-page metadata gate (discoverElCinemaWorkCandidates_ parity).
ELCINEMA_POST_META_MIN = 0.45
ELCINEMA_CHART_LEAD_MIN = 0.55
# Homepage ?s= often returns generic listings full of /work/ links (not real search results).
# If nothing scores at least this well against the query, ignore direct HTML and use web + IMDb assists.
ELCINEMA_DIRECT_SEARCH_TRUST_MIN = 0.55


STAGE_DISCOVERY = "discovery"
STAGE_IMDB_RATINGS = "imdb_ratings"
STAGE_E_LCINEMA_TITLE = "elcinema_title_lookup"
STAGE_E_LCINEMA_RELEASED = "elcinema_released_markets"
STAGE_BOM_TITLE = "bom_title_lookup"
STAGE_BOM_RELEASES = "bom_release_market_enrichment"
STAGE_SUPPLEMENTAL = "supplemental_charts"
STAGE_FINALIZE = "finalize"


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def _score_title_match(query: str, candidate_title: str | None, release_year_hint: int | None, candidate_year_hint: int | None) -> float:
    q_cross = normalize_title_cross_language(query)
    c_cross = normalize_title_cross_language(candidate_title or "")
    score = fuzz.ratio(q_cross, c_cross) / 100.0
    if release_year_hint and candidate_year_hint:
        score += 0.06 if release_year_hint == candidate_year_hint else -0.05
    return _clamp01(score)


def _coverage_from_matches(matches: list[dict[str, Any]]) -> dict[str, Any]:
    by_source: dict[str, dict[str, Any]] = {}
    for m in matches:
        src = m.get("source_name") or "-"
        score = float(m.get("match_score") or 0.0)
        entry = by_source.setdefault(
            src,
            {
                "source_name": src,
                "strong_matches": 0,
                "review_matches": 0,
                "weak_matches": 0,
                "matched_title_rows": 0,
                "best_match_score": 0.0,
            },
        )
        entry["matched_title_rows"] += 1
        entry["best_match_score"] = max(entry["best_match_score"], score)
        if score >= STRONG_THRESHOLD:
            entry["strong_matches"] += 1
        elif score >= REVIEW_LOW_THRESHOLD:
            entry["review_matches"] += 1
        else:
            entry["weak_matches"] += 1
    return {"by_source": dict(sorted(by_source.items(), key=lambda kv: kv[0]))}


def _needs_source_enrichment(coverage: dict[str, Any], source_name: str) -> bool:
    by_source = coverage.get("by_source") or {}
    entry = by_source.get(source_name)
    if not entry:
        return True
    if entry.get("strong_matches", 0) >= 1 and entry.get("matched_title_rows", 0) >= 2:
        return False
    return True


def _get_or_create_review_item(
    session: Session,
    *,
    raw_evidence_id: str | None,
    film_title_raw: str,
    release_year_hint: int | None,
    candidate_film_id: str | None,
    candidate_score: float,
    reason: str,
) -> None:
    if not raw_evidence_id:
        return
    exists = session.execute(
        select(ReviewQueue).where(ReviewQueue.raw_evidence_id == raw_evidence_id, ReviewQueue.status == "open")
    ).scalars().first()
    if exists:
        return
    session.add(
        ReviewQueue(
            raw_evidence_id=raw_evidence_id,
            film_title_raw=film_title_raw,
            release_year_hint=release_year_hint,
            candidate_film_id=candidate_film_id,
            candidate_score=Decimal(str(candidate_score)),
            status="open",
            reason=reason,
        )
    )


def _stage_order(job: LookupJob) -> list[str]:
    stages: list[str] = []
    stages.append(STAGE_DISCOVERY)
    if job.imdb_title_id:
        stages.append(STAGE_IMDB_RATINGS)
    stages.append(STAGE_E_LCINEMA_TITLE)
    stages.append(STAGE_E_LCINEMA_RELEASED)
    stages.append(STAGE_BOM_TITLE)
    stages.append(STAGE_BOM_RELEASES)
    stages.append(STAGE_SUPPLEMENTAL)
    stages.append(STAGE_FINALIZE)
    return stages


def _next_stage(job: LookupJob) -> str | None:
    stages = _stage_order(job)
    try:
        idx = stages.index(job.stage)
    except ValueError:
        return None
    if idx + 1 >= len(stages):
        return None
    return stages[idx + 1]


# Scores for film-linked source IDs (refresh path). High enough for row_accept_with_parent + BOM gates.
STORED_ENTITY_SCORE_EL = 0.96
STORED_ENTITY_SCORE_BOM_RELEASE = 0.96
STORED_ENTITY_SCORE_BOM_TITLE = 0.92


def _resolve_existing_film_id_from_matches(
    matches: list[dict[str, Any]],
    *,
    film_repo: FilmRepository | None = None,
    query: str | None = None,
    release_year_hint: int | None = None,
) -> str | None:
    linked = [
        m
        for m in matches
        if m.get("film_id") and float(m.get("match_score") or 0.0) >= STRONG_THRESHOLD
    ]
    if linked:
        linked.sort(key=lambda x: float(x.get("match_score") or 0.0), reverse=True)
        top = linked[0]
        fid = str(top["film_id"])
        top_score = float(top.get("match_score") or 0.0)
        for m in linked[1:4]:
            if (
                str(m.get("film_id")) != fid
                and abs(float(m.get("match_score") or 0.0) - top_score) <= AUTO_RESOLVE_MARGIN
            ):
                return None
        return fid

    if not film_repo or not query or not query.strip():
        return None

    candidates = film_repo.search(query.strip(), limit=5)
    if not candidates:
        return None

    def _film_fuzz_score(film: Film) -> float:
        q_cross = normalize_title_cross_language(query)
        c_cross = normalize_title_cross_language(film.canonical_title)
        sc = fuzz.ratio(q_cross, c_cross) / 100.0
        if release_year_hint and film.release_year:
            if int(release_year_hint) == int(film.release_year):
                sc = min(1.0, sc + 0.04)
            else:
                sc -= 0.08
        return max(0.0, min(1.0, sc))

    scored_films = sorted(((_film_fuzz_score(f), f) for f in candidates), key=lambda x: x[0], reverse=True)
    top_score, top_film = scored_films[0]
    if top_score < STRONG_THRESHOLD:
        return None
    if len(scored_films) > 1:
        s2, f2 = scored_films[1]
        if str(f2.id) != str(top_film.id) and abs(top_score - s2) <= AUTO_RESOLVE_MARGIN:
            return None
    return str(top_film.id)


def _stored_elcinema_candidate_dicts(work_ids: list[str], elcinema_base_url: str) -> list[dict[str, Any]]:
    base = elcinema_base_url.rstrip("/")
    out: list[dict[str, Any]] = []
    s = STORED_ENTITY_SCORE_EL
    for wid in work_ids:
        su = f"{base}/en/work/{wid}/"
        out.append(
            {
                "source": "elCinema",
                "work_id": wid,
                "entity_id": wid,
                "url": su,
                "source_url": su,
                "matched_title": None,
                "resolved_title_en": None,
                "resolved_title_ar": None,
                "resolved_release_year": None,
                "resolved_score": s,
                "hit_score": s,
                "display_title": "",
                "year": None,
                "score": s,
                "country": None,
                "origin": "database",
            },
        )
    return out


def _elcinema_candidates_from_recent_raw(
    session: Session,
    *,
    query_text: str,
    year_hint: int | None,
    base_url: str,
    max_scan: int = 6000,
    max_out: int = 12,
) -> list[dict[str, Any]]:
    """Mine prior elCinema raw title rows for likely work IDs."""
    rows = session.execute(
        select(
            RawEvidence.source_entity_id,
            RawEvidence.source_url,
            RawEvidence.film_title_raw,
            RawEvidence.release_year_hint,
        )
        .where(
            RawEvidence.source_name == "elCinema",
            RawEvidence.record_scope == "title",
        )
        .order_by(RawEvidence.created_at.desc())
        .limit(max_scan)
    ).all()
    by_work: dict[str, dict[str, Any]] = {}
    for source_entity_id, source_url, film_title_raw, release_year_hint_row in rows:
        work_id = ""
        if source_entity_id:
            m = re.match(r"^(\d+)", str(source_entity_id))
            if m:
                work_id = m.group(1)
        if not work_id and source_url:
            m = re.search(r"/work/(\d+)", str(source_url))
            if m:
                work_id = m.group(1)
        if not work_id:
            continue
        hs = score_candidate_hit(query_text, film_title_raw, year_hint)
        if year_hint and release_year_hint_row:
            hs += 0.06 if int(year_hint) == int(release_year_hint_row) else -0.05
        hs = max(0.0, min(1.0, hs))
        if hs < 0.58:
            continue
        prev = by_work.get(work_id)
        if prev is None or hs > float(prev.get("resolved_score") or 0.0):
            su = f"{base_url.rstrip('/')}/en/work/{work_id}/"
            by_work[work_id] = {
                "source": "elCinema",
                "work_id": work_id,
                "entity_id": work_id,
                "url": su,
                "source_url": su,
                "matched_title": film_title_raw,
                "resolved_title_en": film_title_raw,
                "resolved_title_ar": None,
                "resolved_release_year": release_year_hint_row,
                "resolved_score": hs,
                "hit_score": hs,
                "display_title": film_title_raw or "",
                "year": release_year_hint_row,
                "score": hs,
                "country": None,
                "origin": "recent_raw",
            }
    out = sorted(by_work.values(), key=lambda x: float(x.get("resolved_score") or 0.0), reverse=True)
    return out[:max_out]


def _stored_bom_title_candidate_dicts(title_urls: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    s = STORED_ENTITY_SCORE_BOM_TITLE
    for tu in title_urls:
        out.append(
            {
                "source": "Box Office Mojo",
                "entity_id": tu,
                "url": tu,
                "title_url": tu,
                "release_url": None,
                "matched_title": None,
                "resolved_title_en": None,
                "resolved_title_ar": None,
                "resolved_release_year": None,
                "resolved_score": s,
                "hit_score": s,
                "intl_gross_usd": None,
                "release_urls": [],
                "display_title": "",
                "year": None,
                "score": s,
                "country": None,
                "origin": "database",
            },
        )
    return out


def _stored_bom_release_candidate_dicts(release_urls: list[str], year_hint: int | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    s = STORED_ENTITY_SCORE_BOM_RELEASE
    for ru in release_urls:
        out.append(
            {
                "source": "Box Office Mojo",
                "entity_id": ru,
                "url": ru,
                "release_url": ru,
                "title_url": None,
                "resolved_title_en": None,
                "resolved_release_year": year_hint,
                "resolved_score": s,
                "hit_score": s,
                "display_title": "",
                "year": year_hint,
                "score": s,
                "country": None,
                "origin": "database",
            },
        )
    return out


def _merge_elcinema_candidates(
    priority: list[dict[str, Any]],
    secondary: list[dict[str, Any]],
    *,
    max_items: int = 20,
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for c in priority + secondary:
        eid = str(c.get("entity_id") or c.get("work_id") or "")
        if not eid or eid in seen:
            continue
        seen.add(eid)
        out.append(c)
        if len(out) >= max_items:
            break
    return out


def _merge_bom_title_candidates(
    priority: list[dict[str, Any]],
    secondary: list[dict[str, Any]],
    *,
    max_items: int = 8,
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for c in priority + secondary:
        u = str(c.get("url") or c.get("title_url") or "")
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(c)
        if len(out) >= max_items:
            break
    return out


@dataclass
class LookupJobView:
    job: LookupJob
    fast_matches_strong: list[dict[str, Any]]
    fast_matches_review: list[dict[str, Any]]
    coverage: dict[str, Any]
    warnings: list[dict[str, Any]]
    resolved_film: Film | None

    # UI convenience: stage shown as “current”, even if discovery already ran.
    next_stage_to_run: str | None
    report: Any | None


def _to_fast_bands(matches: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    strong: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    for m in sorted(matches, key=lambda x: float(x.get("match_score") or 0.0), reverse=True):
        score = float(m.get("match_score") or 0.0)
        if score >= STRONG_THRESHOLD:
            strong.append(m)
        elif score >= REVIEW_LOW_THRESHOLD:
            review.append(m)
    return strong[:20], review[:20]


def _compute_fast_matches(session: Session, job: LookupJob) -> list[dict[str, Any]]:
    repo = ReportRepository(session)
    return repo.title_evidence_matches(
        job.query_text,
        release_year_hint=job.release_year_hint,
        limit=40,
        sample_size=5000,
    )


def _classify_matches(matches: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    strong: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    weak: list[dict[str, Any]] = []
    for m in sorted(matches, key=lambda x: float(x.get("match_score") or 0.0), reverse=True):
        score = float(m.get("match_score") or 0.0)
        if score >= STRONG_THRESHOLD:
            strong.append(m)
        elif score >= REVIEW_LOW_THRESHOLD:
            review.append(m)
        else:
            weak.append(m)
    return {"strong_matches": strong[:20], "review_matches": review[:20], "weak_matches": weak[:20]}


def _discover_search_hits(queries: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    for q in queries:
        if not q.strip():
            continue
        try:
            rss = httpx.get(
                f"https://www.bing.com/search?format=rss&q={quote_plus(q)}",
                timeout=10.0,
                headers=headers,
                follow_redirects=True,
            ).text
            items = re.findall(r"<item\b[\s\S]*?</item>", rss, flags=re.I)
            for item in items:
                link_m = re.search(r"<link>([\s\S]*?)</link>", item, flags=re.I)
                title_m = re.search(r"<title>([\s\S]*?)</title>", item, flags=re.I)
                link = re.sub(r"\s+", " ", link_m.group(1)).strip() if link_m else ""
                title = re.sub(r"\s+", " ", title_m.group(1)).strip() if title_m else ""
                if link and link not in seen:
                    seen.add(link)
                    out.append({"url": link, "title": title, "snippet": ""})
        except Exception:  # noqa: BLE001
            pass
        try:
            html = httpx.get(
                f"https://html.duckduckgo.com/html/?q={quote_plus(q)}",
                timeout=10.0,
                headers=headers,
                follow_redirects=True,
            ).text
            for a in re.findall(r'<a[^>]+class="[^"]*(?:result__a|result-link)[^"]*"[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>', html, flags=re.I):
                href = a[0].replace("&amp;", "&")
                uddg = re.search(r"[?&]uddg=([^&]+)", href, flags=re.I)
                if uddg:
                    href = unquote(uddg.group(1))
                title = re.sub(r"<[^>]+>", " ", a[1])
                title = re.sub(r"\s+", " ", title).strip()
                if href and href not in seen:
                    seen.add(href)
                    out.append({"url": href, "title": title, "snippet": ""})
        except Exception:  # noqa: BLE001
            pass
        try:
            # Fallback proxy for environments where direct DuckDuckGo/Bing are filtered.
            proxy_html = httpx.get(
                f"https://r.jina.ai/http://html.duckduckgo.com/html/?q={quote_plus(q)}",
                timeout=15.0,
                follow_redirects=True,
            ).text
            base = get_settings().elcinema_base_url.rstrip("/")
            blob = unquote(proxy_html.replace("+", " "))
            for m in re.finditer(r"elcinema\.com/(?:en/)?work/(\d+)", blob, flags=re.I):
                wid = m.group(1)
                href = f"{base}/en/work/{wid}/"
                if href not in seen:
                    seen.add(href)
                    out.append({"url": href, "title": "", "snippet": ""})
        except Exception:  # noqa: BLE001
            pass
    return out


def _best_snippet_hit_score(
    query_text: str,
    year_hint: int | None,
    variants: list[str],
    snippet: str,
) -> float:
    """Score a search snippet against the query and transliteration variants (DDG titles vary)."""
    s = score_candidate_hit(query_text, snippet, year_hint)
    for v in variants[:8]:
        vt = (v or "").strip()
        if not vt:
            continue
        s = max(s, score_candidate_hit(vt, snippet, year_hint))
    return s


def _organic_elcinema_work_ids_via_proxy(
    query_text: str,
    year_hint: int | None,
    variants: list[str],
) -> dict[str, float]:
    """
    High-recall elCinema work IDs when on-site search returns unrelated /work/ noise.

    Uses DuckDuckGo HTML via r.jina.ai proxy (reliable in restricted networks), scores
    each ID using a local snippet around the URL match.
    """
    by_id: dict[str, float] = {}
    seen_q: set[str] = set()
    # De-dupe: variants list already includes the raw query as its first entry.
    seeds: list[str] = []
    for s in [query_text] + [v for v in variants if v and v.strip()]:
        if s in seeds:
            continue
        seeds.append(s)
    queries: list[str] = []
    for v in seeds[:4]:
        v = (v or "").strip()
        if not v:
            continue
        queries.extend([f'"{v}" elcinema', f"{v} elcinema"])
    for q in queries[:8]:
        if not q.strip() or q in seen_q:
            continue
        seen_q.add(q)
        try:
            # r.jina.ai returns 403 for many browser-like User-Agent / Accept combos.
            # Default httpx headers work reliably here.
            proxy_html = httpx.get(
                f"https://r.jina.ai/http://html.duckduckgo.com/html/?q={quote_plus(q)}",
                timeout=18.0,
                follow_redirects=True,
            ).text
        except Exception:  # noqa: BLE001
            continue
        # uddg= links are often percent-encoded: ...work%2F2095663...
        blob = unquote(proxy_html.replace("+", " "))
        for m in re.finditer(r"elcinema\.com/(?:en/)?work/(\d+)", blob, flags=re.I):
            wid = m.group(1)
            lo = max(0, m.start() - 160)
            hi = min(len(blob), m.end() + 160)
            snippet = blob[lo:hi]
            sc = _best_snippet_hit_score(query_text, year_hint, variants, snippet)
            prev = by_id.get(wid)
            if prev is None or sc > prev:
                by_id[wid] = sc
    return by_id


def _elcinema_direct_search_leads(query_text: str, year_hint: int | None, client: ElCinemaClient | None = None) -> dict[str, dict[str, Any]]:
    """Try to find elCinema work IDs via direct site search."""
    client = client or ElCinemaClient()
    by_work: dict[str, dict[str, Any]] = {}
    try:
        html = client.search_works(query_text)
        results = parse_elcinema_search_results(html)
        for r in results:
            work_id = r["work_id"]
            title = r.get("title", "")
            hit_score = score_candidate_hit(query_text, title, year_hint)
            # Year match bonus
            if year_hint and r.get("year") and int(r["year"]) == int(year_hint):
                hit_score = min(1.0, hit_score + 0.06)
            prev = by_work.get(work_id)
            if prev is None or hit_score > float(prev.get("hit_score") or 0.0):
                by_work[work_id] = {
                    "work_id": work_id,
                    "source_url": f"{client.base_url}/en/work/{work_id}/",
                    "matched_title": title,
                    "hit_score": hit_score,
                }
        if by_work:
            best = max(float(v.get("hit_score") or 0.0) for v in by_work.values())
            if best < ELCINEMA_DIRECT_SEARCH_TRUST_MIN:
                # Keep a tiny best-effort set instead of dropping everything.
                # This allows downstream metadata + row-level gates to recover
                # transliteration-heavy matches in bulk runs.
                top_items = sorted(
                    by_work.items(),
                    key=lambda kv: float(kv[1].get("hit_score") or 0.0),
                    reverse=True,
                )[:3]
                by_work = {k: v for k, v in top_items}
    except Exception:  # noqa: BLE001
        pass  # Fall through; web search fallback will run
    return by_work


def _elcinema_imdb_assisted_queries(query_text: str, year_hint: int | None) -> list[str]:
    """Build site-limited search queries from IMDb suggestions (same signal as BOM title discovery)."""
    try:
        imdb = ImdbClient()
        suggestions = imdb.search_suggestions(query_text)
    except Exception:  # noqa: BLE001
        return []
    seen: set[str] = set()
    out: list[str] = []
    for s in suggestions[:5]:
        title = (s.get("title") or "").strip()
        imdb_tid = (s.get("imdb_id") or "").strip()
        y = s.get("year")
        cands: list[str] = []
        if title:
            cands.append(f'site:elcinema.com "{title}"')
            if y:
                cands.append(f'site:elcinema.com "{title}" {y}')
            if year_hint and int(year_hint) != int(y or 0):
                cands.append(f'site:elcinema.com "{title}" {year_hint}')
        if imdb_tid:
            cands.append(f"site:elcinema.com {imdb_tid}")
        for q in cands:
            if q not in seen:
                seen.add(q)
                out.append(q)
    return out


def _elcinema_imdb_assisted_terms(query_text: str, year_hint: int | None) -> list[str]:
    """Extra direct-search terms sourced from IMDb suggestions (titles + tt IDs)."""
    try:
        imdb = ImdbClient()
        suggestions = imdb.search_suggestions(query_text)
    except Exception:  # noqa: BLE001
        return []
    out: list[str] = []
    seen: set[str] = set()
    for s in suggestions[:6]:
        title = str(s.get("title") or "").strip()
        imdb_id = str(s.get("imdb_id") or "").strip()
        y = s.get("year")
        for term in [title, imdb_id, f"{title} {y}" if title and y else ""]:
            t = str(term).strip()
            if t and t not in seen:
                seen.add(t)
                out.append(t)
    return out


def _elcinema_search_leads(query_text: str, year_hint: int | None, client: ElCinemaClient | None = None) -> dict[str, dict[str, Any]]:
    """Find elCinema work IDs: web proxy first, then on-site search, then broader web."""
    client = client or ElCinemaClient()
    base = get_settings().elcinema_base_url.rstrip("/")
    variants = _expand_discovery_query_variants(query_text)
    by_work: dict[str, dict[str, Any]] = {}

    # 1. DuckDuckGo+jina HTML (elcinema “?s=” listings are often unrelated /work/ noise).
    for wid, sc in _organic_elcinema_work_ids_via_proxy(query_text, year_hint, variants).items():
        raw_sc = float(sc or 0.0)
        # Snippets often use alternate spelling ("Al Selm" vs "El Selem"); DDG still tied to this query.
        hit_score = max(raw_sc, 0.74) if raw_sc >= 0.16 else raw_sc
        prev = by_work.get(wid)
        if prev is None or hit_score > float(prev.get("hit_score") or 0.0):
            by_work[wid] = {
                "work_id": wid,
                "source_url": f"{base}/en/work/{wid}/",
                "matched_title": query_text,
                "hit_score": hit_score,
            }
    if by_work:
        best_guess = max(float(v.get("hit_score") or 0.0) for v in by_work.values())
        if best_guess >= 0.72:
            return by_work

    # 2. On-site search across transliteration + IMDb-assisted terms.
    direct_terms: list[str] = []
    for qv in variants + _elcinema_imdb_assisted_terms(query_text, year_hint):
        if qv and qv not in direct_terms:
            direct_terms.append(qv)
    for qv in direct_terms:
        direct = _elcinema_direct_search_leads(qv, year_hint, client)
        for wid, lead in direct.items():
            canonical_hit = score_candidate_hit(query_text, lead.get("matched_title"), year_hint)
            lead = dict(lead)
            lead["hit_score"] = canonical_hit if canonical_hit > 0 else float(lead.get("hit_score") or 0.0)
            prev = by_work.get(wid)
            if prev is None or float(lead.get("hit_score") or 0.0) > float(prev.get("hit_score") or 0.0):
                by_work[wid] = lead

    if by_work:
        best_direct = max(float(v.get("hit_score") or 0.0) for v in by_work.values())
        if best_direct >= 0.80:
            return by_work
        if best_direct < 0.58:
            by_work = {}

    # 3. Fallback: web search engines (Bing RSS + DuckDuckGo + jina proxy)
    # Use broader queries — strict site:path restricts too much
    queries: list[str] = []
    for v in variants:
        q2 = re.sub(r"[^\u0600-\u06FFA-Za-z0-9]+", " ", v).strip()
        queries.extend(
            [
                f'site:elcinema.com "{v}" box office',
                f'site:elcinema.com/en/work/ "{v}"',
                f'site:elcinema.com "{v}"',
                f'"{v}" elcinema',
                f'"{v}" "elcinema.com/en/work/"',
            ]
        )
        if q2 and q2 != v:
            queries.append(f'site:elcinema.com "{q2}"')
    if year_hint:
        queries.insert(0, f'site:elcinema.com "{query_text}" {year_hint}')
    # IMDb titles + tt IDs surface elCinema pages when the site’s own search is a junk listing.
    imdb_q = _elcinema_imdb_assisted_queries(query_text, year_hint)
    queries = imdb_q + [q for q in queries if q not in set(imdb_q)]
    hits = _discover_search_hits(queries[:16])
    for h in hits:
        blob_all = f"{h.get('url', '')} {h.get('title', '')} {h.get('snippet', '')}"
        m = re.search(r"elcinema\.com/(?:en/)?work/(\d+)", blob_all, flags=re.I)
        if not m:
            continue
        work_id = m.group(1)
        blob = f"{h.get('title', '')} {h.get('snippet', '')}"
        hit_score = score_candidate_hit(query_text, blob, year_hint)
        if hit_score < 0.48:
            hit_score = max(hit_score, 0.72)
        prev = by_work.get(work_id)
        if prev is None or hit_score > float(prev.get("hit_score") or 0.0):
            by_work[work_id] = {
                "work_id": work_id,
                "source_url": h.get("url"),
                "matched_title": h.get("title"),
                "hit_score": hit_score,
            }
    # 5. Last-resort fallback: scan recent weekly chart pages and extract work IDs
    # from real chart rows. This avoids dependence on fragile site search/web indexing.
    if not by_work:
        today = date.today()
        for offset in range(20):
            d = today - timedelta(days=offset * 7)
            iso = d.isocalendar()
            try:
                week_html = client.fetch_boxoffice_chart_week(iso.year, iso.week)
                chart_rows = parse_current_chart(
                    week_html,
                    f"{client.base_url}/en/boxoffice?year={iso.year}&week={iso.week}",
                    country_code="EG",
                    fallback_date=d,
                )
            except Exception:  # noqa: BLE001
                continue
            for r in chart_rows:
                wid = str(r.source_entity_id or "")
                if not wid:
                    continue
                hit_score = max(
                    score_candidate_hit(query_text, r.film_title_raw, year_hint),
                    score_candidate_hit(query_text, r.film_title_ar_raw, year_hint),
                )
                if hit_score < 0.68:
                    continue
                prev = by_work.get(wid)
                if prev is None or hit_score > float(prev.get("hit_score") or 0.0):
                    by_work[wid] = {
                        "work_id": wid,
                        "source_url": f"{client.base_url.rstrip('/')}/en/work/{wid}/",
                        "matched_title": r.film_title_raw,
                        "hit_score": hit_score,
                    }
            if len(by_work) >= 6:
                break
    return by_work


def _expand_discovery_query_variants(query_text: str) -> list[str]:
    """Expand transliteration variants to improve cross-source discovery recall."""
    q = (query_text or "").strip()
    if not q:
        return []
    variants: list[str] = [q]
    lower = q.lower()

    # Common Arabic transliteration variants seen in source pages (token-boundary only).
    replacements = [
        (" mashroa ", " project "),
        (" mashroua ", " project "),
        (" mashroo3 ", " project "),
    ]
    padded = f" {lower} "
    for old, new in replacements:
        if old in padded:
            v = padded.replace(old, new).strip()
            if v and v not in variants:
                variants.append(v)
    # Leading article al/el swap only (never replace "al" inside words like "Sada").
    m_lead = re.match(r"^(al|el)\s+(.+)$", lower)
    if m_lead:
        flipped = ("el" if m_lead.group(1) == "al" else "al") + " " + m_lead.group(2)
        if flipped not in variants:
            variants.append(flipped)
    stripped = re.sub(r"^(al|el)\s+", "", lower).strip()
    if stripped and stripped not in variants:
        variants.append(stripped)
    return variants


def discover_elcinema_candidates(
    query_text: str,
    year_hint: int | None,
    client: ElCinemaClient | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Search leads + work-root metadata + scoreResolvedTitle_ parity (>= 0.80 post-meta).
    """
    client = client or ElCinemaClient()
    by_work = _elcinema_search_leads(query_text, year_hint, client)

    # Fallback: scan the current elCinema chart for title matches
    if not by_work:
        try:
            chart_html = client.fetch_boxoffice_chart()
            chart_rows = parse_current_chart(chart_html, f"{client.base_url}/en/boxoffice", chart_limit=100)
            for r in chart_rows:
                blob = (r.film_title_ar_raw or r.film_title_raw) or ""
                # Score against BOTH title variants: Arabic-titled charts must also match Latin queries
                lead_score = max(
                    score_candidate_hit(query_text, r.film_title_raw, year_hint),
                    score_candidate_hit(query_text, r.film_title_ar_raw, year_hint),
                )
                if lead_score < ELCINEMA_CHART_LEAD_MIN or not r.source_entity_id:
                    continue
                wid = str(r.source_entity_id)
                if wid not in by_work:
                    by_work[wid] = {
                        "work_id": wid,
                        "source_url": f"{client.base_url}/en/work/{wid}/",
                        "matched_title": r.film_title_raw,
                        "hit_score": lead_score,
                    }
        except Exception:  # noqa: BLE001
            pass

    resolved_out: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    probe_budget = 25
    for work_id, lead in sorted(by_work.items(), key=lambda x: float(x[1].get("hit_score") or 0.0), reverse=True):
        try:
            html_en = client.fetch_work_root_en(work_id)
            html_ar: str | None = None
            try:
                html_ar = client.fetch_work_root_ar(work_id)
            except Exception:  # noqa: BLE001
                html_ar = None
            source_url_en = f"{client.base_url}/en/work/{work_id}/"
            meta = parse_elcinema_work_metadata(html_en, html_ar, work_id, source_url_en)
            resolved_score = max(
                float(lead.get("hit_score") or 0.0),
                score_resolved_title(
                    query_text,
                    meta.get("title_en"),
                    meta.get("title_ar"),
                    meta.get("release_year"),
                    year_hint,
                ),
            )
            # Fallback probe: if metadata is weak, inspect /boxoffice rows directly.
            # This rescues valid titles when work-root metadata is sparse or noisy.
            if resolved_score < ELCINEMA_POST_META_MIN and probe_budget > 0:
                probe_budget -= 1
                try:
                    title_html = client.fetch_title_boxoffice(work_id)
                    title_rows = parse_title_boxoffice(title_html, f"{client.base_url}/en/work/{work_id}/boxoffice", work_id)
                    row_scores = [
                        score_resolved_title(
                            query_text,
                            r.film_title_raw,
                            r.film_title_ar_raw,
                            r.release_year_hint,
                            year_hint,
                        )
                        for r in title_rows[:40]
                    ]
                    best_row_score = max(row_scores) if row_scores else 0.0
                    if best_row_score >= 0.60:
                        resolved_score = max(resolved_score, min(0.95, best_row_score + 0.05))
                except Exception:  # noqa: BLE001
                    pass
            if resolved_score < ELCINEMA_POST_META_MIN:
                rejects.append(
                    {"work_id": work_id, "reason": "below_post_meta_threshold", "resolved_score": resolved_score},
                )
                continue
            resolved_out.append(
                {
                    "source": "elCinema",
                    "work_id": work_id,
                    "entity_id": work_id,
                    "url": meta.get("source_url") or source_url_en,
                    "source_url": meta.get("source_url") or source_url_en,
                    "matched_title": lead.get("matched_title"),
                    "resolved_title_en": meta.get("title_en"),
                    "resolved_title_ar": meta.get("title_ar"),
                    "resolved_release_year": meta.get("release_year"),
                    "resolved_score": resolved_score,
                    "hit_score": float(lead.get("hit_score") or 0.0),
                    "display_title": meta.get("title_en") or lead.get("matched_title"),
                    "year": meta.get("release_year"),
                    "score": resolved_score,
                    "country": None,
                },
            )
        except Exception as exc:  # noqa: BLE001
            rejects.append({"work_id": work_id, "reason": "fetch_or_parse_error", "message": str(exc)[:300]})
    resolved_out.sort(key=lambda x: float(x.get("resolved_score") or 0.0), reverse=True)
    return resolved_out[:20], {"rejects": rejects, "search_work_ids": list(by_work.keys())}


def _bom_leads_from_imdb_suggestions(query_text: str, year_hint: int | None) -> list[dict[str, Any]]:
    """Use IMDb suggestion API to find movie tt IDs, then construct BOM title URLs."""
    imdb = ImdbClient()
    suggestions = imdb.search_suggestions(query_text)
    out: list[dict[str, Any]] = []
    for s in suggestions:
        imdb_id = s.get("imdb_id", "")
        title = s.get("title", "")
        year = s.get("year")
        hit_score = score_candidate_hit(query_text, title, year_hint)
        # Year bonus/penalty
        if year_hint and year:
            if int(year_hint) == int(year):
                hit_score = min(1.0, hit_score + 0.06)
            else:
                hit_score = max(0.0, hit_score - 0.05)
        title_url = f"https://www.boxofficemojo.com/title/{imdb_id}/"
        out.append({
            "title_url": title_url,
            "release_url": None,
            "source_hit_url": title_url,
            "hit_title": title,
            "hit_score": hit_score,
            "imdb_id": imdb_id,
            "year": year,
        })
    return out


def _bom_leads_from_search(query_text: str, year_hint: int | None, client: BomClient | None = None) -> list[dict[str, Any]]:
    """Use BOM's own search page to find title/release URLs."""
    client = client or BomClient()
    out: list[dict[str, Any]] = []
    try:
        html = client.search_titles(query_text)
        results = parse_bom_search_results(html)
        for r in results:
            imdb_id = r.get("imdb_id", "")
            title = r.get("title", "")
            year = r.get("year")
            hit_score = score_candidate_hit(query_text, title, year_hint)
            if year_hint and year:
                if int(year_hint) == int(year):
                    hit_score = min(1.0, hit_score + 0.06)
                else:
                    hit_score = max(0.0, hit_score - 0.05)
            title_url = f"https://www.boxofficemojo.com/title/{imdb_id}/"
            out.append({
                "title_url": title_url,
                "release_url": None,
                "source_hit_url": r.get("url", title_url),
                "hit_title": title,
                "hit_score": hit_score,
                "imdb_id": imdb_id,
                "year": year,
            })
    except Exception:  # noqa: BLE001
        pass
    return out


def collect_bom_search_leads(query_text: str, year_hint: int | None) -> list[dict[str, Any]]:
    """Find BOM title/release URLs: IMDb suggestions + BOM search first, web search fallback."""
    by_key: dict[str, dict[str, Any]] = {}

    def _merge_leads(leads: list[dict[str, Any]]) -> None:
        for lead in leads:
            tu = lead.get("title_url")
            ru = lead.get("release_url")
            key = tu or ru or ""
            if not key:
                continue
            hs = float(lead.get("hit_score") or 0.0)
            prev = by_key.get(key)
            if prev is None or hs > float(prev.get("hit_score") or 0.0):
                by_key[key] = lead

    # 1. IMDb suggestion API + BOM direct search across variants.
    for qv in _expand_discovery_query_variants(query_text):
        _merge_leads(_bom_leads_from_imdb_suggestions(qv, year_hint))
        _merge_leads(_bom_leads_from_search(qv, year_hint))

    # 3. If we have good results, skip web search
    if by_key:
        return sorted(by_key.values(), key=lambda x: float(x.get("hit_score") or 0.0), reverse=True)

    # 4. Fallback: web search engines
    variants: list[str] = []
    for base in _expand_discovery_query_variants(query_text):
        q = base.strip()
        if q and q not in variants:
            variants.append(q)
        q2 = re.sub(r"[^\u0600-\u06FFA-Za-z0-9]+", " ", base).strip()
        if q2 and q2 not in variants:
            variants.append(q2)
    queries: list[str] = []
    for v in variants:
        queries.append(f'site:boxofficemojo.com/title/ "{v}"')
        queries.append(f'site:boxofficemojo.com/release/ "{v}"')
        if year_hint:
            queries.append(f'site:boxofficemojo.com/title/ "{v}" "{year_hint}"')
            queries.append(f'site:boxofficemojo.com/release/ "{v}" "{year_hint}"')
    for h in _discover_search_hits(queries):
        tu = normalize_bom_title_url(h.get("url"))
        ru = normalize_bom_release_url(h.get("url"))
        if not tu and not ru:
            continue
        key = ru or tu or ""
        blob = f"{h.get('title', '')} {h.get('snippet', '')}"
        hs = score_candidate_hit(query_text, blob, year_hint)
        prev = by_key.get(key)
        if prev is None or hs > float(prev.get("hit_score") or 0.0):
            by_key[key] = {
                "title_url": tu,
                "release_url": ru,
                "source_hit_url": h.get("url"),
                "hit_title": h.get("title"),
                "hit_score": hs,
            }
    return sorted(by_key.values(), key=lambda x: float(x.get("hit_score") or 0.0), reverse=True)


def resolve_bom_leads(
    query_text: str,
    year_hint: int | None,
    leads: list[dict[str, Any]],
    client: BomClient,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Fetch BOM title / release pages for search leads; rescore with resolved metadata."""
    title_out: list[dict[str, Any]] = []
    release_by_url: dict[str, dict[str, Any]] = {}
    rejects: list[dict[str, Any]] = []
    processed_titles: set[str] = set()

    def _release_row(
        rel: str,
        *,
        title_url: str | None,
        resolved_title_en: str | None,
        resolved_release_year: int | None,
        resolved_score: float,
        hit_score: float,
    ) -> None:
        payload = {
            "source": "Box Office Mojo",
            "entity_id": rel,
            "url": rel,
            "release_url": rel,
            "title_url": title_url,
            "resolved_title_en": resolved_title_en,
            "resolved_release_year": resolved_release_year,
            "resolved_score": resolved_score,
            "hit_score": hit_score,
            "display_title": resolved_title_en or "",
            "year": resolved_release_year or year_hint,
            "score": resolved_score,
            "country": None,
        }
        prev = release_by_url.get(rel)
        if not prev or resolved_score > float(prev.get("resolved_score") or 0.0):
            release_by_url[rel] = payload

    for lead in leads[:16]:
        tu = lead.get("title_url")
        ru = lead.get("release_url")
        hit_score = float(lead.get("hit_score") or 0.0)
        if tu and tu not in processed_titles:
            processed_titles.add(tu)
            try:
                html = client.fetch_title_page(tu)
                info = parse_title_page_candidate(html, tu)
                ry = info.get("release_year")
                tit = (info.get("title_en") or "").strip()
                resolved_score = max(
                    hit_score,
                    score_resolved_title(query_text, tit or None, None, ry, year_hint),
                )
                if resolved_score < REVIEW_LOW_THRESHOLD:
                    rejects.append({"kind": "bom_title", "url": tu, "reason": "below_review_threshold", "resolved_score": resolved_score})
                    continue
                cand = {
                    "source": "Box Office Mojo",
                    "entity_id": tu,
                    "url": tu,
                    "title_url": tu,
                    "release_url": None,
                    "matched_title": lead.get("hit_title"),
                    "resolved_title_en": tit or None,
                    "resolved_title_ar": None,
                    "resolved_release_year": ry,
                    "resolved_score": resolved_score,
                    "hit_score": hit_score,
                    "intl_gross_usd": info.get("intl_gross_usd"),
                    "release_urls": list(info.get("release_urls") or []),
                    "display_title": tit or lead.get("hit_title"),
                    "year": ry,
                    "score": resolved_score,
                    "country": None,
                }
                title_out.append(cand)
                for rel in cand["release_urls"]:
                    norm = normalize_bom_release_url(rel)
                    if norm:
                        _release_row(
                            norm,
                            title_url=tu,
                            resolved_title_en=cand.get("resolved_title_en"),
                            resolved_release_year=ry,
                            resolved_score=resolved_score,
                            hit_score=hit_score,
                        )
            except Exception as exc:  # noqa: BLE001
                rejects.append({"kind": "bom_title", "url": tu, "message": str(exc)[:300]})

        if ru and not tu:
            try:
                html = client.fetch_release_page(ru)
                t, ryy = parse_release_page_header(html, year_hint)
                rs = max(hit_score, score_resolved_title(query_text, t or None, None, ryy, year_hint))
                if not title_matches_query(query_text, t, None, ryy, year_hint) and rs < RELEASE_TITLE_FALLBACK_MIN:
                    rejects.append({"kind": "bom_release", "url": ru, "reason": "below_release_gate", "resolved_score": rs})
                    continue
                norm_ru = normalize_bom_release_url(ru)
                if norm_ru:
                    _release_row(
                        norm_ru,
                        title_url=None,
                        resolved_title_en=t or None,
                        resolved_release_year=ryy,
                        resolved_score=rs,
                        hit_score=hit_score,
                    )
            except Exception as exc:  # noqa: BLE001
                rejects.append({"kind": "bom_release", "url": ru, "message": str(exc)[:300]})

    title_list = sorted(title_out, key=lambda x: float(x.get("resolved_score") or 0.0), reverse=True)[:8]
    release_list = sorted(release_by_url.values(), key=lambda x: float(x.get("resolved_score") or 0.0), reverse=True)[:12]
    return title_list, release_list, {"rejects": rejects}


def discover_bom_candidates_bundle(
    query_text: str,
    year_hint: int | None,
    client: BomClient | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    client = client or BomClient()
    leads = collect_bom_search_leads(query_text, year_hint)
    return resolve_bom_leads(query_text, year_hint, leads, client)


def _safe_refresh(session: Session, obj: Any) -> None:
    """Refresh an ORM instance, silently ignoring errors if it was expelled by a rollback."""
    try:
        session.refresh(obj)
    except Exception:  # noqa: BLE001
        pass


def start_acquisition_lookup_job(
    session: Session,
    *,
    query: str,
    release_year_hint: int | None,
    imdb_title_id: str | None,
    elcinema_work_id_hint: str | None = None,
) -> LookupJobView:
    query = query.strip()
    init_ctx: dict[str, Any] = {}
    if elcinema_work_id_hint and str(elcinema_work_id_hint).strip():
        init_ctx["elcinema_work_id_hint"] = str(elcinema_work_id_hint).strip()
    job = LookupJob(
        query_text=query,
        release_year_hint=release_year_hint,
        imdb_title_id=imdb_title_id or None,
        status="running",
        is_active=True,
        stage=STAGE_DISCOVERY,
        context_json=init_ctx if init_ctx else None,
    )
    session.add(job)
    session.flush()
    # Persist the job row before running discovery so a later rollback in stage
    # processing cannot erase the job itself (which caused "Lookup job not found").
    session.commit()
    job = session.execute(select(LookupJob).where(LookupJob.id == job.id)).scalar_one()

    # Execute discovery immediately for fast analyst retrieval.
    _run_lookup_job_stage(session, job.id, max_stage=STAGE_DISCOVERY)
    session.flush()
    # After discovery stage, the job.stage will have advanced to the next stage to run.
    _safe_refresh(session, job)
    return lookup_job_view(session, job.id)


def resume_acquisition_lookup_job(session: Session, job_id: str) -> LookupJobView:
    job = session.execute(select(LookupJob).where(LookupJob.id == job_id)).scalar_one_or_none()
    if not job or not job.is_active:
        raise ValueError("Lookup job not found or inactive.")

    try:
        _run_lookup_job_stage(session, job_id, max_stage=job.stage)
        session.flush()
    except Exception:  # noqa: BLE001
        pass  # stage runner already rolled back and persisted failure state
    _safe_refresh(session, job)
    return lookup_job_view(session, job_id)


def lookup_job_view(session: Session, job_id: str) -> LookupJobView:
    job = session.execute(select(LookupJob).where(LookupJob.id == job_id)).scalar_one_or_none()
    if not job:
        raise ValueError("Lookup job not found.")

    resolved_film = session.get(Film, job.resolved_film_id) if job.resolved_film_id else None

    coverage = job.coverage_json or {}
    warnings = list((job.warnings_json or {}).get("warnings", []) or [])

    fast_matches_all = job.fast_matches_json or []
    fast_matches_strong, fast_matches_review = _to_fast_bands(fast_matches_all if isinstance(fast_matches_all, list) else [])
    next_stage_to_run = job.stage

    # Provide report only if finalize ran.
    report = None
    if job.status == "completed" and resolved_film:
        report = build_film_report(ReportRepository(session), resolved_film)

    return LookupJobView(
        job=job,
        fast_matches_strong=fast_matches_strong,
        fast_matches_review=fast_matches_review,
        coverage=coverage,
        warnings=warnings,
        resolved_film=resolved_film,
        next_stage_to_run=next_stage_to_run,
        report=report,
    )


def _append_job_warning(job: LookupJob, warning: dict[str, Any]) -> None:
    warnings_blob = make_json_safe(job.warnings_json or {})
    warnings_list = warnings_blob.get("warnings") or []
    warnings_list.append(make_json_safe(warning))
    job.warnings_json = make_json_safe({"warnings": warnings_list})


def _sanitize_lookup_job_json_fields(job: LookupJob) -> None:
    job.warnings_json = make_json_safe(job.warnings_json or {"warnings": []})
    job.coverage_json = make_json_safe(job.coverage_json or {})
    job.fast_matches_json = make_json_safe(job.fast_matches_json or [])
    job.context_json = make_json_safe(job.context_json or {})


def _append_job_note(job: LookupJob, note: str) -> None:
    if not job.notes:
        job.notes = note
    else:
        job.notes = f"{job.notes}\n{note}"


def _merge_bom_release_candidates(prior: list[dict[str, Any]], additions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_url: dict[str, dict[str, Any]] = {}
    for p in prior:
        u = str(p.get("url") or p.get("release_url") or "")
        if u:
            by_url[u] = dict(p)
    for p in additions:
        u = str(p.get("url") or p.get("release_url") or "")
        if not u:
            continue
        prev = by_url.get(u)
        if not prev or float(p.get("resolved_score") or 0.0) > float(prev.get("resolved_score") or 0.0):
            by_url[u] = dict(p)
    return sorted(by_url.values(), key=lambda x: float(x.get("resolved_score") or 0.0), reverse=True)


def _promote_from_live_evidence(session: Session, job: LookupJob, *, stage: str, matcher: TitleMatcher) -> None:
    """Immediate canonical promotion from ingested DB state (live row parity)."""
    ctx = job.context_json = job.context_json or {}
    dbg = ctx.setdefault("debug", {})
    events: list[dict[str, Any]] = dbg.setdefault("promotion_events", [])
    matches = _compute_fast_matches(session, job)
    if not matches:
        events.append({"stage": stage, "ran": True, "outcome": "no_matches"})
        return
    best = sorted(matches, key=lambda x: float(x.get("match_score") or 0.0), reverse=True)[0]
    score = float(best.get("match_score") or 0.0)
    film_id = best.get("film_id")
    created = False
    resolved: Film | None = None
    if film_id:
        resolved = session.get(Film, film_id)
    elif score >= STRONG_THRESHOLD:
        seed_title = str(best.get("title") or job.query_text).strip()
        resolved = matcher.ensure_film(seed_title, job.release_year_hint or best.get("release_year_hint"))
        created = True

    if not resolved:
        events.append({"stage": stage, "ran": True, "outcome": "skipped_weak", "best_score": score})
        return

    job.resolved_film_id = resolved.id

    title_set: set[str] = {job.query_text.strip()}
    for c in ctx.get("elcinema_candidates") or []:
        for k in ("resolved_title_en", "resolved_title_ar", "matched_title"):
            v = c.get(k)
            if v:
                title_set.add(str(v).strip())
    for c in ctx.get("bom_title_candidates") or []:
        for k in ("resolved_title_en", "matched_title"):
            v = c.get(k)
            if v:
                title_set.add(str(v).strip())
    for m in sorted(matches, key=lambda x: float(x.get("match_score") or 0.0), reverse=True)[:16]:
        t = m.get("title")
        if t:
            title_set.add(str(t).strip())

    for t in title_set:
        if not t:
            continue
        is_query = t == job.query_text.strip()
        matcher.attach_observed_titles(
            film_id=resolved.id,
            raw_title=t,
            raw_title_ar=t if contains_arabic(t) else None,
            source_entity_id=f"lookup:{job.id}",
            confidence=0.95 if is_query else 0.85,
            alias_type="query_title" if is_query else "source_title",
        )

    promo = {
        "film_id": str(resolved.id),
        "canonical_title": resolved.canonical_title,
        "created_new_film": created,
        "best_match_score": score,
        "stage": stage,
    }
    prev = ctx.get("live_promotion") or {}
    if float(prev.get("best_match_score") or 0.0) <= score:
        ctx["live_promotion"] = promo
    events.append({"stage": stage, "ran": True, "outcome": "promoted", **promo})


def _relink_recent_unmatched_title_rows(
    session: Session,
    *,
    film_id: str,
    query_text: str,
    release_year_hint: int | None,
) -> int:
    """
    Attach recent unlinked title rows that moderately match the query to the resolved film.
    This is a safe recovery path for transliteration-heavy titles where discovery fetched rows
    but matching confidence stayed just below strong thresholds.
    """
    q_norm = normalize_title_cross_language(query_text or "")
    if not q_norm:
        return 0
    candidates = list(
        session.execute(
            select(NormalizedEvidence, RawEvidence)
            .join(RawEvidence, RawEvidence.id == NormalizedEvidence.raw_evidence_id)
            .where(
                NormalizedEvidence.film_id.is_(None),
                RawEvidence.record_scope == "title",
                RawEvidence.source_name.in_(["elCinema", "Box Office Mojo"]),
            )
            .order_by(RawEvidence.created_at.desc())
            .limit(1200)
        ).all()
    )
    relinked = 0
    for ne, raw in candidates:
        cand = normalize_title_cross_language(raw.film_title_raw or "")
        score = fuzz.ratio(q_norm, cand) / 100.0 if cand else 0.0
        # Gentle year check when both hints exist.
        if release_year_hint and raw.release_year_hint:
            score += 0.06 if int(release_year_hint) == int(raw.release_year_hint) else -0.05
        if score < 0.50:
            continue
        ne.film_id = film_id
        ne.match_confidence = Decimal(str(max(float(ne.match_confidence or 0), min(score, 0.80))))
        relinked += 1
    if relinked > 0:
        rebuild_reconciled_for_film(session, film_id)
    return relinked


def _run_lookup_job_stage(session: Session, job_id: str, *, max_stage: str) -> None:
    job = session.execute(select(LookupJob).where(LookupJob.id == job_id)).scalar_one_or_none()
    if not job:
        raise ValueError("Lookup job not found.")

    # Ensure started_at exists.
    if not job.started_at:
        job.started_at = job.started_at or job.updated_at

    repo = ReportRepository(session)
    film_repo = FilmRepository(session)
    matcher = TitleMatcher(session)

    stage = job.stage
    if stage != max_stage:
        # We only run the caller’s intended stage; “max_stage” name is for convenience.
        return

    try:
        if stage == STAGE_DISCOVERY:
            matches = _compute_fast_matches(session, job)
            bands = _classify_matches(matches)
            # Store all matches so the UI can show strong vs review bands.
            job.fast_matches_json = matches
            job.coverage_json = _coverage_from_matches(matches)
            job.context_json = job.context_json or {}
            job.context_json["stored_search"] = bands
            job.warnings_json = job.warnings_json or {"warnings": []}

            # Review queue integration:
            # - unlinked evidence rows in review+ band
            # - ambiguous top candidates
            best = sorted(matches, key=lambda x: float(x.get("match_score") or 0.0), reverse=True)[:10]

            unlinked = [m for m in matches if m.get("film_id") is None and float(m.get("match_score") or 0.0) >= REVIEW_LOW_THRESHOLD]
            for m in unlinked[:5]:
                _get_or_create_review_item(
                    session,
                    raw_evidence_id=m.get("raw_evidence_id"),
                    film_title_raw=m.get("title") or job.query_text,
                    release_year_hint=job.release_year_hint,
                    candidate_film_id=None,
                    candidate_score=float(m.get("match_score") or 0.0),
                    reason="lookup_unlinked_title_evidence",
                )

            if len({m.get("film_id") for m in best if m.get("film_id")}) >= 2:
                top_two = best[:2]
                if len(top_two) == 2:
                    s0 = float(top_two[0].get("match_score") or 0.0)
                    s1 = float(top_two[1].get("match_score") or 0.0)
                    if (
                        abs(s0 - s1) <= 0.03
                        and s0 >= REVIEW_LOW_THRESHOLD
                        and top_two[0].get("film_id") != top_two[1].get("film_id")
                    ):
                        _get_or_create_review_item(
                            session,
                            raw_evidence_id=top_two[0].get("raw_evidence_id"),
                            film_title_raw=top_two[0].get("title") or job.query_text,
                            release_year_hint=job.release_year_hint,
                            candidate_film_id=top_two[0].get("film_id"),
                            candidate_score=s0,
                            reason="lookup_ambiguous_title_evidence",
                        )

            # Acquisition warnings: missing title-level evidence.
            coverage = job.coverage_json
            by_source = coverage.get("by_source") or {}
            major_sources = ["Filmyard", "elCinema", "Box Office Mojo"]
            missing = [s for s in major_sources if (by_source.get(s, {}).get("matched_title_rows") or 0) == 0]
            if missing:
                _append_job_warning(
                    job,
                    {
                        "type": "missing_title_evidence",
                        "message": f"Missing title-level evidence from: {', '.join(missing)}",
                        "sources": missing,
                    },
                )

            # Weak-only evidence warnings (hidden by default in UI).
            weak_only_sources = [
                s
                for s in major_sources
                if (by_source.get(s, {}).get("strong_matches", 0) == 0)
                and (by_source.get(s, {}).get("review_matches", 0) == 0)
                and (by_source.get(s, {}).get("weak_matches", 0) > 0)
            ]
            if weak_only_sources:
                _append_job_warning(
                    job,
                    {
                        "type": "weak_evidence_only",
                        "message": f"Weak fuzzy matches only for: {', '.join(weak_only_sources)} (hidden by default).",
                        "sources": weak_only_sources,
                    },
                )

            # Resolve existing film (title evidence or Film/alias search) → reuse stored IDs, then refresh.
            existing_film_id = _resolve_existing_film_id_from_matches(
                matches,
                film_repo=film_repo,
                query=job.query_text,
                release_year_hint=job.release_year_hint,
            )
            stored_ids: dict[str, list[str]] = {
                "elcinema_work_ids": [],
                "bom_release_urls": [],
                "bom_title_urls": [],
            }
            if existing_film_id:
                job.resolved_film_id = existing_film_id
                stored_ids = repo.stored_acquisition_source_ids(existing_film_id)
                _append_job_note(
                    job,
                    "Discovery: matched existing film; loading stored source IDs and merging online discovery for refresh.",
                )

            elc_client = ElCinemaClient()
            stored_elc = _stored_elcinema_candidate_dicts(stored_ids["elcinema_work_ids"], elc_client.base_url)
            recent_raw_elc = _elcinema_candidates_from_recent_raw(
                session,
                query_text=job.query_text,
                year_hint=job.release_year_hint,
                base_url=elc_client.base_url,
            )
            stored_bom_titles = _stored_bom_title_candidate_dicts(stored_ids["bom_title_urls"])
            stored_bom_rels = _stored_bom_release_candidate_dicts(
                stored_ids["bom_release_urls"],
                job.release_year_hint,
            )

            # If the caller injected a known elCinema work_id (e.g. from bulk lookup input),
            # prepend it as a high-confidence candidate so discovery can't miss it.
            elc_hint_wid = (job.context_json or {}).get("elcinema_work_id_hint")
            hint_candidates: list[dict[str, Any]] = []
            if elc_hint_wid:
                stored_eids = {str(c.get("entity_id") or c.get("work_id") or "") for c in stored_elc}
                if str(elc_hint_wid) not in stored_eids:
                    hint_candidates = [{
                        "source": "elCinema",
                        "work_id": str(elc_hint_wid),
                        "entity_id": str(elc_hint_wid),
                        "url": f"{elc_client.base_url}/en/work/{elc_hint_wid}/",
                        "source_url": f"{elc_client.base_url}/en/work/{elc_hint_wid}/",
                        "matched_title": job.query_text,
                        "resolved_title_en": None,
                        "resolved_title_ar": None,
                        "resolved_release_year": job.release_year_hint,
                        "resolved_score": STORED_ENTITY_SCORE_EL,
                        "hit_score": STORED_ENTITY_SCORE_EL,
                        "display_title": job.query_text,
                        "year": job.release_year_hint,
                        "score": STORED_ENTITY_SCORE_EL,
                        "country": None,
                        "origin": "hint",
                    }]

            elcinema_online, elc_disc = discover_elcinema_candidates(job.query_text, job.release_year_hint)
            bom_title_online, bom_release_online, bom_disc = discover_bom_candidates_bundle(
                job.query_text,
                job.release_year_hint,
            )

            # Hint candidates take priority over stored and online discovery results.
            elcinema_candidates = _merge_elcinema_candidates(hint_candidates + stored_elc + recent_raw_elc, elcinema_online)
            bom_title_candidates = _merge_bom_title_candidates(stored_bom_titles, bom_title_online)
            bom_release_candidates = _merge_bom_release_candidates(stored_bom_rels, bom_release_online)

            job.context_json["elcinema_candidates"] = elcinema_candidates
            job.context_json["bom_title_candidates"] = bom_title_candidates
            job.context_json["bom_release_candidates"] = bom_release_candidates
            job.context_json["processed_elcinema_work_ids"] = []
            job.context_json["processed_bom_title_urls"] = []
            job.context_json["processed_bom_release_urls"] = []
            dbg = job.context_json.setdefault("debug", {})
            dbg["discovery"] = {
                "elcinema": elc_disc,
                "bom": bom_disc,
                "stored_source_ids": stored_ids,
                "existing_film_id": existing_film_id,
                "recent_raw_elcinema_candidates": len(recent_raw_elc),
            }

            job.stage = _next_stage(job) or STAGE_FINALIZE
            job.updated_at = datetime.utcnow()
            _sanitize_lookup_job_json_fields(job)
            return

        if stage == STAGE_IMDB_RATINGS:
            if not job.imdb_title_id:
                job.stage = _next_stage(job) or STAGE_FINALIZE
                _sanitize_lookup_job_json_fields(job)
                return

            # Resolve film:
            resolved = None
            if job.resolved_film_id:
                resolved = session.get(Film, job.resolved_film_id)
            if not resolved:
                matches = _compute_fast_matches(session, job)
                for m in sorted(matches, key=lambda x: float(x.get("match_score") or 0.0), reverse=True):
                    if m.get("film_id"):
                        resolved = session.get(Film, m["film_id"])
                        break
            if not resolved:
                # Allow explicit release year hint; otherwise keep NULL.
                resolved = matcher.ensure_film(job.query_text, job.release_year_hint)

            # Fetch title-specific ratings (IMDb).
            metrics = run_imdb_daily(job.imdb_title_id, fallback_title=resolved.canonical_title)
            for metric in metrics:
                session.add(
                    RatingsMetric(
                        film_id=resolved.id,
                        source_name=metric.source_name,
                        rating_value=metric.rating_value,
                        vote_count=metric.vote_count,
                        popularity_rank=metric.popularity_rank,
                        metric_date=date.today(),
                        raw_payload_json=metric.payload,
                    )
                )

            # Attach the observed query alias so the lookup identity is explainable.
            matcher.attach_observed_titles(
                film_id=resolved.id,
                raw_title=job.query_text,
                raw_title_ar=None,
                source_entity_id=job.imdb_title_id,
                confidence=0.95,
                alias_type="source_title",
            )
            session.flush()
            job.resolved_film_id = resolved.id

            job.stage = _next_stage(job) or STAGE_FINALIZE
            _sanitize_lookup_job_json_fields(job)
            return

        if stage == STAGE_E_LCINEMA_TITLE:
            client = ElCinemaClient()
            cctx = job.context_json or {}
            discovered_candidates: list[dict[str, Any]] = list(cctx.get("elcinema_candidates") or [])
            processed_ids = set(cctx.get("processed_elcinema_work_ids") or [])
            id_to_parent: dict[str, float] = {}
            # Build a lookup from entity_id → candidate metadata so we can backfill titles
            # when parse_title_boxoffice fails to extract a title from the HTML.
            id_to_candidate: dict[str, dict[str, Any]] = {}
            for c in discovered_candidates:
                eid = str(c.get("entity_id") or c.get("work_id") or "")
                if eid:
                    id_to_parent[eid] = float(c.get("resolved_score") or c.get("score") or 0.0)
                    id_to_candidate[eid] = c

            ordered_work_ids = [
                str(c.get("entity_id"))
                for c in discovered_candidates
                if c.get("entity_id") and str(c.get("entity_id")) not in processed_ids
            ][:12]
            chart_extra: list[dict[str, Any]] = []
            if not ordered_work_ids:
                try:
                    chart_html = client.fetch_boxoffice_chart()
                    chart_rows = parse_current_chart(chart_html, f"{client.base_url}/en/boxoffice", chart_limit=100)
                    for r in chart_rows:
                        blob = (r.film_title_ar_raw or r.film_title_raw) or ""
                        # Score against BOTH title variants so Arabic-titled entries match Latin queries
                        lead = max(
                            score_candidate_hit(job.query_text, r.film_title_raw, job.release_year_hint),
                            score_candidate_hit(job.query_text, r.film_title_ar_raw, job.release_year_hint),
                        )
                        if lead < ELCINEMA_CHART_LEAD_MIN or not r.source_entity_id:
                            continue
                        wid = str(r.source_entity_id)
                        if wid in processed_ids or wid in ordered_work_ids:
                            continue
                        try:
                            html_en = client.fetch_work_root_en(wid)
                            html_ar = None
                            try:
                                html_ar = client.fetch_work_root_ar(wid)
                            except Exception:  # noqa: BLE001
                                html_ar = None
                            meta = parse_elcinema_work_metadata(
                                html_en,
                                html_ar,
                                wid,
                                f"{client.base_url}/en/work/{wid}/",
                            )
                            rs = max(
                                lead,
                                score_resolved_title(
                                    job.query_text,
                                    meta.get("title_en"),
                                    meta.get("title_ar"),
                                    meta.get("release_year"),
                                    job.release_year_hint,
                                ),
                            )
                            if rs < ELCINEMA_POST_META_MIN:
                                continue
                            ordered_work_ids.append(wid)
                            id_to_parent[wid] = rs
                            syn = {
                                "source": "elCinema",
                                "work_id": wid,
                                "entity_id": wid,
                                "url": meta.get("source_url"),
                                "source_url": meta.get("source_url"),
                                "matched_title": blob,
                                "resolved_title_en": meta.get("title_en"),
                                "resolved_title_ar": meta.get("title_ar"),
                                "resolved_release_year": meta.get("release_year"),
                                "resolved_score": rs,
                                "score": rs,
                            }
                            chart_extra.append(syn)
                        except Exception:  # noqa: BLE001
                            continue
                except Exception as exc:  # noqa: BLE001
                    _append_job_warning(job, {"type": "elcinema_discovery_fallback_error", "message": str(exc)})
            if chart_extra:
                discovered_candidates = discovered_candidates + chart_extra
                job.context_json = job.context_json or {}
                job.context_json["elcinema_candidates"] = discovered_candidates
            ordered_work_ids = list(dict.fromkeys(ordered_work_ids))[:12]

            records_to_ingest: list[Any] = []
            work_context: list[dict[str, Any]] = job.context_json.get("elcinema_work_context") if job.context_json else None
            if work_context is None:
                work_context = []
            rows_before_filter = 0

            for work_id in ordered_work_ids:
                parent_score = float(id_to_parent.get(work_id, 0.0))
                try:
                    html = client.fetch_title_boxoffice(work_id)
                    title_url = f"{client.base_url}/en/work/{work_id}/boxoffice"
                    detail_rows = parse_title_boxoffice(html, title_url, work_id)
                    rows_before_filter += len(detail_rows)

                    # If the boxoffice page couldn't extract a title, backfill from the
                    # confirmed candidate metadata so row_accept_with_parent has a real title.
                    if detail_rows:
                        cand_meta = id_to_candidate.get(work_id, {})
                        fallback_title = (
                            cand_meta.get("resolved_title_en")
                            or cand_meta.get("display_title")
                            or cand_meta.get("matched_title")
                            or job.query_text
                        )
                        for r in detail_rows:
                            if not r.film_title_raw:
                                r.film_title_raw = fallback_title

                    scored_detail: list[tuple[float, Any]] = []
                    for r in detail_rows:
                        row_s = score_resolved_title(
                            job.query_text,
                            r.film_title_raw,
                            r.film_title_ar_raw,
                            r.release_year_hint,
                            job.release_year_hint,
                        )
                        if not row_accept_with_parent(
                            job.query_text,
                            job.release_year_hint,
                            r.film_title_raw,
                            r.film_title_ar_raw,
                            r.release_year_hint,
                            parent_score,
                            strong_threshold=STRONG_THRESHOLD,
                            review_threshold=REVIEW_LOW_THRESHOLD,
                            moderate_threshold=0.60,
                        ):
                            continue
                        scored_detail.append((row_s, r))
                    scored_detail.sort(key=lambda x: x[0], reverse=True)
                    top_detail = scored_detail
                    for score, r in top_detail:
                        if score >= STRONG_THRESHOLD or parent_score >= STRONG_THRESHOLD:
                            r.parser_confidence = max(float(r.parser_confidence or 0), 0.92)
                            r.source_confidence = max(float(r.source_confidence or 0), 0.86)
                        else:
                            r.parser_confidence = max(float(r.parser_confidence or 0), 0.85)
                            r.source_confidence = max(float(r.source_confidence or 0), 0.80)
                        records_to_ingest.append(r)

                    if top_detail:
                        _best_score, best_r = top_detail[0]
                        work_context.append(
                            {
                                "work_id": work_id,
                                "film_title_raw": best_r.film_title_raw,
                                "film_title_ar_raw": best_r.film_title_ar_raw,
                                "release_year_hint": best_r.release_year_hint,
                            },
                        )
                except Exception as exc:  # noqa: BLE001
                    _append_job_warning(
                        job,
                        {
                            "type": "elcinema_title_lookup_error",
                            "message": f"elCinema title fetch failed for work_id={work_id}: {exc}",
                        },
                    )

            job.context_json = job.context_json or {}
            dbg = job.context_json.setdefault("debug", {})
            dbg["stages"] = dbg.get("stages") or {}
            dbg["stages"]["elcinema_title"] = {
                "ordered_work_ids": ordered_work_ids,
                "rows_fetched": rows_before_filter,
                "rows_after_filter": len(records_to_ingest),
            }

            if records_to_ingest:
                _append_job_note(job, f"elCinema title lookup ingested {len(records_to_ingest)} title rows across {len(ordered_work_ids)} work_ids.")
                ingest_source(session, "elcinema", "lookup_elcinema_title_specific", records_to_ingest)
                session.flush()
                _promote_from_live_evidence(session, job, stage=STAGE_E_LCINEMA_TITLE, matcher=matcher)
                job.context_json["elcinema_work_context"] = work_context
                job.context_json["processed_elcinema_work_ids"] = list(processed_ids.union(set(ordered_work_ids)))
            else:
                _append_job_warning(job, {"type": "elcinema_title_lookup_empty", "message": "No elCinema title rows matched the query."})

            job.stage = _next_stage(job) or STAGE_FINALIZE
            _sanitize_lookup_job_json_fields(job)
            return

        if stage == STAGE_E_LCINEMA_RELEASED:
            coverage = job.coverage_json or {}
            # Even if title coverage is strong, released markets are “enrichment”; run if we have work context.
            work_context: list[dict[str, Any]] = (job.context_json or {}).get("elcinema_work_context") or []
            if not work_context:
                _append_job_note(job, "No elCinema work context; skipping released-market enrichment.")
                job.stage = _next_stage(job) or STAGE_FINALIZE
                _sanitize_lookup_job_json_fields(job)
                return

            records_to_ingest: list[Any] = []
            for ctx in work_context:
                work_id = ctx.get("work_id")
                if not work_id:
                    continue
                try:
                    recs = run_elcinema_title_released_markets(
                        work_id=str(work_id),
                        film_title_raw=str(ctx.get("film_title_raw") or job.query_text),
                        film_title_ar_raw=ctx.get("film_title_ar_raw"),
                        release_year_hint=ctx.get("release_year_hint") or job.release_year_hint,
                    )
                    # Don’t ingest empty.
                    records_to_ingest.extend(recs)
                except Exception as exc:  # noqa: BLE001
                    _append_job_warning(job, {"type": "elcinema_released_error", "message": f"Released fetch failed for work_id={work_id}: {exc}"})

            if records_to_ingest:
                _append_job_note(job, f"elCinema released-markets enrichment ingested {len(records_to_ingest)} rows.")
                ingest_source(session, "elcinema", "lookup_elcinema_released_markets", records_to_ingest)
                session.flush()
                _promote_from_live_evidence(session, job, stage=STAGE_E_LCINEMA_RELEASED, matcher=matcher)
            else:
                _append_job_warning(job, {"type": "elcinema_released_empty", "message": "No elCinema released-market rows matched/parsed."})

            job.stage = _next_stage(job) or STAGE_FINALIZE
            _sanitize_lookup_job_json_fields(job)
            return

        if stage == STAGE_BOM_TITLE:
            cctx = job.context_json or {}
            processed_urls = set(cctx.get("processed_bom_title_urls") or [])
            title_candidates = cctx.get("bom_title_candidates") or []
            to_process = [c for c in title_candidates if c.get("url") and str(c.get("url")) not in processed_urls][:5]
            title_urls = [str(c.get("url")) for c in to_process]

            client = BomClient()
            records_to_ingest: list[Any] = []
            prior_releases: list[dict[str, Any]] = list(cctx.get("bom_release_candidates") or [])
            new_release_rows: list[dict[str, Any]] = []
            job.context_json = job.context_json or {}
            dbg = job.context_json.setdefault("debug", {})
            dbg["stages"] = dbg.get("stages") or {}
            dbg["stages"]["bom_title"] = {"title_urls": title_urls, "rows_ingested": 0}

            for cand in to_process:
                title_url = str(cand.get("url"))
                parent_res = float(cand.get("resolved_score") or cand.get("score") or 0.0)
                try:
                    html = client.fetch_title_page(title_url)
                    title_info = parse_title_page_candidate(html, title_url)
                    page_score = score_resolved_title(
                        job.query_text,
                        title_info.get("title_en"),
                        None,
                        title_info.get("release_year"),
                        job.release_year_hint,
                    )
                    gate_score = max(parent_res, page_score)
                    if gate_score < REVIEW_LOW_THRESHOLD:
                        continue
                    if title_info.get("intl_gross_usd") is not None:
                        records_to_ingest.append(
                            ExtractedRecord(
                                source_name="Box Office Mojo",
                                source_url=title_url,
                                source_entity_id=title_url,
                                country_code=None,
                                film_title_raw=title_info.get("title_en") or job.query_text,
                                film_title_ar_raw=None,
                                release_year_hint=title_info.get("release_year") or job.release_year_hint,
                                record_scope="title",
                                record_granularity="lifetime",
                                record_semantics="title_cumulative_total",
                                evidence_type="title_performance",
                                period_label_raw="lifetime",
                                period_start_date=None,
                                period_end_date=None,
                                period_key="lifetime",
                                rank=None,
                                period_gross_local=None,
                                cumulative_gross_local=float(title_info["intl_gross_usd"]),
                                currency="USD",
                                admissions_actual=None,
                                parser_confidence=0.82,
                                source_confidence=0.62,
                                notes="BOM title page international total",
                                raw_payload_json=title_info,
                            ),
                        )
                    pt = title_info.get("title_en") or cand.get("resolved_title_en")
                    py = title_info.get("release_year") or cand.get("resolved_release_year")
                    for rel in title_info.get("release_urls") or []:
                        ru = normalize_bom_release_url(rel)
                        if not ru:
                            continue
                        new_release_rows.append(
                            {
                                "source": "Box Office Mojo",
                                "entity_id": ru,
                                "url": ru,
                                "release_url": ru,
                                "title_url": title_url,
                                "resolved_title_en": pt,
                                "resolved_release_year": py,
                                "resolved_score": gate_score,
                                "display_title": pt or "",
                                "year": py or job.release_year_hint,
                                "score": gate_score,
                                "country": None,
                            },
                        )
                except Exception as exc:  # noqa: BLE001
                    _append_job_warning(job, {"type": "bom_title_lookup_error", "message": f"title_url={title_url}: {exc}"})

            dbg["stages"]["bom_title"]["rows_ingested"] = len(records_to_ingest)
            merged_releases = _merge_bom_release_candidates(prior_releases, new_release_rows)
            job.context_json["bom_release_candidates"] = merged_releases
            job.context_json["processed_bom_title_urls"] = list(processed_urls.union(set(title_urls)))

            if records_to_ingest:
                _append_job_note(job, f"BOM title lookup ingested {len(records_to_ingest)} title page rows.")
                ingest_source(session, "bom", "lookup_bom_title_filtered", records_to_ingest)
                session.flush()
                _promote_from_live_evidence(session, job, stage=STAGE_BOM_TITLE, matcher=matcher)
            else:
                _append_job_warning(job, {"type": "bom_title_lookup_empty", "message": "No Box Office Mojo title evidence matched/parsed for the query."})

            job.stage = _next_stage(job) or STAGE_FINALIZE
            _sanitize_lookup_job_json_fields(job)
            return

        if stage == STAGE_BOM_RELEASES:
            cctx = job.context_json or {}
            processed = set(cctx.get("processed_bom_release_urls") or [])
            release_entries = [c for c in (cctx.get("bom_release_candidates") or []) if c.get("url") and str(c.get("url")) not in processed][:8]
            release_urls = [str(c.get("url")) for c in release_entries]
            client = BomClient()
            records_to_ingest: list[Any] = []
            raw_row_total = 0
            job.context_json = job.context_json or {}
            dbg = job.context_json.setdefault("debug", {})
            dbg["stages"] = dbg.get("stages") or {}
            dbg["stages"]["bom_release"] = {"release_urls": release_urls, "rows_before_filter": 0, "rows_ingested": 0}

            for rel_cand in release_entries:
                release_url = str(rel_cand.get("url"))
                parent_sc = float(rel_cand.get("resolved_score") or rel_cand.get("score") or 0.0)
                try:
                    html = client.fetch_release_page(release_url)
                    rel_rows = parse_release_page_evidence(
                        html,
                        release_url,
                        job.query_text,
                        job.release_year_hint,
                        parent_resolved_score=parent_sc,
                    )
                    raw_row_total += len(rel_rows)
                    records_to_ingest.extend(rel_rows)
                except Exception as exc:  # noqa: BLE001
                    _append_job_warning(job, {"type": "bom_release_lookup_error", "message": f"release_url={release_url}: {exc}"})

            dbg["stages"]["bom_release"]["rows_before_filter"] = raw_row_total
            dbg["stages"]["bom_release"]["rows_ingested"] = len(records_to_ingest)

            if records_to_ingest:
                ingest_source(session, "bom", "lookup_bom_release_enrichment", records_to_ingest)
                session.flush()
                _promote_from_live_evidence(session, job, stage=STAGE_BOM_RELEASES, matcher=matcher)
            else:
                _append_job_warning(job, {"type": "bom_release_lookup_empty", "message": "No BOM release page evidence matched/parsed for query."})
            job.context_json["processed_bom_release_urls"] = list(processed.union(set(release_urls)))
            job.stage = _next_stage(job) or STAGE_FINALIZE
            _sanitize_lookup_job_json_fields(job)
            return

        if stage == STAGE_SUPPLEMENTAL:
            # Supplemental charts: elCinema current chart only (from legacy GS).
            client = ElCinemaClient()
            chart_html = client.fetch_boxoffice_chart()
            chart_rows = parse_current_chart(chart_html, f"{client.base_url}/en/boxoffice", chart_limit=100)

            records_to_ingest: list[Any] = []
            for r in chart_rows:
                score = score_resolved_title(
                    job.query_text,
                    r.film_title_raw,
                    r.film_title_ar_raw,
                    r.release_year_hint,
                    job.release_year_hint,
                )
                if score >= REVIEW_LOW_THRESHOLD:
                    # Supplemental confidence bands (legacy GS used <= 0.80 for chart).
                    r.parser_confidence = max(float(r.parser_confidence or 0), 0.80)
                    r.source_confidence = max(float(r.source_confidence or 0), 0.80)
                    records_to_ingest.append(r)

            if records_to_ingest:
                _append_job_note(job, f"Supplemental charts ingested {len(records_to_ingest)} elCinema chart title rows.")
                ingest_source(session, "elcinema", "lookup_elcinema_chart_supplemental", records_to_ingest)
                session.flush()
                _promote_from_live_evidence(session, job, stage=STAGE_SUPPLEMENTAL, matcher=matcher)
            else:
                _append_job_warning(job, {"type": "supplemental_charts_empty", "message": "No supplemental chart rows matched the query."})

            job.stage = _next_stage(job) or STAGE_FINALIZE
            _sanitize_lookup_job_json_fields(job)
            return

        if stage == STAGE_FINALIZE:
            # Choose resolved film using freshly computed matches (post enrichment).
            matches_after = _compute_fast_matches(session, job)
            coverage = _coverage_from_matches(matches_after)
            job.coverage_json = coverage

            best_match = sorted(matches_after, key=lambda x: float(x.get("match_score") or 0.0), reverse=True)
            job.context_json = job.context_json or {}
            dbg = job.context_json.setdefault("debug", {})
            lp = job.context_json.get("live_promotion") or {}

            promoted_film: Film | None = None
            if job.resolved_film_id:
                promoted_film = session.get(Film, job.resolved_film_id)
            if not promoted_film and lp.get("film_id"):
                promoted_film = session.get(Film, lp["film_id"])

            if promoted_film:
                job.resolved_film_id = promoted_film.id
                created_from_promo = bool(lp.get("created_new_film"))
                matcher.attach_observed_titles(
                    film_id=promoted_film.id,
                    raw_title=job.query_text,
                    raw_title_ar=job.query_text if contains_arabic(job.query_text) else None,
                    source_entity_id=f"lookup:{job.id}",
                    confidence=0.95,
                    alias_type="query_title",
                )
                title_variants = {str(m.get("title") or "").strip() for m in best_match[:12] if str(m.get("title") or "").strip()}
                for t in title_variants:
                    matcher.attach_observed_titles(
                        film_id=promoted_film.id,
                        raw_title=t,
                        raw_title_ar=t if contains_arabic(t) else None,
                        source_entity_id=f"lookup:{job.id}",
                        confidence=0.85,
                        alias_type="source_title",
                    )
                job.fast_matches_json = matches_after
                job.status = "completed"
                job.is_active = False
                job.completed_at = datetime.utcnow()
                job.stage = STAGE_FINALIZE
                if created_from_promo:
                    _append_job_note(job, "New film auto-created from strong live evidence during finalize.")
                relinked = _relink_recent_unmatched_title_rows(
                    session,
                    film_id=promoted_film.id,
                    query_text=job.query_text,
                    release_year_hint=job.release_year_hint,
                )
                if relinked:
                    _append_job_note(job, f"Relinked {relinked} recent unmatched title row(s) to resolved film.")
                dbg["finalize"] = {
                    "used_live_promotion": True,
                    "resolved_film_id": str(promoted_film.id),
                    "canonical_title": promoted_film.canonical_title,
                }
                _sanitize_lookup_job_json_fields(job)
                return

            resolved_film: Film | None = None
            best_score: float = 0.0
            top_existing = [m for m in best_match if m.get("film_id")]
            if top_existing:
                top = top_existing[0]
                best_score = float(top.get("match_score") or 0.0)
                resolved_film = session.get(Film, top["film_id"])
                if len(top_existing) > 1:
                    second = top_existing[1]
                    second_score = float(second.get("match_score") or 0.0)
                    if best_score >= REVIEW_LOW_THRESHOLD and abs(best_score - second_score) <= AUTO_RESOLVE_MARGIN and top.get("film_id") != second.get("film_id"):
                        resolved_film = None
                        _get_or_create_review_item(
                            session,
                            raw_evidence_id=top.get("raw_evidence_id"),
                            film_title_raw=top.get("title") or job.query_text,
                            release_year_hint=job.release_year_hint,
                            candidate_film_id=top.get("film_id"),
                            candidate_score=best_score,
                            reason="lookup_finalize_ambiguous_close_scores",
                        )

            # If we got an IMDb-only resolution but no stored evidence links, keep it.
            if not resolved_film and job.resolved_film_id:
                resolved_film = session.get(Film, job.resolved_film_id)

            created_new = False
            if not resolved_film:
                live_titles = [m for m in best_match if float(m.get("match_score") or 0.0) >= STRONG_THRESHOLD]
                if live_titles:
                    seed = live_titles[0]
                    canonical_title = str(seed.get("title") or job.query_text).strip()
                    resolved_film = matcher.ensure_film(canonical_title, job.release_year_hint or seed.get("release_year_hint"))
                    resolved_film.identity_confidence = Decimal(str(min(0.95, max(0.80, float(seed.get("match_score") or 0.84)))))
                    created_new = True

            if resolved_film:
                job.resolved_film_id = resolved_film.id
                matcher.attach_observed_titles(
                    film_id=resolved_film.id,
                    raw_title=job.query_text,
                    raw_title_ar=job.query_text if contains_arabic(job.query_text) else None,
                    source_entity_id=f"lookup:{job.id}",
                    confidence=0.95,
                    alias_type="query_title",
                )
                # attach top evidence titles as aliases without duplicates
                title_variants = {str(m.get("title") or "").strip() for m in best_match[:12] if str(m.get("title") or "").strip()}
                for t in title_variants:
                    matcher.attach_observed_titles(
                        film_id=resolved_film.id,
                        raw_title=t,
                        raw_title_ar=t if contains_arabic(t) else None,
                        source_entity_id=f"lookup:{job.id}",
                        confidence=0.85,
                        alias_type="source_title",
                    )

                job.fast_matches_json = matches_after

                job.status = "completed"
                job.is_active = False
                job.completed_at = datetime.utcnow()
                job.stage = STAGE_FINALIZE
                if created_new:
                    _append_job_note(job, "New film auto-created from strong live evidence during finalize.")
                relinked = _relink_recent_unmatched_title_rows(
                    session,
                    film_id=resolved_film.id,
                    query_text=job.query_text,
                    release_year_hint=job.release_year_hint,
                )
                if relinked:
                    _append_job_note(job, f"Relinked {relinked} recent unmatched title row(s) to resolved film.")
            else:
                _append_job_warning(job, {"type": "finalize_unresolved", "message": "Lookup could not resolve a film identity from available evidence."})
                job.status = "completed"
                job.is_active = False
                job.completed_at = datetime.utcnow()
                job.stage = STAGE_FINALIZE
            _sanitize_lookup_job_json_fields(job)
            return

        # Unknown stage: mark failed.
        job.status = "failed"
        job.is_active = False
        _append_job_warning(job, {"type": "unknown_stage", "message": f"Unknown stage: {stage}"})
        _sanitize_lookup_job_json_fields(job)
    except Exception as exc:  # noqa: BLE001
        # Postgres will keep the current transaction “aborted” after a DB error
        # until we explicitly rollback; without this, future queries in the same
        # Streamlit request can fail even if the logical error was localized.
        message = str(exc)
        session.rollback()

        # Reload job after rollback so we can safely persist failure state.
        job = session.execute(select(LookupJob).where(LookupJob.id == job_id)).scalar_one_or_none()
        if job:
            job.status = "failed"
            job.is_active = False
            warnings_list = (make_json_safe(job.warnings_json or {}).get("warnings") or [])
            warnings_list.append(
                make_json_safe(
                    {
                        "type": "lookup_failed_exception",
                        "message": str(message)[:500],
                        "stage": str(job.stage or ""),
                        "exception_class": exc.__class__.__name__,
                    }
                )
            )
            job.warnings_json = make_json_safe({"warnings": warnings_list})
            job.completed_at = job.completed_at or job.updated_at
            job.updated_at = datetime.utcnow()
            _sanitize_lookup_job_json_fields(job)
            session.flush()

