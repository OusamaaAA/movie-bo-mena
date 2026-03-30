"""Ratings aggregator: fetches film ratings from IMDb, elCinema, and Letterboxd."""
from __future__ import annotations

import json
import re
from datetime import date
from urllib.parse import quote_plus, unquote

import httpx
from bs4 import BeautifulSoup


# ── slug / ID helpers ──────────────────────────────────────────────────────────

def _title_to_letterboxd_slug(title: str) -> str:
    """Convert a film title to a Letterboxd-style slug.
    'Siko Siko' → 'siko-siko'
    "The Wild Robot" → 'the-wild-robot'
    """
    slug = title.lower()
    slug = re.sub(r"[''`ʻʼ]", "", slug)           # apostrophes
    slug = re.sub(r"[^\w\s-]", " ", slug)          # other punctuation → space
    slug = re.sub(r"[\s_]+", "-", slug.strip())    # spaces → hyphens
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def _fuzzy_score(a: str, b: str) -> float:
    """Simple token-overlap fuzzy score between 0 and 1."""
    from rapidfuzz import fuzz
    return fuzz.ratio(a.lower().strip(), b.lower().strip()) / 100.0


def _discover_search_hits(queries: list[str]) -> list[dict[str, str]]:
    """Web-search helper (Bing RSS + DuckDuckGo HTML) for URL discovery."""
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    for q in queries:
        if not q or not q.strip():
            continue
        try:
            rss = httpx.get(
                f"https://www.bing.com/search?format=rss&q={quote_plus(q)}",
                timeout=12.0,
                headers=headers,
                follow_redirects=True,
            ).text
            items = re.findall(r"<item\b[\s\S]*?</item>", rss, flags=re.I)
            for item in items:
                link_m = re.search(r"<link>([\s\S]*?)</link>", item, flags=re.I)
                title_m = re.search(r"<title>([\s\S]*?)</title>", item, flags=re.I)
                desc_m = re.search(r"<description>([\s\S]*?)</description>", item, flags=re.I)
                link = re.sub(r"\s+", " ", link_m.group(1)).strip() if link_m else ""
                title = re.sub(r"\s+", " ", title_m.group(1)).strip() if title_m else ""
                snippet = re.sub(r"<[^>]+>", " ", desc_m.group(1)) if desc_m else ""
                snippet = re.sub(r"\s+", " ", snippet).strip()
                if link and link not in seen:
                    seen.add(link)
                    out.append({"url": link, "title": title, "snippet": snippet})
        except Exception:  # noqa: BLE001
            pass
        try:
            html = httpx.get(
                f"https://html.duckduckgo.com/html/?q={quote_plus(q)}",
                timeout=12.0,
                headers=headers,
                follow_redirects=True,
            ).text
            for a in re.findall(
                r'<a[^>]+class="[^"]*(?:result__a|result-link)[^"]*"[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>',
                html,
                flags=re.I,
            ):
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
    return out


def _extract_imdb_rating_from_text(text: str) -> tuple[float | None, int | None]:
    """Extract IMDb score/votes from search snippets when IMDb blocks scraping."""
    blob = re.sub(r"\s+", " ", text or "").strip()
    if not blob:
        return None, None
    score: float | None = None
    votes: int | None = None
    for pat in (
        r"IMDb(?:\s+rating)?[^\d]{0,16}([0-9](?:\.[0-9])?)\s*/\s*10",
        r"rating[^\d]{0,12}([0-9](?:\.[0-9])?)\s*/\s*10",
    ):
        m = re.search(pat, blob, flags=re.I)
        if m:
            try:
                score = float(m.group(1))
                break
            except ValueError:
                pass
    vm = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*([KMB])?\s+(?:votes|ratings?)", blob, flags=re.I)
    if vm:
        try:
            base = float(vm.group(1).replace(",", ""))
            suffix = (vm.group(2) or "").upper()
            mult = 1
            if suffix == "K":
                mult = 1_000
            elif suffix == "M":
                mult = 1_000_000
            elif suffix == "B":
                mult = 1_000_000_000
            votes = int(base * mult)
        except ValueError:
            votes = None
    if votes is None:
        vm2 = re.search(r"\b([0-9][0-9,]{2,})\s+(?:votes|ratings?)", blob, flags=re.I)
        if vm2:
            try:
                votes = int(vm2.group(1).replace(",", ""))
            except ValueError:
                votes = None
    return score, votes


def _parse_compact_count(raw: str | None) -> int | None:
    txt = (raw or "").strip()
    if not txt:
        return None
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMB]?)\s*$", txt.replace(",", ""), flags=re.I)
    if not m:
        return None
    try:
        base = float(m.group(1))
    except ValueError:
        return None
    suffix = (m.group(2) or "").upper()
    mult = 1
    if suffix == "K":
        mult = 1_000
    elif suffix == "M":
        mult = 1_000_000
    elif suffix == "B":
        mult = 1_000_000_000
    return int(base * mult)


def _fetch_imdb_vote_count_from_ratings_page(imdb_id: str) -> int | None:
    """
    Try the IMDb ratings subpage for vote count.
    This may work on some networks where the main page parsing is blocked.
    """
    urls = [
        f"https://www.imdb.com/title/{imdb_id}/ratings/?ref_=tt_ov_rat",
        f"https://www.imdb.com/title/{imdb_id}/ratings/",
    ]
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for u in urls:
        try:
            html = httpx.get(u, timeout=12.0, headers=headers, follow_redirects=True).text
        except Exception:  # noqa: BLE001
            continue
        if not html:
            continue
        # Common patterns on ratings page and embedded payloads.
        for pat in (
            r'"voteCount"\s*:\s*([0-9,]+)',
            r'"ratingCount"\s*:\s*([0-9,]+)',
            r'([0-9]+(?:\.[0-9]+)?)\s*([KMB]?)\s+votes',
        ):
            m = re.search(pat, html, flags=re.I)
            if not m:
                continue
            if m.lastindex and m.lastindex >= 2 and m.group(2) is not None:
                cnt = _parse_compact_count(f"{m.group(1)}{m.group(2)}")
            else:
                cnt = _parse_compact_count(m.group(1))
            if cnt:
                return cnt
    return None


# ── IMDb ───────────────────────────────────────────────────────────────────────

def search_imdb_id(title: str, year: int | None = None) -> tuple[str | None, str]:
    """
    Search IMDb suggestion API and return (best imdb_id, status_message).
    """
    try:
        from src.sources.imdb.client import ImdbClient
        results = ImdbClient().search_suggestions(title)
        if not results:
            return None, "suggestion API returned no results"
        # Score by title similarity + year bonus
        best = None
        best_score = -1.0
        for c in results:
            score = _fuzzy_score(title, c.get("title", ""))
            if year and c.get("year") == year:
                score += 0.15
            if score > best_score:
                best_score = score
                best = c
        if best and best_score >= 0.3:
            return best["imdb_id"], f"found via title search (score {best_score:.2f})"
        return None, f"best match score {best_score:.2f} below threshold"
    except Exception as exc:  # noqa: BLE001
        return None, f"search error: {exc!s:.120}"


def fetch_imdb_rating(
    title: str,
    year: int | None = None,
    *,
    imdb_id: str | None = None,
) -> tuple[dict | None, str]:
    """
    Fetch IMDb rating. Returns (result_dict | None, status_message).
    Uses imdb_id directly if given, else searches by title.
    """
    resolved_id = imdb_id
    search_msg = ""

    if not resolved_id:
        resolved_id, search_msg = search_imdb_id(title, year)
        if not resolved_id:
            return None, f"IMDb ID not found — {search_msg}"

    try:
        from src.sources.imdb.ingest import run_imdb_daily
        metrics = run_imdb_daily(resolved_id, fallback_title=title)
        if metrics and metrics[0].rating_value is not None:
            m = metrics[0]
            vote_count = m.vote_count
            if vote_count is None:
                vote_count = _fetch_imdb_vote_count_from_ratings_page(resolved_id)
            return {
                "source": "IMDb",
                "imdb_id": resolved_id,
                "rating": float(m.rating_value),
                "rating_scale": 10,
                "vote_count": vote_count,
                "url": f"https://www.imdb.com/title/{resolved_id}/",
            }, f"fetched {resolved_id}" + (f" {search_msg}" if search_msg else "")
        # IMDb sometimes returns anti-bot shells for server-side fetches.
        # Fallback to web search snippets for the same title URL.
        if not metrics or metrics[0].rating_value is None:
            # IMDb sometimes returns anti-bot shells for server-side fetches.
            # Fallback to web search snippets for the same title URL.
            snippet_queries = [
                f"site:imdb.com/title/{resolved_id}/ IMDb rating",
                f'site:imdb.com "{resolved_id}" "{title}" rating',
            ]
            if year:
                snippet_queries.insert(0, f'site:imdb.com "{resolved_id}" "{title}" {year} rating')
            discovered = _discover_search_hits(snippet_queries)
            for hit in discovered:
                combo = f"{hit.get('title', '')} {hit.get('snippet', '')}"
                rating, votes = _extract_imdb_rating_from_text(combo)
                if rating is None:
                    continue
                return {
                    "source": "IMDb",
                    "imdb_id": resolved_id,
                    "rating": float(rating),
                    "rating_scale": 10,
                    "vote_count": votes,
                    "url": f"https://www.imdb.com/title/{resolved_id}/",
                }, "fetched via search-snippet fallback (IMDb direct page blocked)"
            # Last resort: pull IMDb rating mirror by title ID.
            try:
                meta = httpx.get(
                    f"https://cinemeta-live.strem.io/meta/movie/{resolved_id}.json",
                    timeout=12.0,
                    follow_redirects=True,
                ).json().get("meta", {})
                mirror_rating = meta.get("imdbRating")
                if mirror_rating is not None:
                    mirror_votes = _fetch_imdb_vote_count_from_ratings_page(resolved_id)
                    return {
                        "source": "IMDb",
                        "imdb_id": resolved_id,
                        "rating": float(mirror_rating),
                        "rating_scale": 10,
                        "vote_count": mirror_votes,
                        "url": f"https://www.imdb.com/title/{resolved_id}/",
                    }, "fetched via IMDb metadata mirror (direct page blocked)"
            except Exception:  # noqa: BLE001
                pass
            return None, f"IMDb page for {resolved_id} has no parseable rating yet"
    except Exception as exc:  # noqa: BLE001
        return None, f"IMDb fetch error for {resolved_id}: {exc!s:.200}"


# ── elCinema ───────────────────────────────────────────────────────────────────

def search_elcinema_work_id(title: str, year: int | None = None) -> tuple[str | None, str]:
    """Search elCinema and return (work_id | None, status_message)."""
    try:
        from src.sources.elcinema.client import ElCinemaClient
        from src.sources.elcinema.parser import parse_elcinema_search_results
        html = ElCinemaClient().search_works(title)
        candidates = parse_elcinema_search_results(html)
        if not candidates:
            # Fallback: web discovery for work pages (same spirit as acquisition lookup).
            q = [f'site:elcinema.com/en/work/ "{title}"']
            if year:
                q.insert(0, f'site:elcinema.com/en/work/ "{title}" {year}')
            hits = _discover_search_hits(q)
            discovered: list[dict] = []
            seen: set[str] = set()
            for h in hits:
                m = re.search(r"elcinema\.com/(?:en/)?work/(\d+)", h.get("url", ""), flags=re.I)
                if not m:
                    continue
                wid = m.group(1)
                if wid in seen:
                    continue
                seen.add(wid)
                discovered.append({"work_id": wid, "title": h.get("title", ""), "year": year})
            candidates = discovered
        # Same high-recall path as acquisition lookup (DDG + jina proxy).
        try:
            from src.services.acquisition_lookup_job import (
                _expand_discovery_query_variants,
                _organic_elcinema_work_ids_via_proxy,
            )

            seen_ids = {str(c.get("work_id") or "") for c in candidates if c.get("work_id")}
            for wid in _organic_elcinema_work_ids_via_proxy(
                title,
                year,
                _expand_discovery_query_variants(title),
            ):
                if wid in seen_ids:
                    continue
                seen_ids.add(wid)
                candidates.append({"work_id": wid, "title": title, "year": year})
        except Exception:  # noqa: BLE001
            pass
        if not candidates:
            return None, "search returned no work candidates"
        best = None
        best_score = -1.0
        for c in candidates:
            score = _fuzzy_score(title, c.get("title", ""))
            if year and c.get("year") == year:
                score += 0.15
            if score > best_score:
                best_score = score
                best = c
        # Keep this stricter than IMDb because elCinema homepage search is noisy.
        if best and best_score >= 0.55:
            return best["work_id"], f"found via title search (score {best_score:.2f})"
        return None, f"best match score {best_score:.2f} below threshold"
    except Exception as exc:  # noqa: BLE001
        return None, f"search error: {exc!s:.120}"


def _parse_rating_from_html(html: str, source_label: str) -> tuple[float | None, int | None]:
    """
    Generic rating extractor: tries JSON-LD aggregateRating, then common patterns.
    Returns (rating_value, vote_count).
    """
    soup = BeautifulSoup(html, "lxml")

    # 1. JSON-LD aggregateRating (works for Letterboxd, elCinema, and IMDb)
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            raw = (script.string or "").strip()
            # Some pages wrap JSON-LD in comments/CDATAs.
            raw = re.sub(r"^\s*/\*\s*<!\[CDATA\[\s*\*/", "", raw)
            raw = re.sub(r"/\*\s*\]\]>\s*\*/\s*$", "", raw)
            raw = raw.strip()
            if not raw:
                continue
            data = json.loads(raw)
            candidates = data if isinstance(data, list) else [data]
            for entry in candidates:
                if not isinstance(entry, dict):
                    continue
                agg = entry.get("aggregateRating") or {}
                rv = agg.get("ratingValue")
                rc = agg.get("ratingCount") or agg.get("reviewCount")
                if rv is not None:
                    try:
                        rating_value = float(str(rv).replace(",", ""))
                    except ValueError:
                        continue
                    vote_count = None
                    if rc is not None:
                        try:
                            vote_count = int(str(rc).replace(",", ""))
                        except ValueError:
                            vote_count = None
                    return rating_value, vote_count
        except Exception:  # noqa: BLE001
            pass

    # 2. <meta> tags
    for name_attr in ("twitter:data2", "rating"):
        meta = soup.find("meta", attrs={"name": name_attr}) or soup.find("meta", attrs={"property": name_attr})
        if meta:
            content = meta.get("content", "")
            m = re.search(r"([\d.]+)", content)
            if m:
                try:
                    return float(m.group(1)), None
                except ValueError:
                    pass

    # 3. Inline "X.X/10" or "X.X out of 10"
    text = soup.get_text(" ", strip=True)
    for pattern in (r"([\d.]+)\s*/\s*10", r"([\d.]+)\s+out\s+of\s+10"):
        m = re.search(pattern, text, re.I)
        if m:
            try:
                return float(m.group(1)), None
            except ValueError:
                pass

    # 4. elCinema-specific stars widget patterns:
    #    title=" : 8.2" and chart labels like "500 Votes"
    if source_label.lower() == "elcinema":
        legend = soup.select_one(".stars-rating-lg .legend, .stars-rating-sm .legend")
        if legend:
            lm = re.search(r"([0-9](?:\.[0-9])?)", legend.get_text(" ", strip=True))
            if lm:
                rating = float(lm.group(1))
                v = re.search(r"([0-9][0-9,]{1,})\s+Votes", html, flags=re.I)
                votes = int(v.group(1).replace(",", "")) if v else None
                return rating, votes
        m = re.search(r'title="\s*[:\-]?\s*([0-9](?:\.[0-9])?)\s*"', html, flags=re.I)
        rating = float(m.group(1)) if m else None
        v = re.search(r"([0-9][0-9,]{1,})\s+Votes", html, flags=re.I)
        votes = int(v.group(1).replace(",", "")) if v else None
        if rating is not None:
            return rating, votes

    if source_label.lower() == "letterboxd":
        # Robust fallback when JSON-LD is wrapped/non-parseable.
        m = re.search(r'"ratingValue"\s*:\s*([0-9]+(?:\.[0-9]+)?)', html, flags=re.I)
        if m:
            try:
                rating = float(m.group(1))
            except ValueError:
                rating = None
            c = re.search(r'"ratingCount"\s*:\s*([0-9,]+)', html, flags=re.I)
            votes = int(c.group(1).replace(",", "")) if c else None
            if rating is not None:
                return rating, votes

    return None, None


def fetch_elcinema_rating(
    title: str,
    year: int | None = None,
    *,
    work_id: str | None = None,
) -> tuple[dict | None, str]:
    """
    Fetch elCinema rating. Returns (result | None, status_message).
    Tries the stats page first, falls back to the main work page.
    """
    resolved_id = work_id
    search_msg = ""

    if not resolved_id:
        resolved_id, search_msg = search_elcinema_work_id(title, year)
        if not resolved_id:
            return None, f"elCinema work ID not found — {search_msg}"

    try:
        from src.sources.elcinema.client import ElCinemaClient
        client = ElCinemaClient()
        base = client.base_url

        # Try stats page first, then main work page
        pages_to_try = [
            (f"/en/work/{resolved_id}/stats", "stats page"),
            (f"/en/work/{resolved_id}/", "work page"),
        ]
        for path, page_label in pages_to_try:
            try:
                html = client.get(path)
                rating, count = _parse_rating_from_html(html, "elCinema")
                if rating is not None:
                    return {
                        "source": "elCinema",
                        "work_id": resolved_id,
                        "rating": rating,
                        "rating_scale": 10,
                        "vote_count": count,
                        "url": f"{base}/en/work/{resolved_id}/stats",
                    }, f"found on {page_label}" + (f" {search_msg}" if search_msg else "")
            except Exception:  # noqa: BLE001
                continue

        return None, f"no rating found on elCinema stats or work page for {resolved_id}"
    except Exception as exc:  # noqa: BLE001
        return None, f"elCinema fetch error: {exc!s:.200}"


# ── Letterboxd ─────────────────────────────────────────────────────────────────

def fetch_letterboxd_rating(
    title: str,
    year: int | None = None,
    *,
    slug: str | None = None,
) -> tuple[dict | None, str]:
    """
    Fetch Letterboxd rating. Returns (result | None, status_message).
    Uses slug directly if given; otherwise generates candidates from the title
    (no search — Letterboxd search is blocked, but direct film pages work).
    """
    try:
        from src.sources.letterboxd.client import LetterboxdClient
        client = LetterboxdClient()

        # Build the list of slugs to try
        slugs_to_try: list[tuple[str, str]] = []
        if slug:
            slugs_to_try.append((slug, "provided slug"))
        else:
            base_slug = _title_to_letterboxd_slug(title)
            slugs_to_try.append((base_slug, f"slug from title"))
            if year:
                slugs_to_try.append((f"{base_slug}-{year}", f"slug+year"))
            # Discovery fallback via web search (Letterboxd search endpoint is commonly 403).
            query_variants = [f'site:letterboxd.com/film/ "{title}" letterboxd']
            if year:
                query_variants.insert(0, f'site:letterboxd.com/film/ "{title}" {year} letterboxd')
            slug_candidates: list[tuple[float, str]] = []
            for hit in _discover_search_hits(query_variants):
                url = hit.get("url", "")
                m = re.search(r"letterboxd\\.com/film/([^/?#]+)/?", url, flags=re.I)
                if not m:
                    continue
                found_slug = m.group(1).strip().lower()
                slug_like_title = found_slug.replace("-", " ")
                score = _fuzzy_score(title, slug_like_title)
                if year and str(year) in f"{hit.get('title','')} {hit.get('snippet','')}":
                    score += 0.08
                slug_candidates.append((min(1.0, score), found_slug))
            for _, s in sorted(slug_candidates, key=lambda x: x[0], reverse=True):
                if s not in {x[0] for x in slugs_to_try}:
                    slugs_to_try.append((s, "web discovery"))

        for candidate_slug, reason in slugs_to_try:
            try:
                html = client.fetch_film_page(candidate_slug)
                rating, count = _parse_rating_from_html(html, "Letterboxd")
                if rating is not None:
                    return {
                        "source": "Letterboxd",
                        "slug": candidate_slug,
                        "rating": rating,
                        "rating_scale": 5,
                        "rating_count": count,
                        "url": f"https://letterboxd.com/film/{candidate_slug}/",
                    }, f"found via {reason}"
            except Exception:  # noqa: BLE001
                continue

        # If all slugs failed, describe what was tried
        tried = ", ".join(s for s, _ in slugs_to_try)
        return None, f"no rating found — tried slug(s): {tried}"
    except Exception as exc:  # noqa: BLE001
        return None, f"Letterboxd error: {exc!s:.200}"


# ── DB persistence ─────────────────────────────────────────────────────────────

def save_ratings_to_db(session, film_id: str, ratings: list[dict]) -> int:
    """Upsert rating dicts into RatingsMetric (one row per source per day)."""
    from sqlalchemy import select
    from src.models import RatingsMetric

    today = date.today()
    saved = 0
    for r in ratings:
        source = r.get("source") or "unknown"
        rv = r.get("rating")
        if rv is None:
            continue
        existing = session.execute(
            select(RatingsMetric).where(
                RatingsMetric.film_id == film_id,
                RatingsMetric.source_name == source,
                RatingsMetric.metric_date == today,
            )
        ).scalars().first()
        if existing:
            existing.rating_value = rv
            existing.vote_count = r.get("vote_count") or r.get("rating_count")
        else:
            session.add(RatingsMetric(
                film_id=film_id,
                source_name=source,
                rating_value=rv,
                vote_count=r.get("vote_count") or r.get("rating_count"),
                metric_date=today,
                raw_payload_json={k: v for k, v in r.items() if k != "source"},
            ))
        saved += 1
    session.commit()
    return saved
