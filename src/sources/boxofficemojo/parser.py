from __future__ import annotations

import re
from datetime import date

from bs4 import BeautifulSoup

from src.sources.common import ExtractedRecord
from src.services.parser_utils import (
    compact,
    find_market_code_by_country,
    html_to_text,
    parse_int_safe,
    parse_mojo_weekend_label,
    parse_money,
    to_iso_date_from_long,
)


def parse_bom_search_results(html: str) -> list[dict]:
    """Extract title URLs from BOM search results page.

    Returns list of dicts with keys: imdb_id, title, year, url.
    """
    out: list[dict] = []
    seen: set[str] = set()
    # Match href="/title/ttNNNNN/" or href="/title/ttNNNNN/?ref_=..."
    for m in re.finditer(r'href="(/title/(tt\d+)/?(?:\?[^"]*)?)"[^>]*>([\s\S]*?)</a>', html, flags=re.I):
        url_path = m.group(1)
        imdb_id = m.group(2)
        title = html_to_text(m.group(3)).strip()
        if not title or imdb_id in seen:
            continue
        seen.add(imdb_id)
        # Try to find year in nearby context
        context = html[m.end():m.end() + 100]
        year: int | None = None
        year_m = re.search(r'\(?(20\d{2}|19\d{2})\)?', context)
        if year_m:
            year = int(year_m.group(1))
        clean_path = url_path.split('?')[0]
        if not clean_path.endswith('/'):
            clean_path += '/'
        out.append({
            "imdb_id": imdb_id,
            "title": title,
            "year": year,
            "url": f"https://www.boxofficemojo.com{clean_path}",
        })
    return out


def parse_weekend_index_page(html: str, source_url: str, market_code: str, year: int) -> list[dict]:
    """Extract weekend codes from BOM country-by-year index page.

    Returns metadata records for later detail fetch, preserving market-level signals
    without misreading date labels as movie titles.
    """
    soup = BeautifulSoup(html, 'lxml')
    out: list[dict] = []
    seen_codes: set[str] = set()

    for tr in soup.select('table tr'):
        row_html = str(tr)
        if '/weekend/' not in row_html or f'area={market_code}' not in row_html:
            continue
        code_match = re.search(r'/weekend/(20\d{2}W\d{1,2})/\?area=([A-Z]{2})', row_html, flags=re.I)
        if not code_match:
            continue
        weekend_code = code_match.group(1)
        # Some index variants include multiple years; keep only the requested backfill year.
        code_year_match = re.match(r"(?P<y>20\d{2})W", weekend_code)
        if code_year_match and int(code_year_match.group("y")) != int(year):
            continue
        if weekend_code in seen_codes:
            continue
        seen_codes.add(weekend_code)
        cells = [html_to_text(str(td)) for td in tr.select('td')]
        if not cells or len(cells) < 2:
            continue
        date_label = cells[0] if cells else ''
        # Authoritative year is in the weekend URL code, not the index ?yr= (avoids Feb 2025 → Feb 2026).
        weekend_year = int(re.match(r"(20\d{2})W", weekend_code).group(1))
        dates = parse_mojo_weekend_label(date_label, weekend_year)
        out.append(
            {
                'market_code': market_code,
                'weekend_code': weekend_code,
                'date_label': date_label,
                'period_start_date': dates[0] if dates else None,
                'period_end_date': dates[1] if dates else None,
                'source_url': source_url,
            }
        )
    return out


def parse_weekend_detail_page(
    html: str,
    source_url: str,
    market_code: str,
    weekend_code: str,
    *,
    period_start_date: date | None = None,
    period_end_date: date | None = None,
) -> list[ExtractedRecord]:
    """Parse weekend detail page into true title-level weekend records."""
    soup = BeautifulSoup(html, 'lxml')
    out: list[ExtractedRecord] = []
    seen: set[tuple[str, int | None]] = set()

    year_match = re.search(r'(20\d{2})W(\d{1,2})', weekend_code)
    release_year_hint = int(year_match.group(1)) if year_match else None

    # BOM weekend detail pages have stable column positions:
    # 0 rank
    # 1 last weekend rank
    # 2 title
    # 3 weekend gross
    # 4 pct change
    # 5 theaters
    # 6 theater change
    # 7 per-theater average
    # 8 total gross
    for tr in soup.select('table tr'):
        tds = tr.select('td')
        if len(tds) < 4:
            continue

        cells = [compact(td.get_text(' ', strip=True)) for td in tds]
        if not any(cells):
            continue

        rank = int(cells[0]) if re.fullmatch(r'\d+', cells[0] or '') else None

        title_anchor = tr.select_one("a[href*='/release/'], a[href*='/title/']")
        title = compact(title_anchor.get_text(' ', strip=True)) if title_anchor else ''
        if not title:
            continue

        weekend_gross = parse_money(cells[3]) if len(cells) > 3 else None
        total_gross = parse_money(cells[8]) if len(cells) > 8 else None

        if weekend_gross is None and total_gross is None:
            continue

        key = (title.lower(), rank)
        if key in seen:
            continue
        seen.add(key)

        out.append(
            ExtractedRecord(
                source_name='Box Office Mojo',
                source_url=source_url,
                source_entity_id=(title_anchor.get('href', '') if title_anchor else None),
                country_code=market_code,
                film_title_raw=title,
                film_title_ar_raw=None,
                release_year_hint=release_year_hint,
                record_scope='title',
                record_granularity='weekend',
                record_semantics='title_period_gross',
                evidence_type='title_performance',
                period_label_raw=weekend_code,
                period_start_date=period_start_date,
                period_end_date=period_end_date,
                period_key=weekend_code,
                rank=rank,
                period_gross_local=weekend_gross,
                cumulative_gross_local=total_gross,
                currency='USD',
                admissions_actual=None,
                parser_confidence=0.88,
                source_confidence=0.80,
                notes='Box Office Mojo weekend detail title row',
                raw_payload_json={
                    'market_code': market_code,
                    'weekend_code': weekend_code,
                    'cells': cells,
                },
            )
        )

    return out


def normalize_bom_title_url(url: str | None) -> str | None:
    if not url:
        return None
    m = re.search(r"https?://www\.boxofficemojo\.com(/title/tt\d+/?)", str(url), flags=re.I)
    if m:
        return f"https://www.boxofficemojo.com{m.group(1).rstrip('/')}/"
    m2 = re.search(r"(/title/tt\d+/?)", str(url), flags=re.I)
    if m2:
        return f"https://www.boxofficemojo.com{m2.group(1).rstrip('/')}/"
    return None


def normalize_bom_release_url(url: str | None) -> str | None:
    if not url:
        return None
    m = re.search(r"https?://www\.boxofficemojo\.com(/release/rl\d+/weekend/?)", str(url), flags=re.I)
    if m:
        return f"https://www.boxofficemojo.com{m.group(1).rstrip('/')}/"
    m2 = re.search(r"(/release/rl\d+/weekend/?)", str(url), flags=re.I)
    if m2:
        return f"https://www.boxofficemojo.com{m2.group(1).rstrip('/')}/"
    return None


def coerce_bom_release_weekend_url(url: str | None) -> str | None:
    """Accept release URLs with or without /weekend/; always return canonical weekend page."""
    norm = normalize_bom_release_url(url)
    if norm:
        return norm
    if not url:
        return None
    m = re.search(r"/release/(rl\d+)", str(url), flags=re.I)
    if m:
        rel = m.group(1).lower()
        return f"https://www.boxofficemojo.com/release/{rel}/weekend/"
    return None


def parse_title_page_candidate(html: str, source_url: str) -> dict:
    soup = BeautifulSoup(html, 'lxml')
    # Remove script/style tags for cleaner text extraction
    for tag in soup.find_all(['script', 'style']):
        tag.decompose()
    text = soup.get_text(' ', strip=True)
    title = ""
    release_year: int | None = None
    # Try original pattern: # Title (Year)
    m = re.search(r"#\s*(.*?)\s*\((20\d{2})\)", text)
    if m:
        title = compact(m.group(1))
        release_year = int(m.group(2))
    # Fallback: "Title (Year)" after navigation items (current BOM layout)
    if not title:
        m2 = re.search(r"(?:Showdowns\s+)?Indices\s+(.*?)\s*\((20\d{2})\)", text, flags=re.I)
        if m2:
            title = compact(m2.group(1))
            release_year = int(m2.group(2))
    # Fallback: page <title> tag
    if not title:
        title_tag = soup.find('title')
        if title_tag:
            raw = title_tag.get_text(' ', strip=True)
            # "Siko Siko - Box Office Mojo" → "Siko Siko"
            parts = raw.split(" - Box Office Mojo")
            if parts:
                title = compact(parts[0])
    # Try to extract year from text if not found yet
    if not release_year and title:
        ym = re.search(rf"{re.escape(title)}\s*\((20\d{{2}})\)", text)
        if ym:
            release_year = int(ym.group(1))

    intl_gross_usd = None
    ig = re.search(r"International\s+\(?100%\)?\s+\$([\d,]+)", text, flags=re.I)
    if ig:
        intl_gross_usd = parse_money(ig.group(1))

    release_urls: list[str] = []
    # Match release URLs with or without /weekend/ suffix
    abs_hits = re.findall(r"https://www\.boxofficemojo\.com/release/rl\d+(?:/weekend)?/?", html, flags=re.I)
    rel_hits = re.findall(r'href="(/release/rl\d+(?:/weekend)?/?(?:\?[^"]*)?)"', html, flags=re.I)
    for u in abs_hits:
        norm = coerce_bom_release_weekend_url(u)
        if norm and norm not in release_urls:
            release_urls.append(norm)
    for u in rel_hits:
        # Strip query params before normalizing
        clean = u.split('?')[0]
        norm = coerce_bom_release_weekend_url(clean)
        if norm and norm not in release_urls:
            release_urls.append(norm)
    return {
        "title_en": title,
        "release_year": release_year,
        "intl_gross_usd": intl_gross_usd,
        "release_urls": release_urls,
        "source_url": source_url,
    }


def parse_release_page_header(html: str, year_hint: int | None) -> tuple[str, int | None]:
    """BOM release page movie title + year (same patterns as fetchBoxOfficeMojoReleaseEvidence_)."""
    soup = BeautifulSoup(html, 'lxml')
    for tag in soup.find_all(['script', 'style']):
        tag.decompose()
    text = soup.get_text(' ', strip=True)
    title = ""
    release_year: int | None = year_hint
    # Original pattern: # Title (Year) ...
    title_match = re.search(
        r"#\s*(.*?)\s*(?:\((20\d{2})\))?\s+Two cousins|#\s*(.*?)\s*(?:\((20\d{2})\))?\s+Title Summary",
        text,
        flags=re.I,
    )
    if title_match:
        title = compact(title_match.group(1) or title_match.group(3) or "")
        y = title_match.group(2) or title_match.group(4)
        if y:
            release_year = int(y)
    if not title:
        simple = re.search(r"#\s*(.*?)\s+Title Summary", text, flags=re.I)
        if simple:
            title = compact(simple.group(1))
    # Fallback: "Title (Year)" without # (current BOM layout)
    if not title:
        m2 = re.search(r"(?:Showdowns\s+)?Indices\s+(.*?)\s*\((20\d{2})\)", text, flags=re.I)
        if m2:
            title = compact(m2.group(1))
            release_year = int(m2.group(2))
    # Fallback: <title> tag
    if not title:
        title_tag = soup.find('title')
        if title_tag:
            raw = title_tag.get_text(' ', strip=True)
            parts = raw.split(" - Box Office Mojo")
            if parts:
                title = compact(parts[0])
    return title, release_year


def _parse_bom_release_territory_summary(text: str) -> tuple[str, float | None]:
    """
    Territory name + lifetime gross for *this* release from the summary block under
    the main \"Grosses\" heading — not from the \"All Territories …\" tab strip.

    BOM release pages repeat the same tab list (Australia, NZ, UAE, UK, …) on every
    territory-specific URL. The old regex captured that strip and substring-matched
    \"United Arab Emirates\" even on the United Kingdom release page, tagging UK
    weekend rows as AE.
    """
    # Typical layout after html_to_text: "## Grosses" then newline(s) then "United Arab Emirates $657,664"
    # Use [ \t]* after Grosses — not \s* — so newlines are left for (?:\r?\n)+ (otherwise \s* eats \n\n).
    m = re.search(
        r"(?:^|\n)[ \t]*#*[ \t]*Grosses[ \t]*(?:\r?\n)+\s*([A-Za-z][A-Za-z\s]+?)\s+\$([\d,]+)",
        text,
        flags=re.I | re.M,
    )
    if m:
        return compact(m.group(1)), parse_money(m.group(2))
    # Same-line layout: "Grosses United Kingdom $11,448"
    m2 = re.search(r"\bGrosses\s+([A-Za-z][A-Za-z\s]+?)\s+\$([\d,]+)", text, flags=re.I)
    if m2:
        return compact(m2.group(1)), parse_money(m2.group(2))
    return "", None


def parse_release_page_evidence(
    html: str,
    source_url: str,
    query: str,
    year_hint: int | None,
    *,
    parent_resolved_score: float | None = None,
) -> list[ExtractedRecord]:
    text = html_to_text(html)
    out: list[ExtractedRecord] = []
    title, release_year = parse_release_page_header(html, year_hint)
    if not title:
        return []

    from src.services.resolved_title_scoring import (  # noqa: PLC0415
        RELEASE_TITLE_FALLBACK_MIN,
        score_resolved_title,
        title_matches_query,
    )

    page_resolved = score_resolved_title(query, title, None, release_year, year_hint)
    title_ok = title_matches_query(query, title, None, release_year, year_hint)
    parent_ok = (parent_resolved_score or 0.0) >= RELEASE_TITLE_FALLBACK_MIN
    if not title_ok and page_resolved < RELEASE_TITLE_FALLBACK_MIN and not parent_ok:
        return []

    territory = ""
    gross = None
    opening = None
    theaters = None
    release_date = None
    territory, gross = _parse_bom_release_territory_summary(text)
    open_m = re.search(r"Opening\s+\$([\d,]+)", text, flags=re.I)
    if open_m:
        opening = parse_money(open_m.group(1))
    th_m = re.search(r"Opening\s+\$[\d,]+\s+(\d+)\s+theaters", text, flags=re.I)
    if th_m:
        theaters = parse_int_safe(th_m.group(1))
    rd_m = re.search(r"Release Date\s+([A-Za-z]{3,9}\s+\d{1,2},\s+20\d{2})", text, flags=re.I)
    if rd_m:
        release_date = to_iso_date_from_long(rd_m.group(1))

    code = find_market_code_by_country(territory)
    if code and gross is not None:
        out.append(
            ExtractedRecord(
                source_name="Box Office Mojo",
                source_url=source_url,
                source_entity_id=f"{source_url}|lifetime",
                country_code=code,
                film_title_raw=title,
                film_title_ar_raw=None,
                release_year_hint=release_year,
                record_scope="title",
                record_granularity="lifetime",
                record_semantics="title_cumulative_total",
                evidence_type="title_performance",
                period_label_raw="lifetime",
                period_start_date=release_date,
                period_end_date=None,
                period_key=None,
                rank=None,
                period_gross_local=None,
                cumulative_gross_local=gross,
                currency="USD",
                admissions_actual=None,
                parser_confidence=0.88,
                source_confidence=0.76,
                notes=f"BOM release lifetime territory evidence; opening={opening}; theaters={theaters}",
                raw_payload_json={"territory": territory, "gross": gross, "opening": opening, "theaters": theaters},
            )
        )

    table_rows = re.findall(r"<tr[\s\S]*?</tr>", html, flags=re.I)
    label_years: list[int] = []
    for tr in table_rows:
        tds = re.findall(r"<td[\s\S]*?</td>", tr, flags=re.I)
        if len(tds) < 5:
            continue
        cells = [compact(html_to_text(td)) for td in tds]
        date_label = cells[0] if cells else ""
        ym = re.search(r",\s*(20\d{2})\s*$", date_label)
        if ym:
            label_years.append(int(ym.group(1)))
    table_anchor_year = max(label_years) if label_years else None
    weekend_fallback_year = table_anchor_year or year_hint or release_year
    if weekend_fallback_year is None:
        weekend_fallback_year = date.today().year

    for tr in table_rows:
        tds = re.findall(r"<td[\s\S]*?</td>", tr, flags=re.I)
        if len(tds) < 5 or not code:
            continue
        cells = [compact(html_to_text(td)) for td in tds]
        date_label = cells[0] if cells else ""
        if not re.match(r"^[A-Z][a-z]{2}\s+\d{1,2}(?:-[A-Z][a-z]{2}\s+\d{1,2}|-\d{1,2})$", date_label):
            continue
        weekend_gross = None
        for idx, c in enumerate(cells):
            if idx <= 3 and re.match(r"^\$[\d,]+(?:\.\d+)?$", c):
                weekend_gross = parse_money(c)
                break
        to_date = None
        for c in reversed(cells):
            if re.match(r"^\$[\d,]+(?:\.\d+)?$", c):
                to_date = parse_money(c)
                break
        period = parse_mojo_weekend_label(date_label, fallback_year=weekend_fallback_year)
        if not period:
            continue
        out.append(
            ExtractedRecord(
                source_name="Box Office Mojo",
                source_url=source_url,
                source_entity_id=f"{source_url}|{period[2]}",
                country_code=code,
                film_title_raw=title,
                film_title_ar_raw=None,
                release_year_hint=release_year,
                record_scope="title",
                record_granularity="weekend",
                record_semantics="title_period_gross",
                evidence_type="title_performance",
                period_label_raw=date_label,
                period_start_date=period[0] if period else None,
                period_end_date=period[1] if period else None,
                period_key=period[2] if period else None,
                rank=parse_int_safe(cells[1] if len(cells) > 1 else None),
                period_gross_local=weekend_gross,
                cumulative_gross_local=to_date or gross,
                currency="USD",
                admissions_actual=None,
                parser_confidence=0.82 if weekend_gross is not None else 0.70,
                source_confidence=0.68,
                notes="BOM release weekend territory row",
                raw_payload_json={"cells": cells},
            )
        )
    return out
