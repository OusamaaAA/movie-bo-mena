import re

from bs4 import BeautifulSoup


def parse_search_results(html: str) -> list[dict]:
    """
    Parse Letterboxd search results page and return list of film candidates.
    Each dict has: slug, title, year.
    """
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen: set[str] = set()

    # Film entries are in <li class="film-list-entry ..."> or <li class="search-result ...">
    for li in soup.select("li.film-list-entry, li[class*='search-result']"):
        # Try to find the film slug from a link like /film/siko-siko/
        link = li.select_one("a[href*='/film/']")
        if not link:
            continue
        href = link.get("href", "")
        m = re.search(r"/film/([^/]+)/", href)
        if not m:
            continue
        slug = m.group(1)
        if slug in seen:
            continue
        seen.add(slug)

        # Title: from data-film-name, alt text, or link text
        title = (
            li.get("data-film-name")
            or li.select_one("img[alt]") and li.select_one("img[alt]").get("alt", "")
            or link.get_text(strip=True)
            or slug.replace("-", " ").title()
        )

        # Year: from data-film-year or span with year
        year_raw = li.get("data-film-release-year") or li.get("data-film-year")
        year: int | None = None
        if year_raw:
            try:
                year = int(str(year_raw).strip())
            except ValueError:
                pass
        if year is None:
            year_m = re.search(r"\b(19|20)\d{2}\b", li.get_text())
            if year_m:
                year = int(year_m.group(0))

        results.append({"slug": slug, "title": title, "year": year})

    return results


def parse_film_rating(html: str, slug: str) -> dict | None:
    """
    Parse Letterboxd film page and extract average member rating.
    Returns dict with: slug, rating (out of 5), rating_count (if available), url.
    Returns None if no rating found.
    """
    soup = BeautifulSoup(html, "lxml")

    rating: float | None = None
    rating_count: int | None = None

    # Method 1: script tag with rating histogram JSON
    for script in soup.find_all("script"):
        text = script.string or ""
        m = re.search(r'"ratingHistogram"[^}]*"meanRating"\s*:\s*([\d.]+)', text)
        if m:
            try:
                rating = float(m.group(1))
            except ValueError:
                pass
            cnt = re.search(r'"totalRatings"\s*:\s*(\d+)', text)
            if cnt:
                try:
                    rating_count = int(cnt.group(1))
                except ValueError:
                    pass
            if rating is not None:
                break

    # Method 2: meta twitter:data2 content="X.XX out of 5"
    if rating is None:
        meta = soup.find("meta", attrs={"name": "twitter:data2"})
        if meta:
            content = meta.get("content", "")
            m = re.search(r"([\d.]+)\s+out\s+of\s+5", content, re.I)
            if m:
                try:
                    rating = float(m.group(1))
                except ValueError:
                    pass

    # Method 3: span or div with class containing "average-rating" or "display-rating"
    if rating is None:
        for sel in (".average-rating", "[data-average-rating]", ".display-rating"):
            el = soup.select_one(sel)
            if el:
                val = el.get("data-average-rating") or el.get_text(strip=True)
                try:
                    rating = float(str(val).split("/")[0].strip())
                    break
                except (ValueError, AttributeError):
                    pass

    # Method 4: JSON-LD aggregateRating
    if rating is None:
        import json
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or "")
                agg = data.get("aggregateRating", {})
                if agg.get("ratingValue"):
                    # Letterboxd uses 0–5 scale; JSON-LD may vary
                    rating = float(agg["ratingValue"])
                if agg.get("ratingCount"):
                    rating_count = int(agg["ratingCount"])
                if rating is not None:
                    break
            except Exception:  # noqa: BLE001
                pass

    if rating is None:
        return None

    return {
        "source": "Letterboxd",
        "slug": slug,
        "rating": rating,
        "rating_scale": 5,
        "rating_count": rating_count,
        "url": f"https://letterboxd.com/film/{slug}/",
    }
