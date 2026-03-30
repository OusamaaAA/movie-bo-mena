import re
from json import loads

from bs4 import BeautifulSoup

from src.sources.imdb.models import ImdbMetric


def _extract_number(text: str) -> int | None:
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def parse_title_metrics(html: str, source_url: str, fallback_title: str) -> ImdbMetric:
    soup = BeautifulSoup(html, "lxml")
    title = soup.select_one("h1")
    rating_text = soup.select_one('[data-testid="hero-rating-bar__aggregate-rating__score"], [aria-label*="IMDb rating"]')
    votes_text = soup.select_one('[data-testid="hero-rating-bar__aggregate-rating__score"] + div, [data-testid="hero-rating-bar__aggregate-rating__score"] ~ div')
    pop_text = soup.find(string=re.compile("Popularity", re.IGNORECASE))
    json_ld = soup.find("script", attrs={"type": "application/ld+json"})
    rating = None
    votes = _extract_number(votes_text.get_text(" ", strip=True)) if votes_text else None
    if rating_text:
        try:
            rating = float(rating_text.get_text(strip=True).split("/")[0])
        except (ValueError, IndexError):
            rating = None
    if (rating is None or votes is None) and json_ld and json_ld.string:
        try:
            payload = loads(json_ld.string)
            agg = payload.get("aggregateRating", {})
            if rating is None and agg.get("ratingValue") is not None:
                rating = float(agg.get("ratingValue"))
            if votes is None and agg.get("ratingCount") is not None:
                votes = int(agg.get("ratingCount"))
        except Exception:  # noqa: BLE001
            pass
    pop_rank = _extract_number(pop_text) if isinstance(pop_text, str) else None
    return ImdbMetric(
        source_name="IMDb",
        film_title_raw=title.get_text(strip=True) if title else fallback_title,
        rating_value=rating,
        vote_count=votes,
        popularity_rank=pop_rank,
        source_url=source_url,
        payload={"raw_title": title.get_text(strip=True) if title else None, "has_jsonld": bool(json_ld)},
    )

