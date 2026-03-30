from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_fixed

from src.config import get_settings

settings = get_settings()


@dataclass
class ExtractedRecord:
    source_name: str
    source_url: str
    source_entity_id: str | None
    country_code: str | None
    film_title_raw: str
    film_title_ar_raw: str | None
    release_year_hint: int | None
    record_scope: str
    record_granularity: str
    record_semantics: str
    evidence_type: str
    period_label_raw: str | None
    period_start_date: date | None
    period_end_date: date | None
    period_key: str | None
    rank: int | None
    period_gross_local: float | None
    cumulative_gross_local: float | None
    currency: str | None
    admissions_actual: float | None
    parser_confidence: float
    source_confidence: float
    notes: str | None
    raw_payload_json: dict[str, Any]


class BaseHttpClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(
            timeout=settings.http_timeout_seconds,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            follow_redirects=True,
        )

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(settings.http_max_retries))
    def get(self, path: str) -> str:
        url = f"{self.base_url}{path}"
        response = self.client.get(url)
        response.raise_for_status()
        return response.text

