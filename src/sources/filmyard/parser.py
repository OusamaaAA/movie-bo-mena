import html as html_lib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Any

from bs4 import BeautifulSoup

try:
    from src.sources.common import ExtractedRecord  # type: ignore
except Exception:  # noqa: BLE001
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


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    if not s:
        return None

    s = s.replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None


def _extract_inertia_payload(html: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    app = soup.select_one("#app[data-page]")
    if not app:
        return None

    raw = app.get("data-page")
    if not raw:
        return None

    try:
        decoded = html_lib.unescape(raw)
        return json.loads(decoded)
    except Exception:  # noqa: BLE001
        return None


def _find_candidate_rows(obj: Any) -> list[list[dict[str, Any]]]:
    """
    Recursively search the payload for lists of dicts that could contain
    box office movie rows.
    """
    candidates: list[list[dict[str, Any]]] = []

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            if x and all(isinstance(i, dict) for i in x):
                keys = set()
                for item in x[:5]:
                    keys |= set(item.keys())
                key_str = " ".join(keys).lower()
                if any(
                    k in key_str
                    for k in [
                        "title",
                        "movie",
                        "film",
                        "name",
                        "rank",
                        "gross",
                        "revenue",
                        "daily_revenue",
                        "total_revenue",
                        "tickets",
                        "admissions",
                        "total_tickets",
                        "weekly_tickets",
                        "revenues",
                    ]
                ):
                    candidates.append(x)
            for item in x:
                walk(item)

    walk(obj)
    return candidates


def _pick_best_rows(candidates: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    if not candidates:
        return []

    def score(rows: list[dict[str, Any]]) -> tuple[int, int]:
        keys = set()
        for item in rows[:5]:
            keys |= {str(k).lower() for k in item.keys()}

        points = 0

        # Strong Filmyard-specific signals
        for wanted in [
            "name",
            "daily_revenue",
            "total_revenue",
            "daily_tickets",
            "total_tickets",
            "weekly_revenue",
            "weekly_tickets",
            "year",
            "revenues",
        ]:
            if wanted in keys:
                points += 2

        # Generic BO signals
        for wanted in ["title", "rank", "gross", "revenue", "tickets", "total"]:
            if wanted in keys:
                points += 1

        return (points, len(rows))

    candidates.sort(key=score, reverse=True)
    return candidates[0]


def _guess_title(item: dict[str, Any]) -> str | None:
    for k in ["title", "movie_title", "film_title", "movie", "film", "name"]:
        if k in item and item[k]:
            return str(item[k]).strip()
    return None


def _guess_rank(item: dict[str, Any]) -> int | None:
    # "lastWeek" is the previous week's rank, not current — skip it for current rank.
    for k in ["rank", "position", "order"]:
        if k in item and item[k] is not None:
            try:
                return int(float(item[k]))
            except Exception:  # noqa: BLE001
                pass
    return None


def _guess_gross(item: dict[str, Any]) -> float | None:
    for k in [
        "daily_revenue",
        "gross",
        "daily_gross",
        "revenue",
        "box_office",
        "gross_egp",
        "dailyRevenue",
        "weekly_revenue",
    ]:
        if k in item:
            val = _to_float(item[k])
            if val is not None:
                return val
    return None


def _guess_total(item: dict[str, Any]) -> float | None:
    for k in [
        "total_revenue",
        "total",
        "cumulative",
        "total_gross",
        "gross_total",
        "cume",
        "lifetime_gross",
    ]:
        if k in item:
            val = _to_float(item[k])
            if val is not None:
                return val
    return None


def _guess_tickets(item: dict[str, Any]) -> float | None:
    for k in [
        "daily_tickets",
        "tickets",
        "admissions",
        "tickets_sold",
        "attendance",
        "total_tickets",
        "weekly_tickets",
    ]:
        if k in item:
            val = _to_float(item[k])
            if val is not None:
                return val
    return None


def _guess_work_id(item: dict[str, Any]) -> str | None:
    for k in ["id", "movie_id", "film_id", "work_id"]:
        if k in item and item[k]:
            return str(item[k])
    return None


def _infer_filmyard_data_date(item: dict[str, Any]) -> date | None:
    """Day the per-cinema revenue lines refer to (often yesterday vs page / fetch date)."""
    revs = item.get("revenues")
    if not isinstance(revs, list) or not revs:
        return None
    parsed: list[date] = []
    for row in revs:
        if not isinstance(row, dict):
            continue
        raw = row.get("date")
        if raw is None or raw == "":
            continue
        try:
            parsed.append(date.fromisoformat(str(raw)[:10]))
        except Exception:  # noqa: BLE001
            continue
    if not parsed:
        return None
    most_common = Counter(parsed).most_common()
    if len(most_common) == 1:
        return most_common[0][0]
    top_count = most_common[0][1]
    tied = sorted({d for d, c in most_common if c == top_count})
    return tied[0] if tied else parsed[0]


def _guess_weekly_gross(item: dict[str, Any]) -> float | None:
    """Extract weekly (7-day accumulated) gross specifically — not daily."""
    val = _to_float(item.get("weekly_revenue"))
    if val is not None:
        return val
    return None


def _guess_weekly_tickets(item: dict[str, Any]) -> float | None:
    """Extract weekly (7-day accumulated) ticket count specifically — not daily."""
    val = _to_float(item.get("weekly_tickets"))
    if val is not None:
        return val
    return None


def _guess_daily_gross(item: dict[str, Any]) -> float | None:
    """Extract daily gross — prefer daily_revenue, skip weekly_revenue."""
    for k in ["daily_revenue", "gross", "daily_gross", "revenue", "box_office", "gross_egp", "dailyRevenue"]:
        if k in item:
            val = _to_float(item[k])
            if val is not None:
                return val
    return None


def _guess_daily_tickets(item: dict[str, Any]) -> float | None:
    """Extract daily ticket count — prefer daily_tickets, skip weekly_tickets."""
    for k in ["daily_tickets", "tickets", "admissions", "tickets_sold", "attendance"]:
        if k in item:
            val = _to_float(item[k])
            if val is not None:
                return val
    return None


def parse_daily_egypt(html: str, source_url: str) -> list[ExtractedRecord]:
    payload = _extract_inertia_payload(html)
    records: list[ExtractedRecord] = []

    page_date: date | None = None
    if payload:
        props = payload.get("props", {}) if isinstance(payload, dict) else {}
        raw_date = props.get("date")
        if raw_date:
            try:
                page_date = date.fromisoformat(str(raw_date))
            except Exception:  # noqa: BLE001
                page_date = None

        candidates = _find_candidate_rows(props)
        rows = _pick_best_rows(candidates)

        for idx, item in enumerate(rows, start=1):
            title = _guess_title(item)
            if not title:
                continue

            data_date = _infer_filmyard_data_date(item) or page_date

            year_hint = None
            try:
                if item.get("year"):
                    year_hint = int(item.get("year"))
            except Exception:  # noqa: BLE001
                year_hint = None

            rank = _guess_rank(item)
            if rank is None:
                rank = idx

            # Daily record
            daily_gross = _guess_daily_gross(item)
            daily_tickets = _guess_daily_tickets(item)
            if daily_gross is not None:
                records.append(
                    ExtractedRecord(
                        source_name="Filmyard",
                        source_url=source_url,
                        source_entity_id=_guess_work_id(item),
                        country_code="EG",
                        film_title_raw=title,
                        film_title_ar_raw=None,
                        release_year_hint=year_hint,
                        record_scope="title",
                        record_granularity="day",
                        record_semantics="title_period_gross",
                        evidence_type="title_performance",
                        period_label_raw=str(data_date) if data_date else None,
                        period_start_date=data_date,
                        period_end_date=data_date,
                        period_key=str(data_date) if data_date else None,
                        rank=rank,
                        period_gross_local=daily_gross,
                        cumulative_gross_local=_guess_total(item),
                        currency="EGP",
                        admissions_actual=daily_tickets,
                        parser_confidence=0.97,
                        source_confidence=0.92,
                        notes="Filmyard daily Egypt",
                        raw_payload_json=item,
                    )
                )

            # Weekly accumulation record — emitted separately when meaningful weekly data exists.
            # period_key is left None here; ingest.py converts it to the ISO week.
            weekly_gross = _guess_weekly_gross(item)
            weekly_tickets = _guess_weekly_tickets(item)
            if weekly_gross is not None and weekly_gross > 0:
                records.append(
                    ExtractedRecord(
                        source_name="Filmyard",
                        source_url=source_url,
                        source_entity_id=_guess_work_id(item),
                        country_code="EG",
                        film_title_raw=title,
                        film_title_ar_raw=None,
                        release_year_hint=year_hint,
                        record_scope="title",
                        record_granularity="week",
                        record_semantics="title_period_gross",
                        evidence_type="title_performance",
                        period_label_raw=None,
                        period_start_date=data_date,
                        period_end_date=data_date,
                        period_key=None,
                        rank=rank,
                        period_gross_local=weekly_gross,
                        cumulative_gross_local=_guess_total(item),
                        currency="EGP",
                        admissions_actual=weekly_tickets,
                        parser_confidence=0.97,
                        source_confidence=0.95,
                        notes="Filmyard weekly Egypt",
                        raw_payload_json=item,
                    )
                )

        if records:
            return records

    # No fallback DOM parsing for now because current Filmyard payload is in Inertia JSON.
    return []