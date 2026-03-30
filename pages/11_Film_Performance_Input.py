"""Film Performance Input — marketing spend, first-watch outcomes, and ratings for modeling (historical releases)."""
from datetime import date, datetime, timezone
from decimal import Decimal

import streamlit as st
from sqlalchemy import select

from src.db import session_scope
from src.models import MarketingInput, OutcomeTarget, RatingsMetric
from src.repositories.film_repository import FilmRepository
from ui_helpers import (
    inject_global_css,
    page_header,
    section_divider,
    section_header,
    empty_state,
)

MARKETS = ("EG", "SA", "AE")
MARKET_NAMES = {"EG": "Egypt", "SA": "Saudi Arabia", "AE": "UAE"}
DEFAULT_CURRENCY = {"EG": "EGP", "SA": "SAR", "AE": "AED"}


def _latest_marketing(session, film_id: str, market: str) -> MarketingInput | None:
    return (
        session.execute(
            select(MarketingInput)
            .where(MarketingInput.film_id == film_id, MarketingInput.market_code == market)
            .order_by(MarketingInput.updated_at.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )


def _first_watch_display(session, film_id: str, market: str) -> float:
    """Prefer explicit first_watch; fall back to first_watch_target for display."""
    r_fw = (
        session.execute(
            select(OutcomeTarget).where(
                OutcomeTarget.film_id == film_id,
                OutcomeTarget.market_code == market,
                OutcomeTarget.target_label == "first_watch",
            )
        )
        .scalars()
        .first()
    )
    if r_fw and r_fw.target_value is not None:
        return float(r_fw.target_value)
    r_t = (
        session.execute(
            select(OutcomeTarget).where(
                OutcomeTarget.film_id == film_id,
                OutcomeTarget.market_code == market,
                OutcomeTarget.target_label == "first_watch_target",
            )
        )
        .scalars()
        .first()
    )
    if r_t and r_t.target_value is not None:
        return float(r_t.target_value)
    return 0.0


def _prefill_ratings(session, film_id: str) -> tuple[float | None, float | None, int | None]:
    rows = list(
        session.execute(
            select(RatingsMetric)
            .where(RatingsMetric.film_id == film_id)
            .order_by(RatingsMetric.metric_date.desc())
        )
        .scalars()
        .all()
    )
    imdb: float | None = None
    elc: float | None = None
    mm: int | None = None
    for r in rows:
        s = (r.source_name or "").strip()
        if s == "IMDb" and imdb is None:
            imdb = float(r.rating_value) if r.rating_value is not None else None
            mm = r.popularity_rank
        if s == "elCinema" and elc is None:
            elc = float(r.rating_value) if r.rating_value is not None else None
        if imdb is not None and elc is not None:
            break
    return imdb, elc, mm


def _upsert_marketing(session, film_id: str, market: str, spend: Decimal, currency: str | None) -> None:
    row = _latest_marketing(session, film_id, market)
    now = datetime.now(timezone.utc)
    if row:
        row.spend_local = spend
        row.spend_currency = currency
        row.updated_at = now
    else:
        session.add(
            MarketingInput(
                film_id=film_id,
                market_code=market,
                spend_local=spend,
                spend_currency=currency,
            )
        )


def _upsert_first_watch(session, film_id: str, market: str, value: Decimal) -> None:
    row = (
        session.execute(
            select(OutcomeTarget).where(
                OutcomeTarget.film_id == film_id,
                OutcomeTarget.market_code == market,
                OutcomeTarget.target_label == "first_watch",
            )
        )
        .scalars()
        .first()
    )
    now = datetime.now(timezone.utc)
    if row:
        row.target_value = value
        row.target_unit = "admissions"
        row.updated_at = now
    else:
        session.add(
            OutcomeTarget(
                film_id=film_id,
                market_code=market,
                target_label="first_watch",
                target_value=value,
                target_unit="admissions",
            )
        )


def _upsert_rating(
    session,
    film_id: str,
    source_name: str,
    rating: Decimal | None,
    popularity_rank: int | None = None,
) -> None:
    if rating is None and popularity_rank is None:
        return
    row = (
        session.execute(
            select(RatingsMetric)
            .where(RatingsMetric.film_id == film_id, RatingsMetric.source_name == source_name)
            .order_by(RatingsMetric.metric_date.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )
    today = date.today()
    if row:
        if rating is not None:
            row.rating_value = rating
        if source_name == "IMDb" and popularity_rank is not None:
            row.popularity_rank = popularity_rank
        row.metric_date = today
    else:
        if rating is None:
            return
        session.add(
            RatingsMetric(
                film_id=film_id,
                source_name=source_name,
                rating_value=rating,
                popularity_rank=popularity_rank if source_name == "IMDb" else None,
                metric_date=today,
                raw_payload_json={"manual_entry": True},
            )
        )


# ── Page UI ──────────────────────────────────────────────────────────────────
inject_global_css()
page_header(
    "Film Performance Input",
    "Historical marketing spend, first-watch results, and ratings for released titles.",
    icon="📝",
)

query = st.text_input("Search film title", placeholder="e.g. Title name")
with session_scope() as session:
    films = FilmRepository(session).search_rows(query, limit=12) if query else []

selected_film_id: str | None = None
if films:
    labels = [f"{f['canonical_title']} ({f['release_year'] or '-'})" for f in films]
    selected_label = st.selectbox("Select film", labels)
    selected_film_id = films[labels.index(selected_label)]["id"]

if not selected_film_id:
    empty_state("Search and select a film to enter or update performance inputs.")
    st.stop()

# Load current values for form defaults
with session_scope() as session:
    imdb0, elc0, mm0 = _prefill_ratings(session, selected_film_id)
    market_spend: dict[str, tuple[float, str]] = {}
    market_fw: dict[str, float] = {}
    for m in MARKETS:
        mi = _latest_marketing(session, selected_film_id, m)
        spend = float(mi.spend_local or 0) if mi else 0.0
        cur = (mi.spend_currency or DEFAULT_CURRENCY[m]) if mi else DEFAULT_CURRENCY[m]
        market_spend[m] = (spend, cur)
        market_fw[m] = _first_watch_display(session, selected_film_id, m)

# ── Per-market inputs ────────────────────────────────────────────────────────
section_divider()
section_header("Per Market")

cols = st.columns(3)
inputs: dict[str, dict[str, float | str]] = {}
for i, m in enumerate(MARKETS):
    with cols[i]:
        with st.container():
            st.markdown(f"**{MARKET_NAMES[m]} ({m})**")
            spend, cur = market_spend[m]
            inputs[m] = {
                "spend": st.number_input(
                    "Marketing spend (local)",
                    min_value=0.0, value=spend, step=10_000.0, key=f"spend_{m}",
                ),
                "currency": st.text_input("Currency", value=cur, key=f"cur_{m}"),
                "fw": st.number_input(
                    "First watch result",
                    min_value=0.0, value=float(market_fw[m]), step=1_000.0, key=f"fw_{m}",
                    help="Streaming first-watch count (or your unit of record).",
                ),
            }

# ── Ratings ──────────────────────────────────────────────────────────────────
section_header("Ratings (optional)")
r1, r2, r3 = st.columns(3)
with r1:
    imdb_in = st.number_input(
        "IMDb rating", min_value=0.0, max_value=10.0,
        value=float(imdb0) if imdb0 is not None else 0.0,
        step=0.1, format="%.1f", key="imdb_r",
    )
with r2:
    elc_in = st.number_input(
        "elCinema rating", min_value=0.0, max_value=10.0,
        value=float(elc0) if elc0 is not None else 0.0,
        step=0.1, format="%.1f", key="elc_r",
    )
with r3:
    mm_in = st.number_input(
        "Movie Meter / popularity rank", min_value=0,
        value=int(mm0) if mm0 is not None else 0,
        step=1, key="mm_r",
        help="Lower is usually better (e.g. IMDb MOVIE METER).",
    )

st.markdown("")
save = st.button("Save", type="primary", use_container_width=True)
if save:
    with session_scope() as session:
        for m in MARKETS:
            block = inputs[m]
            _upsert_marketing(
                session, selected_film_id, m,
                Decimal(str(block["spend"])),
                str(block["currency"] or DEFAULT_CURRENCY[m]).strip() or None,
            )
            _upsert_first_watch(session, selected_film_id, m, Decimal(str(block["fw"])))

        if imdb_in > 0 or mm_in > 0:
            _upsert_rating(
                session, selected_film_id, "IMDb",
                Decimal(str(imdb_in)) if imdb_in > 0 else None,
                int(mm_in) if mm_in > 0 else None,
            )
        if elc_in > 0:
            _upsert_rating(session, selected_film_id, "elCinema", Decimal(str(elc_in)), None)

    st.success("Saved marketing inputs, first-watch outcomes, and ratings.")
