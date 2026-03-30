"""Ratings Lookup — fetch audience ratings from IMDb, elCinema, and Letterboxd."""
import re

import streamlit as st
from sqlalchemy import select

from src.db import session_scope
from src.models import Film, NormalizedEvidence, RawEvidence, RatingsMetric
from src.services.ratings_service import (
    fetch_elcinema_rating,
    fetch_imdb_rating,
    fetch_letterboxd_rating,
    save_ratings_to_db,
)
from ui_helpers import (
    inject_global_css,
    page_header,
    section_divider,
    section_header,
    kpi_card,
    empty_state,
)

inject_global_css()
page_header(
    "Ratings Lookup",
    "Fetch audience ratings from IMDb, elCinema, and Letterboxd. "
    "Known IDs are auto-detected from the database.",
    icon="⭐",
)

if "ratings_lookup_results" not in st.session_state:
    st.session_state["ratings_lookup_results"] = []
if "ratings_lookup_film_id" not in st.session_state:
    st.session_state["ratings_lookup_film_id"] = None
if "ratings_lookup_film_title" not in st.session_state:
    st.session_state["ratings_lookup_film_title"] = None

# ── Film selector ────────────────────────────────────────────────────────────
with session_scope() as session:
    films = session.execute(
        select(Film).order_by(Film.canonical_title)
    ).scalars().all()
    film_options = {
        f.canonical_title + (f" ({f.release_year})" if f.release_year else ""): {
            "id": f.id,
            "title": f.canonical_title,
            "year": f.release_year,
        }
        for f in films
    }

if not film_options:
    empty_state("No films in the database yet. Use Live Data Fetch or Bulk Lookup first.")
    st.stop()

# ── Bulk fetch section ───────────────────────────────────────────────────────
section_divider()
section_header("Bulk Fetch — All Films")

with st.container():
    st.caption(
        "Fetches IMDb, elCinema, and Letterboxd for every film in the database. "
        "Can take a long time — prefer off-peak for large libraries."
    )
    _bulk_n = len(film_options)
    if st.button(
        f"Fetch ratings for all {_bulk_n} films",
        type="secondary",
        use_container_width=True,
        key="ratings_bulk_all_films",
    ):
        from src.services.live_ratings import refresh_live_ratings_for_all_films

        progress = st.progress(0, text=f"0 / {_bulk_n}")
        status_box = st.empty()
        with session_scope() as session:

            def _on_prog(idx: int, total: int, title: str) -> None:
                denom = total if total else 1
                progress.progress((idx + 1) / denom, text=f"{idx + 1} / {total} — {title[:72]}")
                status_box.markdown(f"**Current:** {title}")

            bulk_result = refresh_live_ratings_for_all_films(session, on_progress=_on_prog)

        progress.progress(1.0, text="Done")
        status_box.empty()
        st.success(
            f"Processed **{bulk_result['films_processed']}** films; "
            f"**{bulk_result['total_ratings_saved']}** rating row(s) written."
        )
        with st.expander("Per-film summary", expanded=False):
            st.dataframe(bulk_result["rows"], use_container_width=True, hide_index=True)

# ── Single film lookup ───────────────────────────────────────────────────────
section_divider()
section_header("Single Film Lookup")

selected_label = st.selectbox("Select film", list(film_options.keys()))
film_meta = film_options[selected_label]
selected_film_id = film_meta["id"]
film_title = film_meta["title"]
film_year = film_meta["year"]


def _detect_stored_ids(film_id: str) -> tuple[str | None, str | None]:
    """Return (imdb_title_id, elcinema_work_id) from existing DB records."""
    imdb_id: str | None = None
    elc_id: str | None = None
    with session_scope() as s:
        raw_rows = s.execute(
            select(RawEvidence)
            .join(NormalizedEvidence, NormalizedEvidence.raw_evidence_id == RawEvidence.id)
            .where(NormalizedEvidence.film_id == film_id)
        ).scalars().all()
        seen_elc: set[str] = set()
        for raw in raw_rows:
            if imdb_id is None and raw.source_name == "IMDb":
                se = (raw.source_entity_id or "").strip()
                if re.match(r"tt\d+", se):
                    imdb_id = se
            if elc_id is None:
                for blob in (raw.source_url or "", raw.source_entity_id or ""):
                    m = re.search(r"elcinema\.com/(?:en/)?work/(\d+)", blob, re.I)
                    if m and m.group(1) not in seen_elc:
                        elc_id = m.group(1)
                        seen_elc.add(elc_id)
                        break
                if elc_id is None and raw.source_name == "elCinema":
                    se = (raw.source_entity_id or "").strip()
                    head = se.split("_", 1)[0] if "_" in se else se
                    if re.fullmatch(r"\d{4,12}", head) and head not in seen_elc:
                        elc_id = head
                        seen_elc.add(head)
    return imdb_id, elc_id


detected_imdb, detected_elc = _detect_stored_ids(selected_film_id)

# ── Optional overrides ───────────────────────────────────────────────────────
with st.expander("Override source IDs (optional)", expanded=False):
    col1, col2, col3 = st.columns(3)
    with col1:
        imdb_override = st.text_input(
            "IMDb ID", value=detected_imdb or "", placeholder="tt1234567",
            help="Leave blank to auto-search by title.",
        )
    with col2:
        elc_override = st.text_input(
            "elCinema Work ID", value=detected_elc or "", placeholder="2091459",
            help="Leave blank to auto-search by title.",
        )
    with col3:
        lb_override = st.text_input(
            "Letterboxd slug", value="", placeholder="siko-siko",
            help="Leave blank — slug is derived from the title.",
        )
    st.markdown("**Vote-count overrides** (used if source doesn't return counts)")
    v1, v2 = st.columns(2)
    with v1:
        imdb_votes_override = st.text_input("IMDb votes", value="", placeholder="e.g. 2.3K or 2300")
    with v2:
        lb_votes_override = st.text_input("Letterboxd ratings", value="", placeholder="e.g. 9749")
    notes = []
    if detected_imdb:
        notes.append(f"IMDb `{detected_imdb}` auto-detected")
    if detected_elc:
        notes.append(f"elCinema `{detected_elc}` auto-detected")
    if notes:
        st.caption("  ·  ".join(notes))

# ── Fetch ────────────────────────────────────────────────────────────────────
if st.button("Fetch Ratings", type="primary", use_container_width=True):
    imdb_id = (imdb_override.strip() or detected_imdb) or None
    elc_wid = (elc_override.strip() or detected_elc) or None
    lb_slug = lb_override.strip() or None

    def _parse_count(raw: str) -> int | None:
        txt = (raw or "").strip()
        if not txt:
            return None
        m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([kKmMbB]?)\s*$", txt.replace(",", ""))
        if not m:
            return None
        val = float(m.group(1))
        suf = m.group(2).upper()
        mult = 1
        if suf == "K":
            mult = 1_000
        elif suf == "M":
            mult = 1_000_000
        elif suf == "B":
            mult = 1_000_000_000
        return int(val * mult)

    imdb_votes_manual = _parse_count(imdb_votes_override)
    lb_votes_manual = _parse_count(lb_votes_override)

    results: list[dict] = []

    # IMDb
    spinner_msg = f"IMDb — fetching {imdb_id}..." if imdb_id else "IMDb — searching by title..."
    with st.spinner(spinner_msg):
        r, msg = fetch_imdb_rating(film_title, film_year, imdb_id=imdb_id)

    if r and r.get("rating") is not None:
        if (r.get("vote_count") is None) and (imdb_votes_manual is not None):
            r["vote_count"] = imdb_votes_manual
            r["count_source"] = "manual_override"
        results.append(r)
        votes_str = f"  ·  {int(r['vote_count']):,} votes" if r.get("vote_count") else ""
        st.success(f"**IMDb** (`{r['imdb_id']}`): **{r['rating']:.1f} / 10**{votes_str}")
    else:
        st.warning(f"IMDb: no rating — {msg}")

    # elCinema
    spinner_msg = f"elCinema — fetching work {elc_wid}..." if elc_wid else "elCinema — searching by title..."
    with st.spinner(spinner_msg):
        r, msg = fetch_elcinema_rating(film_title, film_year, work_id=elc_wid)

    if r and r.get("rating") is not None:
        results.append(r)
        votes_str = f"  ·  {int(r['vote_count']):,} votes" if r.get("vote_count") else ""
        st.success(f"**elCinema** (work `{r['work_id']}`): **{r['rating']} / 10**{votes_str}")
    else:
        st.warning(f"elCinema: no rating — {msg}")

    # Letterboxd
    spinner_msg = f"Letterboxd — fetching /{lb_slug}/..." if lb_slug else "Letterboxd — trying slug from title..."
    with st.spinner(spinner_msg):
        r, msg = fetch_letterboxd_rating(film_title, film_year, slug=lb_slug)

    if r and r.get("rating") is not None:
        if (r.get("rating_count") is None) and (lb_votes_manual is not None):
            r["rating_count"] = lb_votes_manual
            r["count_source"] = "manual_override"
        results.append(r)
        cnt_str = f"  ·  {int(r['rating_count']):,} ratings" if r.get("rating_count") else ""
        st.success(f"**Letterboxd** (`{r['slug']}`): **{r['rating']:.2f} / 5**{cnt_str}")
    else:
        st.warning(f"Letterboxd: no rating — {msg}")

    # Save to session state
    if results:
        st.session_state["ratings_lookup_results"] = results
        st.session_state["ratings_lookup_film_id"] = selected_film_id
        st.session_state["ratings_lookup_film_title"] = film_title
    else:
        st.session_state["ratings_lookup_results"] = []
        st.session_state["ratings_lookup_film_id"] = None
        st.session_state["ratings_lookup_film_title"] = None
        st.error("No ratings retrieved from any source.")

# ── Results summary ──────────────────────────────────────────────────────────
active_results = st.session_state.get("ratings_lookup_results") or []
active_film_id = st.session_state.get("ratings_lookup_film_id")
active_film_title = st.session_state.get("ratings_lookup_film_title") or film_title

if active_results and active_film_id == selected_film_id:
    section_divider()
    section_header("Ratings Summary")

    # KPI cards for ratings
    r_cols = st.columns(len(active_results))
    for i, r in enumerate(active_results):
        with r_cols[i]:
            scale = r.get("rating_scale", 10)
            rating_str = f"{r['rating']:.1f} / {scale}" if scale == 10 else f"{r['rating']:.2f} / {scale}"
            votes = r.get("vote_count") or r.get("rating_count")
            sub = f"{int(votes):,} votes" if votes else ""
            kpi_card(r["source"], rating_str, sub)

    st.markdown("")
    table = [
        {
            "Source": r["source"],
            "Rating": f"{r['rating']:.2f} / {r.get('rating_scale', 10)}",
            "Votes": (
                f"{int(r['vote_count']):,}" if r.get("vote_count") else
                f"{int(r['rating_count']):,}" if r.get("rating_count") else "—"
            ),
            "URL": r.get("url", ""),
        }
        for r in active_results
    ]
    st.dataframe(table, use_container_width=True, hide_index=True)

    if st.button("Save ratings to database", type="primary", use_container_width=True):
        with session_scope() as s:
            saved = save_ratings_to_db(s, selected_film_id, active_results)
            latest_rows = s.execute(
                select(RatingsMetric)
                .where(RatingsMetric.film_id == selected_film_id)
                .order_by(RatingsMetric.metric_date.desc(), RatingsMetric.created_at.desc())
            ).scalars().all()
            latest_rows_view = [
                {
                    "Source": x.source_name,
                    "Rating": float(x.rating_value) if x.rating_value is not None else None,
                    "Votes": x.vote_count,
                    "Date": str(x.metric_date),
                }
                for x in latest_rows[:5]
            ]
        st.success(
            f"Saved {saved} rating record{'s' if saved != 1 else ''} for **{active_film_title}**."
        )
        if latest_rows_view:
            st.caption("Latest saved ratings:")
            st.dataframe(latest_rows_view, use_container_width=True, hide_index=True)
