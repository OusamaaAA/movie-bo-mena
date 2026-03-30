"""Match Review — confirm or reject auto-matched films flagged for manual review."""
import streamlit as st
from sqlalchemy import select

from src.db import session_scope
from src.models import Film, RawEvidence, ReviewQueue
from src.repositories.report_repository import ReportRepository
from src.services.review_workflow import (
    approve_match_to_existing_film,
    create_new_film_from_review_item,
    ignore_review_item,
)
from ui_helpers import (
    inject_global_css,
    market_name,
    page_header,
    section_divider,
    section_header,
    empty_state,
)

inject_global_css()
page_header("Match Review", "Films that couldn't be automatically matched — review and confirm each one.", icon="✅")

# ── Load review items ────────────────────────────────────────────────────────
with session_scope() as session:
    orm_rows = ReportRepository(session).open_review_items()
    rows = [
        {
            "id": r.id,
            "film_title_raw": r.film_title_raw or "-",
            "release_year_hint": r.release_year_hint,
            "candidate_film_id": r.candidate_film_id,
            "candidate_score": float(r.candidate_score or 0),
            "raw_evidence_id": r.raw_evidence_id,
        }
        for r in orm_rows
    ]

if not rows:
    st.success("Nothing to review — all films are matched.")
    st.stop()

total = len(rows)
st.info(f"**{total}** item{'s' if total != 1 else ''} waiting for review.")

# ── Bulk auto-confirm ────────────────────────────────────────────────────────
high_conf = [r for r in rows if r["candidate_film_id"] and r["candidate_score"] >= 0.95]
if high_conf:
    with st.container():
        st.caption(
            f"{len(high_conf)} item{'s' if len(high_conf) != 1 else ''} have a candidate match "
            "at 95%+ confidence and can be confirmed automatically."
        )
        if st.button(
            f"Auto-confirm {len(high_conf)} high-confidence item{'s' if len(high_conf) != 1 else ''}",
            type="primary",
        ):
            confirmed = 0
            errors = 0
            with session_scope() as session:
                for item in high_conf:
                    try:
                        approve_match_to_existing_film(session, item["id"])
                        confirmed += 1
                    except Exception:  # noqa: BLE001
                        errors += 1
            if confirmed:
                st.success(f"Confirmed {confirmed} item{'s' if confirmed != 1 else ''}.")
            if errors:
                st.warning(f"{errors} item{'s' if errors != 1 else ''} could not be confirmed — review manually.")
            st.rerun()

# ── Queue summary table ─────────────────────────────────────────────────────
section_divider()
section_header("Review Queue")

summary = [
    {
        "Raw Title": r["film_title_raw"],
        "Year": r["release_year_hint"] or "—",
        "Candidate": "Yes" if r["candidate_film_id"] else "No",
        "Confidence": f"{r['candidate_score']:.0%}" if r["candidate_film_id"] else "—",
    }
    for r in rows
]
st.dataframe(summary, use_container_width=True, hide_index=True)

# ── Item detail ──────────────────────────────────────────────────────────────
section_divider()
section_header("Review Item")

review_ids = [r["id"] for r in rows]
id_to_title = {r["id"]: r["film_title_raw"] for r in rows}

selected_id = st.selectbox(
    "Select an item to review",
    review_ids,
    format_func=lambda rid: id_to_title.get(rid, str(rid)),
)

selected_row = next((r for r in rows if r["id"] == selected_id), None)

# Fetch candidate film name inside a fresh session
candidate_film_name = None
raw_source = None
raw_market = None

if selected_row:
    with session_scope() as session:
        if selected_row["candidate_film_id"]:
            cfilm = session.get(Film, selected_row["candidate_film_id"])
            if cfilm:
                candidate_film_name = cfilm.canonical_title

        if selected_row["raw_evidence_id"]:
            raw = session.get(RawEvidence, selected_row["raw_evidence_id"])
            if raw:
                raw_source = raw.source_name
                raw_market = market_name(raw.country_code) if raw.country_code else "-"

if selected_row:
    col1, col2 = st.columns(2)
    with col1:
        with st.container():
            st.markdown("**Title from source**")
            st.markdown(f"### {selected_row['film_title_raw']}")
            if selected_row["release_year_hint"]:
                st.caption(f"Release year: {selected_row['release_year_hint']}")
            if raw_source:
                st.caption(f"Source: {raw_source} · {raw_market or '-'}")

    with col2:
        with st.container():
            st.markdown("**Proposed match in database**")
            if candidate_film_name:
                st.markdown(f"### {candidate_film_name}")
                pct = selected_row["candidate_score"]
                if pct >= 0.90:
                    st.success(f"Confidence: {pct:.0%}")
                elif pct >= 0.70:
                    st.warning(f"Confidence: {pct:.0%}")
                else:
                    st.error(f"Confidence: {pct:.0%}")
            else:
                empty_state("No candidate found in the database.")

    analyst_notes = st.text_area("Notes (optional)", placeholder="Add any relevant context...")

    if selected_row["candidate_film_id"]:
        decision = st.radio(
            "Action",
            ["Confirm this match", "Create as a new film", "Ignore / skip"],
            horizontal=True,
        )
    else:
        decision = st.radio(
            "Action",
            ["Create as a new film", "Ignore / skip"],
            horizontal=True,
        )

    if st.button("Apply decision", type="primary", use_container_width=True):
        with session_scope() as session:
            if decision == "Confirm this match":
                approve_match_to_existing_film(session, selected_id, analyst_notes=analyst_notes or None)
                st.success("Match confirmed and data reconciled.")
            elif decision == "Create as a new film":
                create_new_film_from_review_item(session, selected_id, analyst_notes=analyst_notes or None)
                st.success("New film created and data reconciled.")
            else:
                ignore_review_item(session, selected_id, analyst_notes=analyst_notes or None)
                st.success("Item ignored.")
        st.rerun()
