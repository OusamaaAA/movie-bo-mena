"""Campaign Targets — record marketing spend and admissions targets per film and market."""
from decimal import Decimal

import streamlit as st
from sqlalchemy import select

from src.db import session_scope
from src.models import MarketingInput, OutcomeTarget
from src.repositories.film_repository import FilmRepository
from ui_helpers import (
    inject_global_css,
    market_name,
    MARKET_NAMES,
    page_header,
    section_divider,
    section_header,
    empty_state,
)

MARKET_OPTIONS = {v: k for k, v in MARKET_NAMES.items()}

inject_global_css()
page_header(
    "Campaign Targets",
    "Record marketing spend and admissions targets per film and market.",
    icon="🎯",
)

# ── Film search ──────────────────────────────────────────────────────────────
query = st.text_input("Search film title", placeholder="e.g. Siko Siko")
with session_scope() as session:
    films = FilmRepository(session).search_rows(query, limit=10) if query else []

selected_film_id = None
if films:
    labels = [f"{f['canonical_title']} ({f['release_year'] or '-'})" for f in films]
    selected_label = st.selectbox("Select film", labels)
    selected_film_id = films[labels.index(selected_label)]["id"]

if not selected_film_id:
    empty_state("Search and select a film to add campaign data.")
    st.stop()

# ── Add campaign data ────────────────────────────────────────────────────────
section_divider()
section_header("Add Campaign Data")

with st.form("campaign_form"):
    col1, col2 = st.columns(2)
    with col1:
        market_display = st.selectbox("Market", list(MARKET_NAMES.values()))
        market_code = MARKET_OPTIONS[market_display]
    with col2:
        spend_currency = st.text_input("Currency", value="EGP" if market_code == "EG" else "USD")

    col3, col4 = st.columns(2)
    with col3:
        spend_local = st.number_input(
            "Marketing spend (local currency)",
            min_value=0.0, value=0.0, step=10_000.0,
        )
    with col4:
        target_value = st.number_input(
            "Admissions target",
            min_value=0.0, value=0.0, step=1_000.0,
            help="Expected number of first watches / admissions.",
        )

    save_clicked = st.form_submit_button("Save", type="primary", use_container_width=True)

if save_clicked:
    with session_scope() as session:
        film = FilmRepository(session).get(selected_film_id)
        if not film:
            st.error("Film not found.")
        else:
            if spend_local > 0:
                session.add(MarketingInput(
                    film_id=selected_film_id,
                    market_code=market_code,
                    spend_local=Decimal(str(spend_local)),
                    spend_currency=spend_currency,
                ))
            if target_value > 0:
                session.add(OutcomeTarget(
                    film_id=selected_film_id,
                    market_code=market_code,
                    target_label="first_watch_target",
                    target_value=Decimal(str(target_value)),
                    target_unit="admissions",
                ))
            st.success("Saved.")

# ── Existing data ────────────────────────────────────────────────────────────
section_divider()
section_header("Saved Campaign Data")

with session_scope() as session:
    spend_display = [
        {
            "Market": market_name(m.market_code),
            "Spend": f"{float(m.spend_local or 0):,.0f} {m.spend_currency or ''}".strip(),
        }
        for m in session.execute(
            select(MarketingInput).where(MarketingInput.film_id == selected_film_id)
        ).scalars().all()
    ]
    target_display = [
        {
            "Market": market_name(t.market_code),
            "Target": f"{float(t.target_value or 0):,.0f} {t.target_unit or ''}".strip(),
        }
        for t in session.execute(
            select(OutcomeTarget).where(OutcomeTarget.film_id == selected_film_id)
        ).scalars().all()
    ]

col1, col2 = st.columns(2)
with col1:
    with st.container():
        st.markdown("**Marketing Spend**")
        if spend_display:
            st.dataframe(spend_display, use_container_width=True, hide_index=True)
        else:
            empty_state("No spend recorded yet.")

with col2:
    with st.container():
        st.markdown("**Admissions Targets**")
        if target_display:
            st.dataframe(target_display, use_container_width=True, hide_index=True)
        else:
            empty_state("No targets recorded yet.")
