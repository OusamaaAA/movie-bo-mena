"""Film Commercial Evaluation — decision support for potential new MENA releases."""
from __future__ import annotations

import streamlit as st

from src.db import session_scope
from src.services.commercial_decision_engine import (
    PotentialFilm,
    evaluate_potential_film,
    MARKET_NAMES,
)
from ui_helpers import (
    inject_global_css,
    page_header,
    section_divider,
    section_header,
    kpi_card,
    decision_badge,
    empty_state,
)

st.set_page_config(page_title="Film Evaluation", layout="wide")
inject_global_css()

page_header(
    "Film Commercial Evaluation",
    "Evaluate a potential new film for MENA acquisition/release. "
    "The engine finds comparable historical films, forecasts admissions, "
    "and produces a commercial decision recommendation.",
    icon="🎬",
)

# ── Constants ────────────────────────────────────────────────────────────────
MARKET_CURRENCIES = {"EG": "EGP", "AE": "AED", "SA": "SAR"}

GENRE_OPTIONS = [
    "", "Action", "Comedy", "Drama", "Horror", "Romance",
    "Thriller", "Animation", "Documentary", "Family", "Adventure",
    "Crime", "Fantasy", "Sci-Fi", "Musical", "War",
]

ORIGIN_OPTIONS = [
    "", "Egypt", "Saudi Arabia", "UAE", "Lebanon", "Morocco",
    "Tunisia", "Jordan", "Iraq", "USA", "India", "Korea",
    "Turkey", "France", "UK", "Other",
]


# ── Input Form ───────────────────────────────────────────────────────────────
with st.form("evaluation_form"):
    section_header("Film Information")
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        title = st.text_input("Film Title *", placeholder="Enter film title")
    with col_b:
        genre = st.selectbox("Genre", GENRE_OPTIONS)
    with col_c:
        origin = st.selectbox("Country of Origin", ORIGIN_OPTIONS)

    section_header("Audience Signals")
    col_d, col_e, col_f = st.columns(3)
    with col_d:
        imdb = st.number_input(
            "Expected IMDb Rating", min_value=0.0, max_value=10.0,
            value=0.0, step=0.1, help="Leave at 0 if unknown",
        )
    with col_e:
        elc = st.number_input(
            "Expected elCinema Rating", min_value=0.0, max_value=10.0,
            value=0.0, step=0.1, help="Leave at 0 if unknown",
        )
    with col_f:
        meter = st.number_input(
            "BOM Movie Meter Rank", min_value=0, max_value=10000,
            value=0, step=1, help="Lower rank = more buzz. Leave at 0 if unknown.",
        )

    section_header("Marketing Spend")
    col_g, col_h, col_i, col_j = st.columns(4)
    with col_g:
        spend_eg = st.number_input("Egypt (EGP)", min_value=0.0, value=0.0, step=50_000.0)
    with col_h:
        spend_ae = st.number_input("UAE (AED)", min_value=0.0, value=0.0, step=10_000.0)
    with col_i:
        spend_sa = st.number_input("Saudi (SAR)", min_value=0.0, value=0.0, step=10_000.0)
    with col_j:
        spend_usd = st.number_input("Total MENA (USD)", min_value=0.0, value=0.0, step=5_000.0)

    section_header("First Watch Target (Admissions)")
    col_k, col_l, col_m, col_n = st.columns(4)
    with col_k:
        target_eg = st.number_input("Egypt Target", min_value=0, value=0, step=10_000)
    with col_l:
        target_ae = st.number_input("UAE Target", min_value=0, value=0, step=5_000)
    with col_m:
        target_sa = st.number_input("Saudi Target", min_value=0, value=0, step=5_000)
    with col_n:
        target_total = st.number_input(
            "MENA Total Target", min_value=0, value=0, step=10_000,
            help="If set, overrides sum of per-market targets.",
        )

    submitted = st.form_submit_button("Evaluate Film", type="primary", use_container_width=True)

# ── Run Evaluation ───────────────────────────────────────────────────────────
if submitted:
    if not title.strip():
        st.error("Please enter a film title.")
        st.stop()

    marketing_spend: dict[str, float] = {}
    spend_currencies: dict[str, str] = {}
    if spend_eg > 0:
        marketing_spend["EG"] = spend_eg
        spend_currencies["EG"] = "EGP"
    if spend_ae > 0:
        marketing_spend["AE"] = spend_ae
        spend_currencies["AE"] = "AED"
    if spend_sa > 0:
        marketing_spend["SA"] = spend_sa
        spend_currencies["SA"] = "SAR"

    first_watch_target: dict[str, float] = {}
    if target_eg > 0:
        first_watch_target["EG"] = float(target_eg)
    if target_ae > 0:
        first_watch_target["AE"] = float(target_ae)
    if target_sa > 0:
        first_watch_target["SA"] = float(target_sa)

    total_fw = float(target_total) if target_total > 0 else sum(first_watch_target.values())

    potential = PotentialFilm(
        title=title.strip(),
        genre=genre,
        country_of_origin=origin,
        expected_imdb_rating=imdb if imdb > 0 else None,
        expected_elcinema_rating=elc if elc > 0 else None,
        movie_meter_rank=meter if meter > 0 else None,
        marketing_spend=marketing_spend,
        spend_currencies=spend_currencies,
        total_marketing_spend_usd=spend_usd,
        first_watch_target=first_watch_target,
        total_first_watch_target=total_fw,
    )

    with st.spinner("Analysing comparable films and building forecast..."):
        with session_scope() as session:
            result = evaluate_potential_film(session, potential)

    st.session_state["eval_result"] = result
    st.session_state["eval_title"] = title

# ── Display Results ──────────────────────────────────────────────────────────
result = st.session_state.get("eval_result")
eval_title = st.session_state.get("eval_title", "")

if not result:
    empty_state("Fill in the form above and click Evaluate Film to see results.")
    st.stop()

if result.get("error"):
    st.error(result["error"])
    st.stop()

section_divider()

# ── Decision header ──────────────────────────────────────────────────────────
decision_label = result.get("final_decision_label", "—")
decision_score = result.get("final_decision_score", 0)
risk_label = result.get("risk_level", "—")
confidence_label = result.get("confidence_label", "—")

col1, col2, col3, col4 = st.columns(4)
with col1:
    decision_badge(decision_label)
with col2:
    kpi_card("Decision Score", f"{decision_score:.0%}")
with col3:
    kpi_card("Risk", risk_label)
with col4:
    kpi_card("Confidence", confidence_label)

# ── Analyst summary ──────────────────────────────────────────────────────────
summary = result.get("analyst_summary", "")
if summary:
    st.markdown("")
    st.info(summary)

# ── Comparable films ─────────────────────────────────────────────────────────
section_divider()
section_header("Comparable Films")
comps = result.get("comparable_films", [])
if comps:
    import pandas as pd

    comp_rows = []
    for c in comps:
        comp_rows.append({
            "Title": c.get("title", "—"),
            "Year": c.get("release_year") or "—",
            "Similarity": f"{c.get('similarity', 0):.0%}",
            "IMDb": c.get("imdb_rating") or "—",
            "MENA Admissions": f"{c.get('total_mena_admissions', 0):,}",
            "Reason": c.get("reason", "—"),
        })
    st.dataframe(pd.DataFrame(comp_rows), use_container_width=True, hide_index=True)
else:
    empty_state("No comparable films found in the historical database.")

# ── Score breakdown ──────────────────────────────────────────────────────────
section_divider()
section_header("Score Breakdown")
col_s1, col_s2, col_s3 = st.columns(3)
with col_s1:
    kpi_card("Demand Potential", f"{result.get('demand_potential_score', 0):.0%}")
with col_s2:
    kpi_card("Marketing Dependency", f"{result.get('marketing_dependency_score', 0):.0%}")
with col_s3:
    kpi_card("Risk Score", f"{result.get('risk_score', 0):.0%}")

st.markdown("")
col_s4, col_s5, col_s6 = st.columns(3)
with col_s4:
    kpi_card("Target Realism", str(result.get("target_realism", "—")))
with col_s5:
    kpi_card("Spend Adequacy", str(result.get("spend_adequacy", "—")))
with col_s6:
    kpi_card("Confidence", f"{result.get('confidence_score', 0):.0%}")

# ── Forecast ─────────────────────────────────────────────────────────────────
section_divider()
section_header("First Watch Forecast")
forecast = result.get("forecast", {})
if forecast:
    import pandas as pd

    fc_rows = []
    for cc in ["EG", "AE", "SA", "MENA_TOTAL"]:
        fc = forecast.get(cc, {})
        fc_rows.append({
            "Market": MARKET_NAMES.get(cc, cc),
            "Floor": f"{fc.get('floor', 0):,}",
            "Base": f"{fc.get('base', 0):,}",
            "Stretch": f"{fc.get('stretch', 0):,}",
        })
    st.dataframe(pd.DataFrame(fc_rows), use_container_width=True, hide_index=True)

    target_realism = result.get("target_realism", "")
    mena_base = forecast.get("MENA_TOTAL", {}).get("base", 0)
    if target_realism and target_realism not in ("Not Specified", "Insufficient Forecast Data"):
        st.caption(f"Target realism: **{target_realism}** (vs MENA base {mena_base:,})")

# ── Comparable run shapes ────────────────────────────────────────────────────
if comps:
    with st.expander("Comparable Film Run Shapes", expanded=False):
        for c in comps:
            shapes = c.get("run_shapes", {})
            shares = c.get("market_shares", {})
            shape_parts = [f"{MARKET_NAMES.get(k, k)}: {v}" for k, v in shapes.items()]
            share_parts = [f"{MARKET_NAMES.get(k, k)}: {v:.0%}" for k, v in shares.items()]
            st.markdown(
                f"**{c.get('title', '—')}** — "
                f"Run: {', '.join(shape_parts) or '—'} | "
                f"Market mix: {', '.join(share_parts) or '—'}"
            )

# ── Raw JSON ─────────────────────────────────────────────────────────────────
with st.expander("Raw JSON payload", expanded=False):
    st.json(result)

# ── Meta ─────────────────────────────────────────────────────────────────────
meta = result.get("_meta", {})
if meta:
    st.caption(
        f"Historical films: {meta.get('historical_films_with_data', 0)} | "
        f"Comparables: {meta.get('comparables_found', 0)} | "
        f"Input completeness: {meta.get('input_completeness', 0):.0%}"
    )
