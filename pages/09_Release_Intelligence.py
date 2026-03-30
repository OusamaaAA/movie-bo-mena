"""Release Intelligence — decision-grade film analysis for MENA release planning."""
from __future__ import annotations

import streamlit as st
from sqlalchemy import select

from src.db import session_scope
from src.models import Film
from src.repositories.film_repository import FilmRepository
from src.repositories.report_repository import ReportRepository
from src.services.report_builder import build_film_report
from ui_helpers import (
    inject_global_css,
    page_header,
    section_divider,
    section_header,
    kpi_card,
    score_color,
    empty_state,
)

inject_global_css()
page_header(
    "Release Intelligence",
    "Commercial potential, market performance, audience validation, confidence, "
    "and forecast bands — derived from historical box-office evidence and ratings.",
    icon="🧠",
)

# ── Film selector ────────────────────────────────────────────────────────────
with session_scope() as session:
    films = session.execute(
        select(Film).order_by(Film.canonical_title)
    ).scalars().all()
    film_options = {
        f.canonical_title + (f" ({f.release_year})" if f.release_year else ""): f.id
        for f in films
    }

if not film_options:
    empty_state("No films in the database yet.")
    st.stop()

selected_label = st.selectbox("Select film", list(film_options.keys()))
film_id = film_options[selected_label]

# ── Build report ─────────────────────────────────────────────────────────────
with session_scope() as session:
    film = FilmRepository(session).get(film_id)
    if not film:
        st.error("Film not found.")
        st.stop()
    repo = ReportRepository(session)
    report = build_film_report(repo, film)

intel = report.release_intelligence
if not intel or not isinstance(intel, dict):
    st.warning("No release intelligence available. Not enough reconciled evidence yet.")
    st.stop()


# ── Helpers ──────────────────────────────────────────────────────────────────
def _pct(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v * 100:.0f}%"


def _num(v: float | int | None) -> str:
    if v is None or v == 0:
        return "—"
    if isinstance(v, float) and v == int(v):
        v = int(v)
    if isinstance(v, int):
        return f"{v:,}"
    return f"{v:,.0f}"


MARKET_NAMES = {"EG": "Egypt", "AE": "UAE", "SA": "Saudi Arabia"}


# ── Headline scores ──────────────────────────────────────────────────────────
section_divider()
section_header("Headline Scores")

cp = intel.get("commercial_potential_score", 0)
col1, col2, col3 = st.columns(3)
with col1:
    kpi_card(
        "Commercial Potential",
        _pct(cp),
        "Composite score blending market, audience, interest, confidence",
    )
with col2:
    kpi_card(
        "Core Market Performance",
        _pct(intel.get("core_market_performance_score")),
    )
with col3:
    conf = intel.get("confidence_score", 0)
    kpi_card(
        f"Confidence ({intel.get('confidence_label', '?')})",
        _pct(conf),
    )

st.markdown("")
col4, col5, col6 = st.columns(3)
with col4:
    kpi_card("Audience Validation", _pct(intel.get("audience_validation_score")))
with col5:
    kpi_card("Interest Heat", _pct(intel.get("interest_heat_score")))
with col6:
    kpi_card("Benchmark Usefulness", _pct(intel.get("benchmark_usefulness_score")))


# ── Analyst commentary ───────────────────────────────────────────────────────
commentary = intel.get("analyst_commentary", "")
if commentary:
    section_divider()
    section_header("Analyst Commentary")
    st.info(commentary)


# ── Market breakdown ─────────────────────────────────────────────────────────
mkt_breakdown = intel.get("market_breakdown", {})
if mkt_breakdown:
    section_divider()
    section_header("Market-by-Market Analysis")

    for cc in ["EG", "AE", "SA"] + sorted(k for k in mkt_breakdown if k not in ("EG", "AE", "SA")):
        mkt = mkt_breakdown.get(cc)
        if not mkt:
            continue
        name = MARKET_NAMES.get(cc, cc)
        ms = mkt.get("market_score", 0)
        with st.expander(
            f"{score_color(ms)} **{name}** — Market Score: {_pct(ms)}  ·  {mkt.get('run_shape_label', '?')}",
            expanded=(cc in ("EG", "AE", "SA")),
        ):
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                kpi_card("Opening Admissions", _num(mkt.get("opening_admissions")))
            with c2:
                kpi_card("Total Admissions", _num(mkt.get("tracked_total_admissions")))
            with c3:
                ccy = mkt.get("currency", "")
                kpi_card("Opening Gross", f"{ccy} {_num(mkt.get('opening_gross'))}")
            with c4:
                kpi_card("Total Gross", f"{ccy} {_num(mkt.get('tracked_total_gross'))}")

            st.caption(f"Periods: {mkt.get('period_count', 0)}  ·  Evidence quality: {_pct(mkt.get('evidence_quality'))}")

            # Dimension scores
            dims = [
                ("Opening Power", mkt.get("opening_power_score", 0), "25%"),
                ("Peak Strength", mkt.get("peak_strength_score", 0), "10%"),
                ("Tracked Volume", mkt.get("tracked_volume_strength_score", 0), "20%"),
                ("Hold Quality", mkt.get("hold_quality_score", 0), "20%"),
                ("Run Depth", mkt.get("run_depth_score", 0), "15%"),
                ("Stability", mkt.get("stability_score", 0), "10%"),
            ]
            dim_table = [
                {
                    "Dimension": d[0],
                    "Score": _pct(d[1]),
                    "Weight": d[2],
                    "Signal": score_color(d[1]),
                }
                for d in dims
            ]
            st.dataframe(dim_table, use_container_width=True, hide_index=True)


# ── Forecast bands ───────────────────────────────────────────────────────────
forecast = intel.get("forecast", {})
has_forecast = any(v.get("base", 0) > 0 for v in forecast.values())
if has_forecast:
    section_divider()
    section_header("First-Watch Forecast Bands")
    st.caption(
        "Admissions ranges derived from historical distributions, "
        "mapped through the film's market scores and adjusted for confidence."
    )

    forecast_table = []
    for cc in ["EG", "AE", "SA", "MENA_TOTAL"]:
        band = forecast.get(cc)
        if not band or band.get("base", 0) == 0:
            continue
        name = MARKET_NAMES.get(cc, cc)
        if cc == "MENA_TOTAL":
            name = "MENA Total"
        forecast_table.append({
            "Market": name,
            "Floor": _num(band.get("floor")),
            "Base": _num(band.get("base")),
            "Stretch": _num(band.get("stretch")),
        })

    if forecast_table:
        st.dataframe(forecast_table, use_container_width=True, hide_index=True)
    else:
        empty_state("Insufficient historical data to build forecast bands.")


# ── Raw payload ──────────────────────────────────────────────────────────────
with st.expander("Raw intelligence payload (JSON)", expanded=False):
    st.json(intel)
