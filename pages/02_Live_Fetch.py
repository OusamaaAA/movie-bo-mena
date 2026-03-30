"""Live Data Fetch — fetches box office data for new films not yet in the database."""
import re

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db import session_scope
from src.models import NormalizedEvidence, RawEvidence
from src.repositories.report_repository import ReportRepository
from src.services.acquisition_lookup import perform_acquisition_lookup
from src.services.period_display import dedupe_bom_weekend_rows, format_period, format_period_row
from src.services.ratings_service import (
    fetch_elcinema_rating,
    fetch_imdb_rating,
    fetch_letterboxd_rating,
    save_ratings_to_db,
)
from src.services.ratings_display import format_ratings_line
from src.services.report_builder import build_film_report
from src.services.semantics import period_key_sort_ordinal, period_key_to_iso_week
from src.services.ticket_pricing import BASELINE_TICKET_PRICES, estimate_admissions, load_prices, price_basis_label
from ui_helpers import (
    inject_global_css,
    market_name,
    format_gross,
    page_header,
    section_divider,
    section_header,
    kpi_card,
    confidence_indicator,
    empty_state,
    MARKET_NAMES,
)

inject_global_css()
page_header(
    "Live Data Fetch",
    "Search and ingest a film from live sources (elCinema, Box Office Mojo, Filmyard). "
    "For films already in the database, use Film Report.",
    icon="🔍",
)

# ── helpers ──────────────────────────────────────────────────────────────────


def _filmyard_egg_ticket_price(reconciled: list[dict]) -> float | None:
    """Derive Egypt ticket price from any Filmyard rows in reconciled that have admissions."""
    prices = []
    for r in reconciled:
        if (r.get("source") or "").lower() != "filmyard":
            continue
        if (r.get("country_code") or "").upper() != "EG":
            continue
        gross = float(r.get("period_gross_local") or 0)
        adm = r.get("admissions_actual")
        if adm is None or gross <= 0:
            continue
        adm_f = float(adm)
        if adm_f > 0:
            prices.append(gross / adm_f)
    if not prices:
        return None
    prices.sort()
    n = len(prices)
    mid = n // 2
    return prices[mid] if n % 2 == 1 else (prices[mid - 1] + prices[mid]) / 2


def _build_market_totals(reconciled: list[dict], selected_codes: set[str], eg_price: float | None) -> list[dict]:
    totals: dict[str, dict] = {}
    for r in reconciled:
        if r.get("semantics") != "title_period_gross" or r.get("period_key") == "lifetime":
            continue
        code = r.get("country_code") or ""
        if code not in selected_codes:
            continue
        gross = float(r.get("period_gross_local") or 0)
        currency = r.get("currency") or ""
        adm = r.get("admissions_actual")
        if code not in totals:
            totals[code] = {"total_gross": 0.0, "currency": currency, "weeks": 0,
                            "latest_pk": "", "total_adm": 0.0, "has_actual_adm": False}
        totals[code]["total_gross"] += gross
        totals[code]["weeks"] += 1
        if adm is not None:
            totals[code]["total_adm"] += float(adm)
            totals[code]["has_actual_adm"] = True
        pk = str(r.get("period_key") or "")
        if period_key_sort_ordinal(pk) > period_key_sort_ordinal(totals[code]["latest_pk"]):
            totals[code]["latest_pk"] = pk

    rows = []
    for code, t in sorted(totals.items(), key=lambda x: x[1]["total_gross"], reverse=True):
        if t["has_actual_adm"] and t["total_adm"] > 0:
            adm_str = f"{int(t['total_adm']):,} (actual)"
        else:
            price_override = eg_price if code.upper() == "EG" else None
            gcur = (t.get("currency") or "").strip() or None
            est = estimate_admissions(t["total_gross"], code, price_override, gross_currency=gcur)
            adm_str = f"~{est:,} (est.)" if est else "-"
        rows.append({
            "Market": market_name(code),
            "Total Gross": format_gross(t["total_gross"], t["currency"]),
            "Periods Tracked": t["weeks"],
            "Latest Period": format_period(t["latest_pk"]),
            "Est. Admissions": adm_str,
        })
    return rows


def _build_chart_data(reconciled: list[dict], country_code: str) -> pd.DataFrame | None:
    rows = [
        r for r in reconciled
        if r.get("semantics") == "title_period_gross"
        and r.get("period_key") != "lifetime"
        and (r.get("country_code") or "").upper() == country_code.upper()
    ]
    if not rows:
        return None
    records = [
        {
            "label": format_period_row(r),
            "ordinal": period_key_sort_ordinal(str(r.get("period_key") or "")),
            "gross": float(r.get("period_gross_local") or 0),
        }
        for r in rows
    ]
    df = pd.DataFrame(records).sort_values("ordinal").drop_duplicates("label")
    return df.set_index("label")[["gross"]]


def _detect_stored_ids(session, film_id: str) -> tuple[str | None, str | None]:
    """Return (imdb_title_id, elcinema_work_id) from existing evidence for this film."""
    imdb_id: str | None = None
    elc_id: str | None = None
    raw_rows = session.execute(
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


# ── Search form ──────────────────────────────────────────────────────────────
with st.form("search_form"):
    col1, col2 = st.columns([3, 1])
    with col1:
        query = st.text_input("Film title", placeholder="e.g. Siko Siko")
    with col2:
        year_hint = st.number_input("Year (optional)", min_value=0, max_value=2100, value=0, step=1,
                                    help="Leave 0 to search across all years.")
    submitted = st.form_submit_button("Search", use_container_width=True, type="primary")

if not submitted:
    empty_state("Enter a film title and click Search. The system will fetch data from all available sources.")
    st.stop()

if not query or not query.strip():
    st.warning("Please enter a film title.")
    st.stop()

release_year_hint = int(year_hint) if year_hint and year_hint > 0 else None

film_dict: dict | None = None
result = None
market_eg_price: float | None = None

with st.spinner(f"Looking up '{query}'... this may take 30-60 seconds on first search."):
    try:
        with session_scope() as session:
            result = perform_acquisition_lookup(
                session,
                query=query.strip(),
                release_year_hint=release_year_hint,
                imdb_title_id=None,
            )
            market_eg_price: float | None = ReportRepository(session).egypt_avg_ticket_price()
            if result.resolved_film is not None:
                _f = result.resolved_film
                film_dict: dict | None = {
                    "id": str(_f.id),
                    "canonical_title": str(_f.canonical_title or ""),
                    "canonical_title_ar": str(_f.canonical_title_ar or "") if _f.canonical_title_ar else None,
                    "release_year": int(_f.release_year) if _f.release_year else None,
                }
                # Live ratings step
                detected_imdb, detected_elc = _detect_stored_ids(session, str(_f.id))
                ratings_payload: list[dict] = []
                r_imdb, _ = fetch_imdb_rating(film_dict["canonical_title"], film_dict["release_year"], imdb_id=detected_imdb)
                if r_imdb and r_imdb.get("rating") is not None:
                    ratings_payload.append(r_imdb)
                r_elc, _ = fetch_elcinema_rating(film_dict["canonical_title"], film_dict["release_year"], work_id=detected_elc)
                if r_elc and r_elc.get("rating") is not None:
                    ratings_payload.append(r_elc)
                r_lb, _ = fetch_letterboxd_rating(film_dict["canonical_title"], film_dict["release_year"], slug=None)
                if r_lb and r_lb.get("rating") is not None:
                    ratings_payload.append(r_lb)
                ratings_saved = save_ratings_to_db(session, str(_f.id), ratings_payload) if ratings_payload else 0
                result.report = build_film_report(ReportRepository(session), _f)
                st.caption(f"Live ratings refresh: saved {ratings_saved} source record{'s' if ratings_saved != 1 else ''}.")
            else:
                film_dict = None
    except Exception as exc:
        st.warning(
            f"No data found for **'{query}'** — this film may not be tracked yet in any of our sources "
            "(Filmyard, elCinema, Box Office Mojo, IMDb)."
        )
        with st.expander("Technical details", expanded=False):
            st.code(str(exc))
        st.stop()

# ── Results ──────────────────────────────────────────────────────────────────

if film_dict is None:
    st.warning(
        f"No match found for **'{query}'**. "
        "Try a different spelling or add a release year."
    )
    st.stop()

film = film_dict
st.success(f"Found: **{film['canonical_title']}** ({film['release_year'] or 'year unknown'})")

if not result.report:
    st.info("Film matched but no box office data available yet.")
    st.stop()

report = result.report
reconciled = dedupe_bom_weekend_rows(report.reconciled or [])

ratings_line = format_ratings_line(report.ratings or [])
if ratings_line:
    st.caption(ratings_line)

# ── Source coverage ──────────────────────────────────────────────────────────
section_divider()
coverage_after = result.coverage_after or []
if coverage_after:
    src_cols = st.columns(len(coverage_after))
    for i, c in enumerate(coverage_after):
        src = c.get("source_name") or "-"
        rows_count = c.get("matched_title_rows") or 0
        with src_cols[i]:
            if rows_count:
                kpi_card(src, str(rows_count), "rows matched")
            else:
                kpi_card(src, "0", "no data")

# Action notes
warning_actions = [a for a in (result.actions_taken or []) if a.get("type") == "warning"]
critical_types = {"elcinema_title_lookup_empty", "finalize_unresolved", "missing_title_evidence"}
has_critical = any(w.get("warning_type") in critical_types for w in warning_actions)
if warning_actions:
    with st.expander("Data notes", expanded=has_critical):
        for w in warning_actions:
            msg = w.get("message") or str(w)
            st.caption(f"• {msg}")

# ── Market filter ────────────────────────────────────────────────────────────
title_period_rows_all = [
    r for r in reconciled
    if r.get("semantics") == "title_period_gross" and r.get("period_key") != "lifetime"
]
all_codes = sorted({(r.get("country_code") or "") for r in title_period_rows_all if r.get("country_code")})
all_market_labels = [market_name(c) for c in all_codes]
code_to_label = {c: market_name(c) for c in all_codes}
label_to_code = {v: k for k, v in code_to_label.items()}

if all_codes:
    selected_labels = st.multiselect("Markets to display", all_market_labels, default=all_market_labels)
    selected_codes = {label_to_code[l] for l in selected_labels}
else:
    selected_codes = set(all_codes)

# ── Market totals ────────────────────────────────────────────────────────────
section_header("Box Office Summary")
film_eg_price = _filmyard_egg_ticket_price(reconciled)
eg_price = market_eg_price or film_eg_price

totals = _build_market_totals(reconciled, selected_codes, eg_price)
if totals:
    st.dataframe(totals, use_container_width=True, hide_index=True)
    price_notes = []
    prices = load_prices()
    for code in sorted(selected_codes):
        if code in prices or (code.upper() == "EG" and eg_price):
            basis = price_basis_label(code, eg_price if code.upper() == "EG" else None)
            price_notes.append(f"{market_name(code)}: {basis}")
    if price_notes:
        st.caption("Admission estimates based on — " + " · ".join(price_notes))
else:
    empty_state("No box office data available.")

# ── Per-market performance charts ────────────────────────────────────────────
if selected_codes:
    section_divider()
    section_header("Week-by-Week Performance")
    for code in sorted(selected_codes):
        df = _build_chart_data(reconciled, code)
        if df is None or df.empty:
            continue
        currency = next(
            (r.get("currency") or "" for r in title_period_rows_all
             if (r.get("country_code") or "").upper() == code.upper()),
            ""
        )
        label = f"{market_name(code)} ({currency})" if currency else market_name(code)
        with st.container():
            st.markdown(f"**{label}**")
            df.columns = ["Gross"]
            st.line_chart(df, y="Gross", use_container_width=True, height=200)

# ── Acquisition recommendation ───────────────────────────────────────────────
section_divider()
section_header("Acquisition Recommendation")
overall = float(report.score.get("overall") or 0)
confidence_indicator(overall, 100)
st.write(report.analyst_explanation)
