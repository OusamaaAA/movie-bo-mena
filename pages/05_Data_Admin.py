"""Data Refresh — trigger data updates and monitor source status."""
from datetime import date

import streamlit as st
from sqlalchemy import func, select

from sqlalchemy import delete, update

from src.db import session_scope
from src.models import NormalizedEvidence, RawEvidence, ReconciledEvidence, Source, SourceRun
from src.services.ingestion_service import run_backfill, run_bom_backfill_year_range_job, run_daily, run_weekly
from src.services.ticket_pricing import load_prices, save_prices
from ui_helpers import (
    inject_global_css,
    page_header,
    section_divider,
    section_header,
    kpi_card,
)

inject_global_css()
page_header("Data Refresh", "Trigger data updates from external sources and check their status.", icon="🔄")

# ── Status overview ──────────────────────────────────────────────────────────
section_header("Source Status")

with session_scope() as session:
    row_counts = session.execute(
        select(RawEvidence.source_name, func.count(RawEvidence.id))
        .group_by(RawEvidence.source_name)
        .order_by(func.count(RawEvidence.id).desc())
    ).all()

    latest_runs = {}
    for run_type_filter, label in [("daily", "Daily"), ("weekly", "Weekly")]:
        run = session.execute(
            select(SourceRun)
            .order_by(SourceRun.started_at.desc())
            .where(SourceRun.run_type == run_type_filter)
            .limit(1)
        ).scalars().first()
        if run:
            latest_runs[label] = {
                "started_at": str(run.started_at)[:16] if run.started_at else "-",
                "status": run.status or "-",
                "fetched": run.fetched_count or 0,
            }

    failure_rows = [
        {
            "started_at": str(r.started_at)[:16] if r.started_at else "-",
            "run_type": r.run_type or "-",
            "error_message": r.error_message or "no message",
        }
        for r in session.execute(
            select(SourceRun).where(SourceRun.status == "failed").order_by(SourceRun.started_at.desc()).limit(5)
        ).scalars().all()
    ]

# Source record counts
if row_counts:
    cols = st.columns(len(row_counts))
    for i, (source_name, cnt) in enumerate(row_counts):
        with cols[i]:
            kpi_card(source_name, f"{cnt:,}", "records in DB")

st.markdown("")

# Latest runs
run_info = [
    {
        "Fetch Type": label,
        "Last Run": run["started_at"],
        "Status": run["status"],
        "Records Fetched": run["fetched"],
    }
    for label, run in latest_runs.items()
]
if run_info:
    st.dataframe(run_info, use_container_width=True, hide_index=True)

if failure_rows:
    with st.expander(f"Recent failures ({len(failure_rows)})", expanded=False):
        for f in failure_rows:
            st.error(f"[{f['started_at']}] {f['run_type']}: {f['error_message']}")

section_divider()

# ── Trigger updates ──────────────────────────────────────────────────────────
section_header("Trigger Updates")

col1, col2 = st.columns(2)
with col1:
    with st.container():
        st.markdown("**Daily fetch** — Filmyard admissions")
        if st.button("Run daily fetch", use_container_width=True, type="primary"):
            with st.spinner("Running daily fetch..."):
                with session_scope() as session:
                    results = run_daily(session)
            fetched = sum(r.get("fetched", 0) for r in results if isinstance(r, dict))
            normalized = sum(r.get("normalized", 0) for r in results if isinstance(r, dict))
            ratings = sum(r.get("ratings_refreshed", 0) for r in results if isinstance(r, dict))
            st.success(f"Fetched: {fetched} · Normalized: {normalized} · Ratings: {ratings}")
with col2:
    with st.container():
        st.markdown("**Weekly fetch** — elCinema + Box Office Mojo")
        if st.button("Run weekly fetch", use_container_width=True, type="primary"):
            with st.spinner("Running weekly fetch..."):
                with session_scope() as session:
                    results = run_weekly(session)
            fetched = sum(r.get("fetched", 0) for r in results if isinstance(r, dict))
            normalized = sum(r.get("normalized", 0) for r in results if isinstance(r, dict))
            reconciled = sum(r.get("reconciled", 0) for r in results if isinstance(r, dict))
            msg = f"Fetched: {fetched} · Normalized: {normalized} · Reconciled: {reconciled}"
            for r in results:
                if isinstance(r, dict) and r.get("bom_period_repair"):
                    msg += f" · BOM repair: {r['bom_period_repair']}"
            st.success(msg)

section_divider()

# ── Backfill ─────────────────────────────────────────────────────────────────
section_header("Backfill Historical Data")

with st.expander("Backfill by days (all sources)", expanded=False):
    st.caption(
        "**elCinema:** Fetches EG + SA weekly charts for every ISO week that overlaps "
        "the selected date range. Existing rows refresh; new films insert."
    )
    col3, col4 = st.columns(2)
    with col3:
        bf_source = st.selectbox("Source", ["filmyard", "elcinema", "bom"], key="bf_src")
    with col4:
        bf_days = st.number_input("Days back", min_value=1, max_value=3650, value=90, key="bf_days")
    if st.button("Run backfill", key="run_bf", use_container_width=True):
        with st.spinner(f"Backfilling {bf_source} ({bf_days} days)..."):
            with session_scope() as session:
                result = run_backfill(session, source_code=bf_source, days=int(bf_days))
        msg = f"Fetched: {result.get('fetched', '?')} · Normalized: {result.get('normalized', '?')} · Reconciled: {result.get('reconciled', '?')}"
        if result.get("bom_period_repair"):
            msg += f" · BOM repair: {result['bom_period_repair']}"
        st.success(msg)

with st.expander("Box Office Mojo year-range backfill", expanded=False):
    col5, col6, col7 = st.columns(3)
    with col5:
        bom_start = st.number_input("Start year", min_value=1990, max_value=2100, value=2020, key="bom_start")
    with col6:
        bom_end = st.number_input("End year", min_value=1990, max_value=2100, value=date.today().year, key="bom_end")
    with col7:
        bom_markets = st.text_input("Markets", value="AE,SA,EG,KW,BH,LB,QA,OM,JO", key="bom_mkt")
    if st.button("Run BOM backfill", key="run_bom_bf", use_container_width=True):
        market_codes = [m.strip().upper() for m in bom_markets.split(",") if m.strip()]
        with st.spinner(f"Backfilling BOM {bom_start}–{bom_end}..."):
            with session_scope() as session:
                result = run_bom_backfill_year_range_job(session, start_year=int(bom_start), end_year=int(bom_end), market_codes=market_codes)
        msg = f"Fetched: {result.get('fetched', '?')} · Normalized: {result.get('normalized', '?')} · Reconciled: {result.get('reconciled', '?')}"
        if result.get("bom_period_repair"):
            msg += f" · BOM repair: {result['bom_period_repair']}"
        st.success(msg)

section_divider()

# ── Ticket prices ────────────────────────────────────────────────────────────
section_header("Ticket Price Baselines")
st.caption(
    "Used to estimate admissions when actual ticket counts aren't available. "
    "Egypt is auto-derived from Filmyard data when possible — the value below is the fallback."
)

MARKET_LABELS = {
    "EG": "Egypt (EGP)", "SA": "Saudi Arabia (SAR)", "AE": "UAE (AED)",
    "KW": "Kuwait (KWD)", "BH": "Bahrain (BHD)", "QA": "Qatar (QAR)",
    "OM": "Oman (OMR)", "JO": "Jordan (JOD)",
}

current_prices = load_prices()
price_editor_rows = [
    {"Market": MARKET_LABELS.get(code, code), "Code": code, "Avg Ticket Price": price}
    for code, price in sorted(current_prices.items())
    if code in MARKET_LABELS
]

edited = st.data_editor(
    price_editor_rows,
    column_config={
        "Market": st.column_config.TextColumn("Market", disabled=True),
        "Code": st.column_config.TextColumn("Code", disabled=True, width="small"),
        "Avg Ticket Price": st.column_config.NumberColumn(
            "Avg Ticket Price (local currency)",
            min_value=0.1, max_value=10000.0, step=0.5, format="%.2f",
        ),
    },
    use_container_width=True,
    hide_index=True,
    key="price_editor",
)

if st.button("Save Ticket Prices", type="primary"):
    new_prices = {row["Code"]: float(row["Avg Ticket Price"]) for row in edited}
    save_prices(new_prices)
    st.success("Ticket prices saved.")
    st.rerun()

section_divider()

# ── Database cleanup ─────────────────────────────────────────────────────────
section_header("Database Cleanup")
st.caption("One-time fixes for known data quality issues. Safe to run multiple times.")

with st.expander("Fix empty period_key records (elCinema)", expanded=True):
    st.markdown(
        "elCinema records fetched before the week-detection fix have an empty `period_key`. "
        "This deletes them so they'll be recreated correctly on the next weekly fetch."
    )

    with session_scope() as s:
        bad_count = s.execute(
            select(func.count(RawEvidence.id)).where(
                RawEvidence.source_name == "elCinema",
                RawEvidence.period_key == "",
            )
        ).scalar() or 0

    st.info(f"Found **{bad_count}** elCinema records with empty period_key.")

    if bad_count > 0 and st.button("Delete empty-period_key elCinema records", type="primary"):
        with session_scope() as s:
            bad_ids = s.execute(
                select(RawEvidence.id).where(
                    RawEvidence.source_name == "elCinema",
                    RawEvidence.period_key == "",
                )
            ).scalars().all()
            if bad_ids:
                s.execute(delete(NormalizedEvidence).where(NormalizedEvidence.raw_evidence_id.in_(bad_ids)))
                s.execute(delete(RawEvidence).where(RawEvidence.id.in_(bad_ids)))
                s.commit()
        st.success(f"Deleted {len(bad_ids)} records. Run Weekly Fetch to repopulate.")
        st.rerun()

with st.expander("Fix BOM records with wrong year (future dates)", expanded=True):
    st.markdown(
        "Every BOM ingest runs the repair pipeline: align weekend dates to the year in "
        "`weekend_code`, then shift remaining future periods back one year."
    )

    today = date.today()
    with session_scope() as s:
        bom_future_count = s.execute(
            select(func.count(RawEvidence.id)).where(
                RawEvidence.source_name == "Box Office Mojo",
                RawEvidence.period_start_date > today,
            )
        ).scalar() or 0
        bom_norm_future_count = s.execute(
            select(func.count(NormalizedEvidence.id)).where(
                NormalizedEvidence.source_name == "Box Office Mojo",
                NormalizedEvidence.period_start_date > today,
            )
        ).scalar() or 0
    st.info(
        f"Rows needing repair: **{bom_future_count}** raw · **{bom_norm_future_count}** "
        "normalized with future dates (usually zero after a weekly fetch)."
    )

    if st.button("Run BOM period repair now", type="primary", key="bom_repair_now"):
        from src.services.bom_period_repair import repair_all_bom_period_issues  # noqa: PLC0415

        with session_scope() as s:
            stats = repair_all_bom_period_issues(s)
            s.commit()
        st.success(f"Repair complete: {stats}")
        st.rerun()

with st.expander("Fix zero-gross Filmyard weekly records", expanded=False):
    st.markdown(
        "Filmyard weekly records with `period_gross_local = 0` from the old ingest. "
        "Deleting them prevents 'EGP 0' in reports."
    )

    with session_scope() as s:
        fy_zero_count = s.execute(
            select(func.count(RawEvidence.id)).where(
                RawEvidence.source_name == "Filmyard",
                RawEvidence.record_granularity.in_(["week", "weekly"]),
                RawEvidence.period_gross_local == 0,
            )
        ).scalar() or 0

    st.info(f"Found **{fy_zero_count}** zero-gross Filmyard weekly records.")

    if fy_zero_count > 0 and st.button("Delete zero-gross Filmyard weekly records"):
        with session_scope() as s:
            fy_ids = s.execute(
                select(RawEvidence.id).where(
                    RawEvidence.source_name == "Filmyard",
                    RawEvidence.record_granularity.in_(["week", "weekly"]),
                    RawEvidence.period_gross_local == 0,
                )
            ).scalars().all()
            if fy_ids:
                s.execute(delete(NormalizedEvidence).where(NormalizedEvidence.raw_evidence_id.in_(fy_ids)))
                s.execute(delete(RawEvidence).where(RawEvidence.id.in_(fy_ids)))
                s.commit()
        st.success(f"Deleted {len(fy_ids)} records.")
        st.rerun()
