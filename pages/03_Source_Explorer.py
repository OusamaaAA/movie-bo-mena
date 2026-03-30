"""Box Office Charts — separate tabs for Filmyard (daily), elCinema (weekly), and BOM (weekend)."""
import re
from collections import defaultdict
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from rapidfuzz import fuzz
from sqlalchemy import select

from src.db import session_scope
from src.models import NormalizedEvidence, RawEvidence
from src.services.semantics import (
    period_key_sort_ordinal,
    period_key_to_iso_week,
    period_key_to_start_date,
)
from src.services.text_utils import normalize_title_cross_language
from src.services.ticket_pricing import estimate_admissions, load_prices, price_basis_label
from ui_helpers import inject_global_css, market_name, format_gross, page_header, section_header

inject_global_css()

# ── constants ─────────────────────────────────────────────────────────────────

MARKET_NAMES = {
    "EG": "Egypt", "SA": "Saudi Arabia", "AE": "UAE",
    "KW": "Kuwait", "BH": "Bahrain", "QA": "Qatar",
    "OM": "Oman", "JO": "Jordan", "LB": "Lebanon",
}
# Which markets each source covers
SOURCE_MARKETS = {
    "Filmyard":       ["EG"],
    "elCinema":       ["EG", "SA"],
    "Box Office Mojo": ["EG", "SA", "AE", "KW", "BH", "QA", "OM", "JO", "LB"],
}
SOURCE_PREC = {"Filmyard": 100, "elCinema": 90, "Box Office Mojo": 70}


def _date_to_iso_week(d: date) -> str:
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _iso_week_label(iso_week: str) -> str:
    m = re.match(r"(\d{4})-W(\d+)$", iso_week)
    if not m:
        return iso_week
    year, week = int(m.group(1)), int(m.group(2))
    try:
        mon = date.fromisocalendar(year, week, 1)
        sun = date.fromisocalendar(year, week, 7)
        return f"Wk {week}  ({mon.strftime('%b %d')} – {sun.strftime('%b %d')}, {year})"
    except Exception:
        return iso_week


def _is_elcinema_source(source_name: str) -> bool:
    return (source_name or "").strip().lower() == "elcinema"


def _elcinema_native_ew_index(week_start: date) -> int:
    """elCinema box-office week index: week 1 starts 1 January (same grid as ingest)."""
    y = week_start.year
    return (week_start - date(y, 1, 1)).days // 7 + 1


def _elcinema_period_date_span(period_key: str) -> tuple[date, date] | None:
    """Resolve elCinema weekly row to an explicit (start, end) for matching legacy keys."""
    pk = period_key or ""
    m = re.match(r"^(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})$", pk)
    if m:
        try:
            return date.fromisoformat(m.group(1)), date.fromisoformat(m.group(2))
        except ValueError:
            pass
    d0 = period_key_to_start_date(pk)
    if d0 is None:
        return None
    return d0, d0 + timedelta(days=6)


def _elcinema_span_label(period_key: str, d0: date, d1: date) -> str:
    """Distinguish site-native EW weeks vs legacy ISO ``YYYY-Www`` keys (Mon–Sun)."""
    y = d0.year
    ew = _elcinema_native_ew_index(d0)
    span = f"{d0.strftime('%b %d')} – {d1.strftime('%b %d, %Y')}"
    base = f"{y} · EW{ew:02d} · {span}"
    pk = (period_key or "").strip()
    if re.match(r"^\d{4}-W\d+$", pk):
        base += " · legacy ISO key (Mon–Sun)"
    return base


def _chart_period_label(period_key: str, source_name: str) -> str:
    """Human label for week selector and trends (canonical date spans for elCinema)."""
    if _is_elcinema_source(source_name):
        sp = _elcinema_period_date_span(period_key or "")
        if sp:
            return _elcinema_span_label(period_key, sp[0], sp[1])
    iw = period_key_to_iso_week(period_key)
    if iw:
        return _iso_week_label(iw)
    return period_key or ""


def _dedupe_elcinema_period_keys(keys: list[str]) -> list[str]:
    """One selector entry per concrete 7-day window (legacy EW/W keys vs canonical span)."""
    by_span: dict[tuple[date, date], list[str]] = defaultdict(list)
    loose: list[str] = []
    for pk in keys:
        sp = _elcinema_period_date_span(pk)
        if sp:
            by_span[sp].append(pk)
        else:
            loose.append(pk)
    picked: list[str] = []
    for sp, variants in by_span.items():
        canonical = f"{sp[0].isoformat()}..{sp[1].isoformat()}"
        chosen = canonical if canonical in variants else sorted(variants)[0]
        picked.append(chosen)
    return sorted(loose + picked, key=period_key_sort_ordinal, reverse=True)


def _closest_period_key(available: list[str], jump: date) -> str | None:
    best: str | None = None
    best_delta: int | None = None
    for pk in available:
        d0 = period_key_to_start_date(pk)
        if d0 is None:
            continue
        delta = abs((d0 - jump).days)
        if best_delta is None or delta < best_delta:
            best, best_delta = pk, delta
    return best


def _belongs_to_week(pk: str, selected: str, *, source_name: str) -> bool:
    """Match chart rows to the selected period without conflating ISO Mon–Sun vs elCinema EW weeks."""
    if not pk or not selected:
        return False
    if pk == selected:
        return True
    if _is_elcinema_source(source_name):
        s1 = _elcinema_period_date_span(pk)
        s2 = _elcinema_period_date_span(selected)
        return bool(s1 and s2 and s1 == s2)
    t1 = period_key_to_iso_week(pk)
    t2 = period_key_to_iso_week(selected)
    return bool(t1 and t2 and t1 == t2)


# ── DB loaders (cached) ───────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def _available_iso_weeks(source_name: str, market_code: str) -> list[str]:
    """Chart periods in DB for this source+market, newest first.

    elCinema uses native box-office weeks from 1 Jan (EWnn), not ISO Mon–Sun; BOM/Filmyard use ISO or dates.
    """
    with session_scope() as s:
        pks = s.execute(
            select(RawEvidence.period_key)
            .where(
                RawEvidence.source_name == source_name,
                RawEvidence.country_code == market_code,
                RawEvidence.record_scope == "title",
                RawEvidence.period_gross_local > 0,
                RawEvidence.period_key.isnot(None),
                RawEvidence.period_key != "",
                RawEvidence.period_key != "lifetime",
            )
            .distinct()
        ).scalars().all()
    if _is_elcinema_source(source_name):
        keys = [pk for pk in pks if pk]
        return _dedupe_elcinema_period_keys(keys)
    weeks: set[str] = set()
    for pk in pks:
        w = period_key_to_iso_week(pk)
        if w:
            weeks.add(w)
    return sorted(weeks, key=period_key_sort_ordinal, reverse=True)


@st.cache_data(ttl=30)
def _load_rows(source_name: str, market_code: str) -> list[dict]:
    """All title-level RawEvidence with positive gross for source+market, with film_id."""
    with session_scope() as s:
        rows = s.execute(
            select(RawEvidence, NormalizedEvidence.film_id)
            .outerjoin(NormalizedEvidence, NormalizedEvidence.raw_evidence_id == RawEvidence.id)
            .where(
                RawEvidence.source_name == source_name,
                RawEvidence.country_code == market_code,
                RawEvidence.record_scope == "title",
                RawEvidence.period_gross_local > 0,
                RawEvidence.period_key.isnot(None),
                RawEvidence.period_key != "",
                RawEvidence.period_key != "lifetime",
            )
            .order_by(RawEvidence.period_gross_local.desc())
        ).all()
        return [
            {
                "source_name": r.source_name or "",
                "period_key": r.period_key or "",
                "record_granularity": r.record_granularity or "",
                "rank": r.rank,
                "film_title_raw": r.film_title_raw or "",
                "period_gross_local": float(r.period_gross_local or 0),
                "admissions_actual": float(r.admissions_actual) if r.admissions_actual is not None else None,
                "currency": r.currency or "",
                "film_id": film_id,
            }
            for r, film_id in rows
        ]


# ── deduplication (same film_id or fuzzy title match) ────────────────────────

def _dedup(rows: list[dict]) -> list[dict]:
    by_film: dict[str, list[dict]] = {}
    unmatched: list[dict] = []
    for r in rows:
        fid = r.get("film_id")
        if fid:
            by_film.setdefault(fid, []).append(r)
        else:
            unmatched.append(r)

    normed = [(normalize_title_cross_language(r["film_title_raw"]) or r["film_title_raw"].lower(), r) for r in unmatched]
    clusters: list[list[dict]] = []
    cluster_norms: list[str] = []
    for norm, record in normed:
        best_idx, best_score = -1, 0
        for i, cn in enumerate(cluster_norms):
            s = fuzz.ratio(norm, cn)
            if s > best_score:
                best_score, best_idx = s, i
        if best_score >= 80:
            clusters[best_idx].append(record)
        else:
            clusters.append([record])
            cluster_norms.append(norm)

    def _best(lst: list[dict]) -> dict:
        return max(lst, key=lambda r: (SOURCE_PREC.get(r["source_name"], 0), r["period_gross_local"]))

    result = [_best(v) for v in by_film.values()]
    result += [_best(c) for c in clusters]
    return sorted(result, key=lambda x: -x["period_gross_local"])


# ── shared chart + table renderer ────────────────────────────────────────────

def _render_chart(
    rows: list[dict],
    market_code: str,
    *,
    period_label: str = "",
    eg_price: float | None = None,
    top_n: int = 15,
) -> None:
    deduped = _dedup(rows)[:top_n]
    if not deduped:
        st.info(f"No data for {market_name(market_code)} — {period_label}")
        return

    currency = deduped[0]["currency"]
    prices = load_prices()

    # Bar chart
    titles = [r["film_title_raw"][:28] + "…" if len(r["film_title_raw"]) > 28 else r["film_title_raw"] for r in deduped]
    gross_vals = [r["period_gross_local"] for r in deduped]
    chart_df = pd.DataFrame({"Film": titles, f"Gross ({currency})": gross_vals}).set_index("Film")
    st.bar_chart(chart_df, use_container_width=True, height=320)

    # Table
    table_rows = []
    for i, r in enumerate(deduped, start=1):
        gross = r["period_gross_local"]
        adm = r.get("admissions_actual")
        if adm is not None and float(adm) > 0:
            adm_str = f"{int(adm):,} (actual)"
        else:
            price_ov = eg_price if market_code == "EG" else None
            gcur = (r.get("currency") or currency or "").strip() or None
            est = estimate_admissions(gross, market_code, price_ov, gross_currency=gcur)
            adm_str = f"~{est:,} (est.)" if est else "—"
        # After _dedup, rows are gross-sorted; DB rank can repeat (title-page legacy) or
        # disagree with gross order, so # must be display position only.
        table_rows.append({
            "#": i,
            "Film": r["film_title_raw"],
            "Gross": format_gross(gross, currency),
            "Admissions": adm_str,
        })
    st.dataframe(table_rows, use_container_width=True, hide_index=True)

    basis = price_basis_label(market_code, eg_price if market_code == "EG" else None)
    if basis != "no price data":
        st.caption(f"Admissions: {market_name(market_code)} ticket price — {basis}")


def _week_selector(source_name: str, market_code: str, key_prefix: str) -> str | None:
    """Render a week selectbox + date jump. Returns selected period key (or ISO week for BOM)."""
    available = _available_iso_weeks(source_name, market_code)
    if not available:
        st.info(f"No {source_name} data for {market_name(market_code)} yet. Run a fetch first.")
        return None

    col_week, col_date = st.columns([3, 2])
    with col_date:
        jump = st.date_input(
            "Jump to date",
            value=date.today(),
            key=f"{key_prefix}_date",
            help="Pick any date — auto-selects the closest chart period.",
        )

    with col_week:
        if _is_elcinema_source(source_name):
            options = [_chart_period_label(w, source_name) for w in available]
            closest = _closest_period_key(available, jump)
            default_pk = closest if closest in available else available[0]
            default_idx = available.index(default_pk)
        else:
            options = [_iso_week_label(w) for w in available]
            jumped = _date_to_iso_week(jump)
            default_idx = available.index(jumped) if jumped in available else 0
        chosen_label = st.selectbox("Week", options, index=default_idx, key=f"{key_prefix}_week")
        return available[options.index(chosen_label)]


def _trend_expander(source_name: str, market_code: str, label: str) -> None:
    with st.expander(f"Multi-week trend — {label}", expanded=False):
        all_rows = _load_rows(source_name, market_code)
        trend: dict[str, float] = {}
        for r in all_rows:
            pk = r["period_key"]
            if not pk:
                continue
            if _is_elcinema_source(source_name):
                sp = _elcinema_period_date_span(pk)
                canon = (
                    f"{sp[0].isoformat()}..{sp[1].isoformat()}" if sp else pk
                )
                trend[canon] = trend.get(canon, 0.0) + r["period_gross_local"]
            else:
                w = period_key_to_iso_week(pk)
                if w:
                    trend[w] = trend.get(w, 0.0) + r["period_gross_local"]
        if trend:
            sorted_keys = sorted(trend.keys(), key=period_key_sort_ordinal)
            if _is_elcinema_source(source_name):
                week_labels = [_chart_period_label(k, source_name) for k in sorted_keys]
            else:
                week_labels = [_iso_week_label(k) for k in sorted_keys]
            df = pd.DataFrame({
                "Week": week_labels,
                "Total Gross": [trend[k] for k in sorted_keys],
            }).set_index("Week")
            st.line_chart(df, y="Total Gross", use_container_width=True, height=200)
        else:
            st.caption("No data.")


# ── Egypt ticket price from DB ────────────────────────────────────────────────

@st.cache_data(ttl=600)
def _market_egypt_price() -> float | None:
    from src.repositories.report_repository import ReportRepository
    with session_scope() as s:
        return ReportRepository(s).egypt_avg_ticket_price()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE
# ═══════════════════════════════════════════════════════════════════════════════

page_header(
    "Box Office Charts",
    "Browse the box office chart for each data source. Sources use different time windows.",
    icon="📅",
)

eg_price = _market_egypt_price()
prices = load_prices()

tab_fy, tab_elc, tab_bom = st.tabs(["📅 Filmyard  (daily)", "📊 elCinema  (weekly)", "🌍 Box Office Mojo  (weekend)"])

# ═══════════════════════════════════════
# TAB 1 — FILMYARD (daily, Egypt only)
# ═══════════════════════════════════════
with tab_fy:
    st.markdown("**Source:** Filmyard Egypt — updates every day. Shows a single day's gross.")
    st.caption("Only Egypt is tracked on Filmyard.")

    # For Filmyard we show by date (each period_key = YYYY-MM-DD)
    @st.cache_data(ttl=120)
    def _filmyard_dates() -> list[str]:
        with session_scope() as s:
            pks = s.execute(
                select(RawEvidence.period_key)
                .where(
                    RawEvidence.source_name == "Filmyard",
                    RawEvidence.country_code == "EG",
                    RawEvidence.record_scope == "title",
                    RawEvidence.record_granularity.in_(["day", "daily"]),
                    RawEvidence.period_gross_local > 0,
                    RawEvidence.period_key.isnot(None),
                    RawEvidence.period_key != "",
                )
                .distinct()
            ).scalars().all()
        return sorted([pk for pk in pks if re.match(r"\d{4}-\d{2}-\d{2}$", pk)], reverse=True)

    fy_dates = _filmyard_dates()
    if not fy_dates:
        st.info("No Filmyard daily data yet. Run the daily fetch from Data Refresh.")
    else:
        col_d1, col_d2 = st.columns([3, 2])
        with col_d2:
            fy_jump = st.date_input("Jump to date", value=date.today(), key="fy_jump")
            fy_jump_str = fy_jump.isoformat()
        with col_d1:
            fy_date_labels = [
                f"{pk}  ({date.fromisoformat(pk).strftime('%A, %b %d')})"
                for pk in fy_dates
            ]
            fy_default = fy_dates.index(fy_jump_str) if fy_jump_str in fy_dates else 0
            fy_sel_label = st.selectbox("Date", fy_date_labels, index=fy_default, key="fy_date_sel")
            fy_sel_date = fy_dates[fy_date_labels.index(fy_sel_label)]

        st.markdown(f"**Egypt — {date.fromisoformat(fy_sel_date).strftime('%A, %B %d, %Y')}**")

        @st.cache_data(ttl=120)
        def _fy_rows_for_date(d: str) -> list[dict]:
            with session_scope() as s:
                rows = s.execute(
                    select(RawEvidence, NormalizedEvidence.film_id)
                    .outerjoin(NormalizedEvidence, NormalizedEvidence.raw_evidence_id == RawEvidence.id)
                    .where(
                        RawEvidence.source_name == "Filmyard",
                        RawEvidence.country_code == "EG",
                        RawEvidence.record_scope == "title",
                        RawEvidence.record_granularity.in_(["day", "daily"]),
                        RawEvidence.period_key == d,
                        RawEvidence.period_gross_local > 0,
                    )
                    .order_by(RawEvidence.rank.asc().nulls_last(), RawEvidence.period_gross_local.desc())
                ).all()
                return [
                    {
                        "source_name": r.source_name,
                        "period_key": r.period_key,
                        "record_granularity": r.record_granularity,
                        "rank": r.rank,
                        "film_title_raw": r.film_title_raw or "",
                        "period_gross_local": float(r.period_gross_local or 0),
                        "cumulative_gross_local": float(r.cumulative_gross_local) if r.cumulative_gross_local else None,
                        "admissions_actual": float(r.admissions_actual) if r.admissions_actual is not None else None,
                        "currency": r.currency or "EGP",
                        "film_id": film_id,
                    }
                    for r, film_id in rows
                ]

        fy_rows = _fy_rows_for_date(fy_sel_date)

        # Bar chart
        top_n = min(len(fy_rows), 15)
        if fy_rows:
            titles = [r["film_title_raw"][:28] for r in fy_rows[:top_n]]
            gross_vals = [r["period_gross_local"] for r in fy_rows[:top_n]]
            chart_df = pd.DataFrame({"Film": titles, "Daily Gross (EGP)": gross_vals}).set_index("Film")
            st.bar_chart(chart_df, use_container_width=True, height=320)

            table = []
            for i, r in enumerate(fy_rows[:top_n], start=1):
                gross = r["period_gross_local"]
                adm = r.get("admissions_actual")
                cumul = r.get("cumulative_gross_local")
                if adm is not None and float(adm) > 0:
                    adm_str = f"{int(adm):,} (actual)"
                else:
                    est = estimate_admissions(gross, "EG", eg_price)
                    adm_str = f"~{est:,} (est.)" if est else "—"
                row = {
                    "#": r.get("rank") or i,
                    "Film": r["film_title_raw"],
                    "Daily Gross": format_gross(gross, "EGP"),
                    "Admissions": adm_str,
                }
                if cumul and cumul > 0:
                    row["Cumulative"] = format_gross(cumul, "EGP")
                table.append(row)
            st.dataframe(table, use_container_width=True, hide_index=True)
            if eg_price:
                st.caption(f"Egypt ticket price (Filmyard-derived market avg): ~{eg_price:,.0f} EGP")

        # Multi-day trend in expander
        with st.expander("Daily trend — last 30 days", expanded=False):
            @st.cache_data(ttl=120)
            def _fy_daily_trend() -> list[dict]:
                cutoff = (date.today() - timedelta(days=30)).isoformat()
                with session_scope() as s:
                    rows = s.execute(
                        select(RawEvidence.period_key, RawEvidence.period_gross_local)
                        .where(
                            RawEvidence.source_name == "Filmyard",
                            RawEvidence.country_code == "EG",
                            RawEvidence.record_scope == "title",
                            RawEvidence.record_granularity.in_(["day", "daily"]),
                            RawEvidence.period_gross_local > 0,
                            RawEvidence.period_key >= cutoff,
                        )
                    ).all()
                    return [{"period_key": pk, "gross": float(g or 0)} for pk, g in rows]

            trend_rows = _fy_daily_trend()
            by_date: dict[str, float] = {}
            for r in trend_rows:
                by_date[r["period_key"]] = by_date.get(r["period_key"], 0.0) + r["gross"]
            if by_date:
                sorted_dates = sorted(by_date.keys())
                trend_df = pd.DataFrame({
                    "Date": [date.fromisoformat(d).strftime("%b %d") for d in sorted_dates],
                    "Total Daily Gross (EGP)": [by_date[d] for d in sorted_dates],
                }).set_index("Date")
                st.line_chart(trend_df, y="Total Daily Gross (EGP)", use_container_width=True, height=200)
            else:
                st.caption("No data.")


# ═══════════════════════════════════════
# TAB 2 — ELCINEMA (weekly, EG + SA)
# ═══════════════════════════════════════
with tab_elc:
    st.markdown("**Source:** elCinema — weekly chart, updated every week.")
    st.caption(
        "elCinema weeks are **box-office weeks (EW01 = 1–7 Jan; consecutive 7-day blocks)** — not ISO Monday–Sunday weeks. "
        "If you see two lines with similar dates, one may be tagged **legacy ISO key** from older ingest; the **EWnn** label matches the site."
    )

    elc_market_opts = ["Egypt (EG)", "Saudi Arabia (SA)"]
    elc_market_sel = st.radio("Market", elc_market_opts, horizontal=True, key="elc_market")
    elc_code = "EG" if "EG" in elc_market_sel else "SA"

    elc_week = _week_selector("elCinema", elc_code, key_prefix=f"elc_{elc_code}")
    if elc_week:
        all_elc = _load_rows("elCinema", elc_code)
        week_elc = [r for r in all_elc if _belongs_to_week(r["period_key"], elc_week, source_name="elCinema")]

        elc_label = _chart_period_label(elc_week, "elCinema")
        st.markdown(f"**{market_name(elc_code)} — {elc_label}**")
        _render_chart(week_elc, elc_code, period_label=elc_label,
                      eg_price=eg_price if elc_code == "EG" else None)

        if not week_elc:
            avail_wks = _available_iso_weeks("elCinema", elc_code)
            if avail_wks:
                st.caption(
                    "Available periods: "
                    + ", ".join(_chart_period_label(w, "elCinema") for w in avail_wks[:6])
                )

        _trend_expander("elCinema", elc_code, label=f"{market_name(elc_code)} (elCinema)")


# ═══════════════════════════════════════
# TAB 3 — BOM (weekend, all markets)
# ═══════════════════════════════════════
with tab_bom:
    st.markdown("**Source:** Box Office Mojo — weekend chart (Fri–Sun), updated weekly.")

    bom_markets = ["EG", "SA", "AE", "KW", "BH", "QA", "OM", "JO", "LB"]
    bom_market_sel = st.selectbox(
        "Market",
        [market_name(c) for c in bom_markets],
        key="bom_market",
    )
    bom_code = next(c for c in bom_markets if market_name(c) == bom_market_sel)

    bom_week = _week_selector("Box Office Mojo", bom_code, key_prefix=f"bom_{bom_code}")
    if bom_week:
        all_bom = _load_rows("Box Office Mojo", bom_code)
        week_bom = [r for r in all_bom if _belongs_to_week(r["period_key"], bom_week, source_name="Box Office Mojo")]

        st.markdown(f"**{market_name(bom_code)} — {_iso_week_label(bom_week)}** *(weekend)*")
        st.caption(
            "Box Office Mojo weekend gross is often **USD**. Estimated admissions convert USD → "
            "local ticket currency (e.g. **EGP** for Egypt) before dividing by the ticket baseline. "
            "Override **EGP_PER_USD** (and similar) in the environment if you need a fresher FX rate."
        )
        _render_chart(week_bom, bom_code, period_label=_iso_week_label(bom_week))

        if not week_bom:
            avail_wks = _available_iso_weeks("Box Office Mojo", bom_code)
            if avail_wks:
                st.caption(f"Available weeks: {', '.join(_iso_week_label(w) for w in avail_wks[:6])}")

        _trend_expander("Box Office Mojo", bom_code, label=f"{market_name(bom_code)} (BOM)")
