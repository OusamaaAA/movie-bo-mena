"""Shared UI design system — consistent styling, layout helpers, and reusable components.

This module provides a unified look-and-feel across all pages without
introducing external dependencies beyond Streamlit itself.
"""
from __future__ import annotations

import streamlit as st

# ── Constants ────────────────────────────────────────────────────────────────

MARKET_NAMES: dict[str, str] = {
    "EG": "Egypt",
    "SA": "Saudi Arabia",
    "AE": "UAE",
    "KW": "Kuwait",
    "BH": "Bahrain",
    "QA": "Qatar",
    "OM": "Oman",
    "JO": "Jordan",
    "LB": "Lebanon",
}


def market_name(code: str) -> str:
    return MARKET_NAMES.get((code or "").upper(), code or "-")


def format_gross(value: float, currency: str = "") -> str:
    prefix = f"{currency} " if currency else ""
    if value >= 1_000_000:
        return f"{prefix}{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{prefix}{value / 1_000:.0f}K"
    return f"{prefix}{value:.0f}"


# ── Global CSS injection ────────────────────────────────────────────────────

_CSS = """
<style>
/* ── Page-wide typography refinements ─────────────────────────────────── */
[data-testid="stAppViewContainer"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}
[data-testid="stAppViewContainer"] h1,
[data-testid="stAppViewContainer"] h2,
[data-testid="stAppViewContainer"] h3,
[data-testid="stAppViewContainer"] h4,
[data-testid="stAppViewContainer"] h5,
[data-testid="stAppViewContainer"] h6 {
    color: #ffffff !important;
}
[data-testid="stAppViewContainer"] p,
[data-testid="stAppViewContainer"] label,
[data-testid="stAppViewContainer"] span,
[data-testid="stAppViewContainer"] li {
    color: #e8edf8;
}
[data-testid="stCaptionContainer"] {
    color: #c8d2e7 !important;
}

/* ── Section headers ─────────────────────────────────────────────────── */
.section-header {
    font-size: 1.1rem;
    font-weight: 600;
    margin: 1.4rem 0 0.4rem 0;
    padding-bottom: 0.3rem;
    border-bottom: 1px solid rgba(128,128,128,0.2);
    letter-spacing: -0.01em;
    color: #ffffff;
}

/* ── KPI metric cards — transparent, no box ─────────────────────────── */
.kpi-card {
    padding: 0.6rem 0;
    text-align: left;
    animation: riseIn 0.45s ease-out both;
}
.kpi-label {
    font-size: 0.75rem;
    font-weight: 500;
    opacity: 0.85;
    color: #d5e0f2;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    margin-bottom: 0.2rem;
}
.kpi-value {
    font-size: 1.65rem;
    font-weight: 700;
    line-height: 1.2;
    color: #ffffff;
}
.kpi-sub {
    font-size: 0.72rem;
    opacity: 0.75;
    margin-top: 0.1rem;
    color: #c7d4eb;
}

/* ── Decision badges ─────────────────────────────────────────────────── */
.decision-badge {
    display: inline-block;
    padding: 0.35rem 0.9rem;
    border-radius: 6px;
    font-weight: 700;
    font-size: 0.95rem;
    letter-spacing: 0.02em;
}
.decision-green  { background: #d4edda; color: #155724; }
.decision-blue   { background: #d1ecf1; color: #0c5460; }
.decision-yellow { background: #fff3cd; color: #856404; }
.decision-red    { background: #f8d7da; color: #721c24; }

/* ── Score indicators ────────────────────────────────────────────────── */
.score-pill {
    display: inline-block;
    padding: 0.15rem 0.6rem;
    border-radius: 999px;
    font-size: 0.8rem;
    font-weight: 600;
}
.score-high   { background: #d4edda; color: #155724; }
.score-medium { background: #fff3cd; color: #856404; }
.score-low    { background: #f8d7da; color: #721c24; }

/* ── Streamlit metric — remove default white box ─────────────────────── */
[data-testid="stMetric"] {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    padding: 0.5rem 0 !important;
}

/* ── Dataframe styling ───────────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    border-radius: 8px;
    overflow: hidden;
}

/* ── Homepage nav cards — minimal, blends in ─────────────────────────── */
.nav-card {
    padding: 0.8rem 0.2rem;
    height: 100%;
    border-radius: 10px;
    transition: transform .2s ease, box-shadow .2s ease, background-color .2s ease;
    animation: fadeInUp 0.45s ease-out both;
}
.nav-card:hover {
    transform: translateY(-2px);
    background: rgba(255, 255, 255, 0.04);
    box-shadow: 0 8px 20px rgba(0, 0, 0, 0.15);
}
.nav-card h4 {
    color: #ffffff;
    font-size: 1.05rem;
    font-weight: 600;
    margin: 0.5rem 0 0.4rem 0;
}
.nav-card p {
    color: #cad7ee;
    font-size: 0.85rem;
    margin: 0;
    line-height: 1.4;
}

/* ── Empty state styling ─────────────────────────────────────────────── */
.empty-state {
    text-align: center;
    padding: 2rem;
    color: #b9c6de;
    font-size: 0.9rem;
}

/* ── Form button consistency ─────────────────────────────────────────── */
.stButton > button[kind="primary"] {
    border-radius: 8px;
    font-weight: 600;
    transition: transform .15s ease, box-shadow .15s ease;
}
.stButton > button[kind="secondary"] {
    border-radius: 8px;
    transition: transform .15s ease, box-shadow .15s ease;
}
.stButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 6px 14px rgba(0, 0, 0, 0.18);
}

/* ── Reduce top padding in main view ─────────────────────────────────── */
.block-container { padding-top: 2rem; }

/* ── Tab panel refinements ───────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0.5rem;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0;
    font-weight: 500;
}
/* ── Subtle motion system ────────────────────────────────────────────── */
@keyframes fadeInUp {
    from { opacity: 0; transform: translateY(8px); }
    to { opacity: 1; transform: translateY(0); }
}
@keyframes riseIn {
    from { opacity: 0; transform: translateY(4px); }
    to { opacity: 1; transform: translateY(0); }
}
</style>
"""


def inject_global_css() -> None:
    """Call once at the top of every page to apply the design system."""
    st.markdown(_CSS, unsafe_allow_html=True)


# ── Page header ──────────────────────────────────────────────────────────────

def page_header(title: str, subtitle: str = "", icon: str = "") -> None:
    """Render a consistent page header with optional icon and subtitle."""
    display = f"{icon}  {title}" if icon else title
    st.markdown(f"## {display}")
    if subtitle:
        st.caption(subtitle)
    st.markdown("")  # spacing


# ── Section divider ──────────────────────────────────────────────────────────

def section_divider() -> None:
    """Consistent section break."""
    st.markdown("---")


def section_header(text: str) -> None:
    """Styled sub-section header with underline."""
    st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)


# ── KPI card ─────────────────────────────────────────────────────────────────

def kpi_card(label: str, value: str, sub: str = "") -> None:
    """Render a styled KPI card using HTML/CSS."""
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    st.markdown(
        f'<div class="kpi-card">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'{sub_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


# ── Decision badge ───────────────────────────────────────────────────────────

DECISION_STYLES: dict[str, str] = {
    "STRONG BUY": "decision-green",
    "BUY": "decision-green",
    "GREENLIGHT": "decision-green",
    "STRONG ACQUISITION CASE": "decision-green",
    "GOOD BUT SELECTIVE": "decision-blue",
    "CONSIDER": "decision-yellow",
    "SPEND-SENSITIVE OPPORTUNITY": "decision-yellow",
    "RISKY": "decision-red",
    "RISKY PROJECT": "decision-red",
    "PASS": "decision-red",
    "WEAK COMMERCIAL CASE": "decision-red",
    "CAUTION": "decision-red",
}


def decision_badge(decision: str) -> None:
    """Render a color-coded decision badge."""
    style_class = DECISION_STYLES.get(decision.upper().strip(), "decision-yellow")
    st.markdown(
        f'<div class="decision-badge {style_class}">{decision}</div>',
        unsafe_allow_html=True,
    )


# ── Score pill ───────────────────────────────────────────────────────────────

def score_pill(value: float, label: str = "") -> str:
    """Return HTML for a color-coded score pill."""
    if value >= 0.70:
        cls = "score-high"
    elif value >= 0.45:
        cls = "score-medium"
    else:
        cls = "score-low"
    text = f"{label} {value:.0%}" if label else f"{value:.0%}"
    return f'<span class="score-pill {cls}">{text}</span>'


# ── Score color emoji ────────────────────────────────────────────────────────

def score_color(v: float) -> str:
    if v >= 0.70:
        return "🟢"
    if v >= 0.45:
        return "🟡"
    return "🔴"


# ── Empty state ──────────────────────────────────────────────────────────────

def empty_state(message: str) -> None:
    """Render a centered empty-state message."""
    st.markdown(f'<div class="empty-state">{message}</div>', unsafe_allow_html=True)


# ── Result container ─────────────────────────────────────────────────────────

def result_container_start() -> None:
    st.markdown('<div class="result-container">', unsafe_allow_html=True)


def result_container_end() -> None:
    st.markdown('</div>', unsafe_allow_html=True)


# ── Confidence bar ───────────────────────────────────────────────────────────

def confidence_indicator(score: float, max_score: float = 100) -> None:
    """Show a visual confidence indicator with color-coded status."""
    pct = score / max_score if max_score else 0
    if pct >= 0.70:
        st.success(f"**High confidence** — score {score:.0f}/{max_score:.0f}")
    elif pct >= 0.50:
        st.warning(f"**Moderate confidence** — score {score:.0f}/{max_score:.0f}")
    else:
        st.error(f"**Low confidence** — score {score:.0f}/{max_score:.0f}")


# ── Nav card for homepage ────────────────────────────────────────────────────

def nav_card(icon: str, title: str, description: str) -> None:
    """Render a styled navigation card (use inside st.columns)."""
    st.markdown(
        f'<div class="nav-card">'
        f'<span style="font-size:1.6rem">{icon}</span>'
        f'<h4>{title}</h4>'
        f'<p>{description}</p>'
        f'</div>',
        unsafe_allow_html=True,
    )
