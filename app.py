import streamlit as st

st.set_page_config(page_title="Hub", layout="wide")

from ui_helpers import inject_global_css, nav_card, page_header, section_divider

inject_global_css()

# ── Header ───────────────────────────────────────────────────────────────────
page_header(
    "Hub",
    "Box office, marketing, ratings, and investment analysis across Egypt, Saudi Arabia, UAE, and MENA.",
)

section_divider()

# ── Row 1: Core workflows ────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

with col1:
    nav_card(
        "📊", "Film Performance",
        "View film box office, ratings, and run Investment Analysis (predict first watch + required spend).",
    )
    st.page_link("pages/01_Film_Report.py", label="Open Film Performance", icon="📊")

with col2:
    nav_card(
        "🔍", "Live Film Fetch",
        "Search and ingest a title from live sources, then generate/update the in-app film report.",
    )
    st.page_link("pages/02_Live_Fetch.py", label="Open Live Film Fetch", icon="🔍")

with col3:
    nav_card(
        "📅", "Weekly Charts",
        "Browse box office rankings across all tracked markets — Egypt, Saudi Arabia, UAE, and more.",
    )
    st.page_link("pages/03_Source_Explorer.py", label="Open Weekly Charts", icon="📅")

st.markdown("")

# ── Row 2: Data management ───────────────────────────────────────────────────
col4, col5, col6 = st.columns(3)

with col4:
    nav_card(
        "✅", "Match Review",
        "Review films flagged for manual confirmation — approve a match, create a new entry, or ignore.",
    )
    st.page_link("pages/04_Review_Queue.py", label="Open Match Review", icon="✅")

with col5:
    nav_card(
        "📝", "Film Performance Input",
        "Enter marketing spend, first watch outcomes, and ratings for EG/SA/AE historical modeling.",
    )
    st.page_link("pages/11_Film_Performance_Input.py", label="Open Film Performance Input", icon="📝")

with col6:
    nav_card(
        "🔄", "Data Refresh",
        "Trigger manual data updates or backfills. Check the last time each source was updated.",
    )
    st.page_link("pages/05_Data_Admin.py", label="Open Data Refresh", icon="🔄")

st.markdown("")

# ── Row 3: Intelligence & tools ──────────────────────────────────────────────
col7, col8, col9 = st.columns(3)

with col7:
    nav_card(
        "⭐", "Ratings Lookup",
        "Fetch IMDb, elCinema, and Letterboxd ratings for one film or bulk-fetch for all films.",
    )
    st.page_link("pages/08_Ratings_Lookup.py", label="Open Ratings Lookup", icon="⭐")

with col8:
    nav_card(
        "📦", "Bulk Lookup",
        "Run queued lookup jobs at scale to discover, ingest, and reconcile films in batches.",
    )
    st.page_link("pages/07_Bulk_Lookup.py", label="Open Bulk Lookup", icon="📦")

with col9:
    nav_card(
        "🧠", "Release Intelligence",
        "Inspect release-level intelligence and evaluation outputs for acquisition and planning.",
    )
    st.page_link("pages/09_Release_Intelligence.py", label="Open Release Intelligence", icon="🧠")
