"""One-off probe: compare parser yield vs HTML link patterns for old chart weeks."""
import re
import sys

from src.sources.elcinema.client import ElCinemaClient
from src.sources.elcinema.parser import parse_current_chart

year, week = int(sys.argv[1]), int(sys.argv[2])
market = sys.argv[3] if len(sys.argv) > 3 else "EG"

c = ElCinemaClient()
html = c.fetch_boxoffice_chart_week(year, week, market)
print("html_len", len(html))
pat_dq = len(re.findall(r'href="/en/work/\d+/', html))
pat_sq = len(re.findall(r"href='/en/work/\d+/", html))
pat_flex = len(
    re.findall(r'href=(["\'])(?:/en)?/work/(\d+)/\1', html)
)
wk = re.search(r"Weekly Revenue", html, re.I)
print("double_quote /en/work count", pat_dq)
print("single_quote /en/work count", pat_sq)
print("flex matching pairs", pat_flex)
print("'Weekly Revenue' in html", bool(wk))
rows = parse_current_chart(
    html,
    f"probe?year={year}&week={week}",
    country_code=market,
    fallback_date=None,
)
print("parsed", len(rows), "sample titles", [r.film_title_raw[:30] for r in rows[:5]])
