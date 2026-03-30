"""Unit tests for GS-parity resolved-title scoring and candidate-aware row acceptance."""
from __future__ import annotations

from src.services.resolved_title_scoring import (
    RELEASE_TITLE_FALLBACK_MIN,
    row_accept_with_parent,
    score_resolved_title,
)
from src.sources.boxofficemojo.parser import parse_release_page_evidence


def test_score_resolved_exact_latin_query_year_bonus() -> None:
    s = score_resolved_title("Siko Siko", "Siko Siko", None, 2025, 2025)
    assert s >= 0.98


def test_row_accept_strong_parent_keeps_slightly_noisy_row_title() -> None:
    assert row_accept_with_parent(
        "Siko Siko",
        2025,
        "Siko Siko (re-release)",
        None,
        2025,
        0.92,
        strong_threshold=0.88,
        review_threshold=0.70,
        moderate_threshold=0.70,
    )


def test_row_accept_strong_parent_drops_wrong_year() -> None:
    assert not row_accept_with_parent(
        "Siko Siko",
        2025,
        "Siko Siko",
        None,
        2019,
        0.92,
        strong_threshold=0.88,
        review_threshold=0.70,
        moderate_threshold=0.70,
    )


def test_row_accept_weak_parent_requires_row_strength() -> None:
    # Parent at review band edge should require row to clear review threshold.
    ok = row_accept_with_parent(
        "Siko Siko",
        2025,
        "Siko Siko",
        None,
        2025,
        0.72,
        strong_threshold=0.88,
        review_threshold=0.70,
        moderate_threshold=0.70,
    )
    assert ok


def test_bom_release_page_kept_when_parent_score_high_even_if_row_title_gate_narrow() -> None:
    # Page title does not hit title_matches_query threshold alone; parent from title discovery passes fallback.
    html = """
    <html><body>
    <h1># Odd Parser Title (2025) Title Summary</h1>
    <p>All Territories Egypt Grosses</p>
    <p>Grosses Egypt $1,234,567</p>
    <table>
    <tr><td>Apr 4-6</td><td>1</td><td>$10</td><td></td><td>$10</td></tr>
    </table>
    </body></html>
    """
    rows = parse_release_page_evidence(
        html,
        "https://www.boxofficemojo.com/release/rl999/weekend/",
        "Siko Siko",
        2025,
        parent_resolved_score=RELEASE_TITLE_FALLBACK_MIN + 0.02,
    )
    assert len(rows) >= 1
