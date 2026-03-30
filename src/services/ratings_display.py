"""Format stored rating metrics for UI (Film Report, Live Fetch, etc.)."""

from __future__ import annotations


def format_ratings_line(ratings: list[dict]) -> str | None:
    """Build one caption line: ``Source: x.x/10 · n votes`` per source, joined by `` · ``.

    Each row should include ``source_name`` and ``rating_value`` (as in report ``ratings``).
    """
    if not ratings:
        return None
    parts: list[str] = []
    for r in sorted(ratings, key=lambda x: str(x.get("source_name") or "").lower()):
        src = (r.get("source_name") or "?").strip()
        try:
            rv = float(r.get("rating_value") or 0)
        except (TypeError, ValueError):
            continue
        if rv <= 0:
            continue
        votes = r.get("vote_count")
        vote_str = ""
        if votes is not None:
            try:
                vi = int(float(votes))
                if vi > 0:
                    vote_str = f" · {vi:,} votes"
            except (TypeError, ValueError):
                pass
        parts.append(f"{src}: **{rv:.1f}/10**{vote_str}")
    return "  ·  ".join(parts) if parts else None
