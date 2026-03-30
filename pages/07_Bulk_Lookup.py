"""Bulk Lookup — durable queue for multi-title live acquisition + ratings refresh."""
from __future__ import annotations

import re
from datetime import datetime, timezone

import streamlit as st

from src.db import session_scope
from src.models import BulkLookupBatch, BulkLookupItem
from src.services.acquisition_lookup import perform_acquisition_lookup
from src.services.bulk_lookup_queue import (
    claim_next_item,
    create_batch,
    get_batch,
    list_items,
    mark_item_completed,
    mark_item_failed,
    recompute_batch_counters,
    requeue_stuck_running_items,
    retry_failed_items,
)
from src.services.live_ratings import refresh_live_ratings_for_film
from ui_helpers import (
    inject_global_css,
    page_header,
    section_divider,
    section_header,
    kpi_card,
    empty_state,
)

inject_global_css()
page_header(
    "Bulk Acquisition Lookup",
    "Queue multiple titles for the full live workflow. Each title runs acquisition lookup, "
    "then ratings refresh. Queue state is persisted in DB.",
    icon="📦",
)


def _parse_lines(text: str) -> list[tuple[str, int | None, dict | None]]:
    """Parse bulk input lines. Returns (title, year_hint, meta) tuples."""
    out: list[tuple[str, int | None, dict | None]] = []
    seen: set[tuple[str, int | None]] = set()
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        elc_hint: str | None = None
        pipe_match = re.search(r"\|\s*elcinema\s*:\s*(\d+)", line, re.I)
        if pipe_match:
            elc_hint = pipe_match.group(1)
            line = (line[: pipe_match.start()] + line[pipe_match.end() :]).strip().rstrip("|, ")

        m = re.match(r"^(.+?)[,\s]+\(?((?:19|20)\d{2})\)?$", line)
        if m:
            title = m.group(1).strip()
            year: int | None = int(m.group(2))
        else:
            title = line
            year = None

        key = (title.lower().strip(), year)
        if title and key not in seen:
            seen.add(key)
            meta: dict | None = {"elcinema_work_id_hint": elc_hint} if elc_hint else None
            out.append((title, year, meta))
    return out


def _item_status_label(item: BulkLookupItem) -> str:
    if item.status == "completed":
        return "Completed" if item.resolved_film_id else "Completed (no match)"
    if item.status == "running":
        return "Running"
    if item.status in ("failed", "retryable"):
        return "Failed (retryable)"
    return "Queued"


def _item_status_icon(item: BulkLookupItem) -> str:
    if item.status == "completed":
        return "✅" if item.resolved_film_id else "⚠️"
    if item.status == "running":
        return "⏳"
    if item.status in ("failed", "retryable"):
        return "❌"
    return "🕒"


def _coverage_from_stage_debug(actions_taken: list[dict], coverage_after: list[dict] | None = None) -> str:
    stages: dict = {}
    for action in actions_taken or []:
        if action.get("type") == "stage_ingestion_debug":
            stages = action.get("stages") or {}
            break
    if not stages:
        cov = coverage_after or []
        if cov:
            return ", ".join(
                f"{c.get('source_name')} ({c.get('matched_title_rows') or 0})"
                for c in cov
                if (c.get("matched_title_rows") or 0) > 0
            ) or "no data"
        return "no stage debug"
    elc_rows = int((stages.get("elcinema_title") or {}).get("rows_ingested") or 0)
    bom_title_rows = int((stages.get("bom_title") or {}).get("rows_ingested") or 0)
    bom_release_rows = int((stages.get("bom_release") or {}).get("rows_ingested") or 0)
    bom_rows = bom_title_rows + bom_release_rows
    chunks = [
        f"elCinema={elc_rows}",
        f"BOM={bom_rows}",
    ]
    return " · ".join(chunks)


def _dt_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _dt_iso_utc(dt: datetime | None) -> str:
    d = _dt_utc(dt)
    return d.isoformat() if d else ""


if "bulk_batch_id" not in st.session_state:
    st.session_state["bulk_batch_id"] = None

try:
    need_rerun = False
    with session_scope() as session:
        active_batch_id = st.session_state.get("bulk_batch_id")
        active_batch = get_batch(session, active_batch_id) if active_batch_id else None

    # ── Input form when no active running batch ──────────────────────────────
        if not active_batch or active_batch.status == "completed":
            with st.form("bulk_form"):
                raw_input = st.text_area(
                    "Film titles (one per line)",
                    height=200,
                    placeholder=(
                        "Siko Siko, 2025\n"
                        "Bershama\n"
                        "Al Sada Al Afadel | elcinema:2095663\n"
                        "# Lines starting with # are ignored"
                    ),
                )
                submitted = st.form_submit_button("Start Bulk Lookup", type="primary", use_container_width=True)
            if submitted:
                entries = _parse_lines(raw_input or "")
                if not entries:
                    st.warning("No valid titles found.")
                else:
                    batch = create_batch(session, entries)
                    st.session_state["bulk_batch_id"] = batch.id
                    need_rerun = True

        active_batch_id = st.session_state.get("bulk_batch_id")
        active_batch = get_batch(session, active_batch_id) if active_batch_id else None
        if not active_batch:
            st.stop()

        # Recover stale "running" items
        recovered = requeue_stuck_running_items(session, active_batch.id, stale_after_seconds=90)
        if recovered:
            recompute_batch_counters(session, active_batch.id)
            st.warning(f"Recovered {recovered} interrupted item(s).")

    # ── Controls ─────────────────────────────────────────────────────────────
        section_divider()
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Pause queue", disabled=(active_batch.status != "running"), use_container_width=True):
                active_batch.status = "paused"
                need_rerun = True
        with c2:
            if st.button("Resume queue", disabled=(active_batch.status != "paused"), use_container_width=True):
                active_batch.status = "running"
                need_rerun = True
        with c3:
            if st.button("Retry failed items", use_container_width=True):
                touched = retry_failed_items(session, active_batch.id)
                recompute_batch_counters(session, active_batch.id)
                st.info(f"Marked {touched} item(s) for retry.")
                need_rerun = True

    # ── Process one queue item per rerun ─────────────────────────────────────
        if active_batch.status == "running":
            next_item = claim_next_item(session, active_batch.id)
            if next_item is not None:
                with st.spinner(f"Processing #{next_item.queue_index}: {next_item.query_text}"):
                    try:
                        item_meta = next_item.meta_json or {}
                        elc_hint = item_meta.get("elcinema_work_id_hint") or None
                        result = perform_acquisition_lookup(
                            session,
                            query=next_item.query_text,
                            release_year_hint=next_item.release_year_hint,
                            imdb_title_id=None,
                            elcinema_work_id_hint=elc_hint,
                        )
                        matched_title = None
                        resolved_film_id = None
                        if result.resolved_film is not None:
                            resolved_film_id = str(result.resolved_film.id)
                            matched_title = str(result.resolved_film.canonical_title or next_item.query_text)
                        coverage_str = _coverage_from_stage_debug(result.actions_taken or [], result.coverage_after or [])
                        if elc_hint:
                            coverage_str = f"[hint elcinema:{elc_hint}] " + coverage_str

                        ratings_summary = "not-run"
                        ratings_saved = 0
                        if resolved_film_id and matched_title:
                            rr = refresh_live_ratings_for_film(
                                session,
                                film_id=resolved_film_id,
                                film_title=matched_title,
                                film_year=next_item.release_year_hint,
                            )
                            ratings_saved = int(rr.get("saved") or 0)
                            ratings_summary = f"saved={ratings_saved} ({'; '.join(rr.get('status') or [])})"

                        lookup_job_id = None
                        mark_item_completed(
                            session,
                            item_id=next_item.id,
                            resolved_film_id=resolved_film_id,
                            matched_title=matched_title,
                            coverage_summary=coverage_str,
                            ratings_summary=ratings_summary,
                            lookup_job_id=lookup_job_id,
                            meta_json={"ratings_saved": ratings_saved},
                        )
                    except Exception as exc:  # noqa: BLE001
                        mark_item_failed(
                            session,
                            item_id=next_item.id,
                            error_message=str(exc),
                            retryable=True,
                        )
                recompute_batch_counters(session, active_batch.id)
                need_rerun = True

            recompute_batch_counters(session, active_batch.id)
            active_batch = get_batch(session, active_batch.id)

    # ── Progress & status ────────────────────────────────────────────────────
        items = list_items(session, active_batch.id)
        batch = recompute_batch_counters(session, active_batch.id)
        if batch:
            done = int(batch.processed_items or 0)
            total = int(batch.total_items or 0)
            progress = (done / total) if total else 0.0
            st.progress(progress, text=f"{done}/{total} processed")

            queued_n = sum(1 for i in items if i.status == "queued")
            running_n = sum(1 for i in items if i.status == "running")
            completed_n = sum(1 for i in items if i.status == "completed")
            retryable_n = sum(1 for i in items if i.status in ("failed", "retryable"))

            m1, m2, m3, m4 = st.columns(4)
            with m1:
                kpi_card("Queued", str(queued_n))
            with m2:
                kpi_card("Running", str(running_n))
            with m3:
                kpi_card("Completed", str(completed_n), f"{batch.success_items or 0} matched")
            with m4:
                kpi_card("Failed", str(retryable_n))

            running_item = next((i for i in items if i.status == "running"), None)
            if running_item:
                st.info(
                    f"Currently processing #{running_item.queue_index}: "
                    f"**{running_item.query_text}**"
                    + (f" ({running_item.release_year_hint})" if running_item.release_year_hint else "")
                )

            last_touch = max((_dt_utc(i.updated_at) for i in items), default=None)
            if last_touch:
                st.caption(f"Last activity: {last_touch.isoformat()} UTC")
            if batch.status == "completed":
                st.success("Batch completed.")
            elif batch.status == "paused":
                st.warning("Batch is paused.")

    # ── Queue results table ──────────────────────────────────────────────────
        section_divider()
        section_header("Queue Results")
        rows = [
            {
                "#": i.queue_index,
                "Title": i.query_text,
                "Year": i.release_year_hint or "—",
                "Status": f"{_item_status_icon(i)} {_item_status_label(i)}",
                "Matched As": i.matched_title or "—",
                "Coverage": i.coverage_summary or "—",
                "Ratings": i.ratings_summary or "—",
                "Attempts": i.attempts,
                "Error": i.error_message or "",
            }
            for i in items
        ]
        st.dataframe(rows, use_container_width=True, hide_index=True)

        with st.expander("Recent activity", expanded=False):
            recent_rows = [
                {
                    "#": i.queue_index,
                    "Title": i.query_text,
                    "Status": i.status,
                    "Attempts": i.attempts,
                    "Updated (UTC)": _dt_iso_utc(i.updated_at),
                    "Error": i.error_message or "",
                }
                for i in sorted(items, key=lambda x: _dt_utc(x.updated_at or x.created_at) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)[:10]
            ]
            st.dataframe(recent_rows, use_container_width=True, hide_index=True)

        if batch and batch.status == "completed":
            st.markdown("")
            if st.button("Start new batch", use_container_width=True):
                st.session_state["bulk_batch_id"] = None
                need_rerun = True
    if need_rerun:
        st.rerun()
except Exception as exc:  # noqa: BLE001
    st.error("Bulk queue encountered an error.")
    msg = str(exc).lower()
    if "bulk_lookup_" in msg and ("does not exist" in msg or "undefined table" in msg):
        st.error(
            "Bulk queue tables are not available yet. Apply migration "
            "`migrations/006_bulk_lookup_queue.sql`, then refresh this page."
        )
    with st.expander("Technical details", expanded=False):
        st.code(str(exc))
