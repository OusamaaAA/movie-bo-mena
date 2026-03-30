from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import BulkLookupBatch, BulkLookupItem


def create_batch(
    session: Session,
    entries: "list[tuple[str, int | None]] | list[tuple[str, int | None, dict | None]]",
) -> BulkLookupBatch:
    batch = BulkLookupBatch(
        status="running",
        total_items=len(entries),
        processed_items=0,
        success_items=0,
        failed_items=0,
        started_at=datetime.utcnow(),
    )
    session.add(batch)
    session.flush()
    for idx, entry in enumerate(entries, start=1):
        title = entry[0]
        year_hint = entry[1] if len(entry) > 1 else None
        meta_init = entry[2] if len(entry) > 2 else None  # type: ignore[misc]
        session.add(
            BulkLookupItem(
                batch_id=batch.id,
                queue_index=idx,
                query_text=title,
                release_year_hint=year_hint,
                status="queued",
                attempts=0,
                meta_json=meta_init or {},
            )
        )
    session.flush()
    return batch


def get_batch(session: Session, batch_id: str) -> BulkLookupBatch | None:
    return session.get(BulkLookupBatch, batch_id)


def list_items(session: Session, batch_id: str) -> list[BulkLookupItem]:
    return list(
        session.execute(
            select(BulkLookupItem)
            .where(BulkLookupItem.batch_id == batch_id)
            .order_by(BulkLookupItem.queue_index.asc())
        ).scalars().all()
    )


def claim_next_item(session: Session, batch_id: str) -> BulkLookupItem | None:
    item = session.execute(
        select(BulkLookupItem)
        .where(
            BulkLookupItem.batch_id == batch_id,
            BulkLookupItem.status.in_(["queued", "retryable"]),
        )
        .order_by(BulkLookupItem.queue_index.asc())
        .limit(1)
    ).scalar_one_or_none()
    if not item:
        return None
    item.status = "running"
    item.attempts = int(item.attempts or 0) + 1
    item.updated_at = datetime.utcnow()
    session.flush()
    return item


def requeue_stuck_running_items(session: Session, batch_id: str, *, stale_after_seconds: int = 90) -> int:
    """
    Recover items left in 'running' after an app crash/rerun failure.
    Marks stale running items as retryable so the queue can continue.
    """
    now_utc = datetime.now(timezone.utc)
    items = list(
        session.execute(
            select(BulkLookupItem).where(
                BulkLookupItem.batch_id == batch_id,
                BulkLookupItem.status == "running",
            )
        ).scalars().all()
    )
    touched = 0
    for item in items:
        updated = item.updated_at or item.created_at
        if updated is None:
            item.status = "retryable"
            item.error_message = "Recovered from interrupted run."
            item.updated_at = datetime.utcnow()
            touched += 1
            continue
        updated_utc = updated.replace(tzinfo=timezone.utc) if updated.tzinfo is None else updated.astimezone(timezone.utc)
        age_sec = (now_utc - updated_utc).total_seconds()
        if age_sec >= stale_after_seconds:
            item.status = "retryable"
            item.error_message = "Recovered from interrupted run."
            item.updated_at = datetime.utcnow()
            touched += 1
    session.flush()
    return touched


def mark_item_completed(
    session: Session,
    *,
    item_id: str,
    resolved_film_id: str | None,
    matched_title: str | None,
    coverage_summary: str,
    ratings_summary: str,
    lookup_job_id: str | None = None,
    meta_json: dict | None = None,
) -> None:
    item = session.get(BulkLookupItem, item_id)
    if not item:
        return
    item.status = "completed"
    item.resolved_film_id = resolved_film_id
    item.matched_title = matched_title
    item.coverage_summary = coverage_summary
    item.ratings_summary = ratings_summary
    item.lookup_job_id = lookup_job_id
    item.error_message = None
    item.meta_json = meta_json or {}
    now = datetime.utcnow()
    item.updated_at = now
    item.completed_at = now
    session.flush()


def mark_item_failed(
    session: Session,
    *,
    item_id: str,
    error_message: str,
    retryable: bool = True,
) -> None:
    item = session.get(BulkLookupItem, item_id)
    if not item:
        return
    item.status = "retryable" if retryable else "failed"
    item.error_message = (error_message or "")[:500]
    item.updated_at = datetime.utcnow()
    session.flush()


def retry_failed_items(session: Session, batch_id: str) -> int:
    items = list(
        session.execute(
            select(BulkLookupItem).where(
                BulkLookupItem.batch_id == batch_id,
                BulkLookupItem.status.in_(["failed", "retryable"]),
            )
        ).scalars().all()
    )
    for item in items:
        item.status = "retryable"
        item.error_message = None
        item.updated_at = datetime.utcnow()
    session.flush()
    return len(items)


def recompute_batch_counters(session: Session, batch_id: str) -> BulkLookupBatch | None:
    batch = session.get(BulkLookupBatch, batch_id)
    if not batch:
        return None
    items = list_items(session, batch_id)
    total = len(items)
    completed = sum(1 for i in items if i.status == "completed")
    running = sum(1 for i in items if i.status == "running")
    failed = sum(1 for i in items if i.status in ("failed", "retryable"))
    success = sum(1 for i in items if i.status == "completed" and i.resolved_film_id)
    batch.total_items = total
    batch.processed_items = completed + failed
    batch.success_items = success
    batch.failed_items = failed
    if completed + failed >= total and running == 0 and total > 0:
        batch.status = "completed"
        batch.completed_at = datetime.utcnow()
    elif batch.status != "paused":
        batch.status = "running"
    session.flush()
    return batch
