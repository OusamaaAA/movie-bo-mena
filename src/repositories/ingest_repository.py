from datetime import datetime
from typing import Iterable

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from src.models import NormalizedEvidence, RawEvidence, ReviewQueue, Source, SourceRun
from src.schema_types import RunStatus


class IngestRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_run(self, source_code: str, run_type: str) -> SourceRun:
        source = self.session.execute(select(Source).where(Source.source_code == source_code)).scalar_one_or_none()
        run = SourceRun(
            source_id=source.id if source else None,
            run_type=run_type,
            status=RunStatus.RUNNING.value,
            started_at=datetime.utcnow(),
        )
        self.session.add(run)
        self.session.flush()
        return run

    def close_run(
        self,
        run: SourceRun,
        *,
        status: RunStatus,
        fetched: int,
        normalized: int,
        reconciled: int = 0,
        error: str | None = None,
    ) -> None:
        run.status = status.value
        run.completed_at = datetime.utcnow()
        run.fetched_count = fetched
        run.normalized_count = normalized
        run.reconciled_count = reconciled
        run.error_count = 1 if error else 0
        run.error_message = error
        self.session.add(run)

    def add_raw_records(self, records: Iterable[RawEvidence]) -> list[RawEvidence]:
        rows = list(records)
        if not rows:
            return []

        ids = [r.id for r in rows if getattr(r, "id", None)]
        new_rows: list[RawEvidence] = []
        refreshed_rows: list[RawEvidence] = []

        if ids:
            # Fetch existing records so we can update their financial fields instead of skipping.
            existing_map: dict[str, RawEvidence] = {
                str(x.id): x
                for x in self.session.execute(select(RawEvidence).where(RawEvidence.id.in_(ids))).scalars().all()
            }
            for r in rows:
                rid = str(r.id) if getattr(r, "id", None) else ""
                if rid and rid in existing_map:
                    existing = existing_map[rid]
                    # Full merge: stable id is the same but parser/ingest fixes can refresh any field.
                    existing.source_run_id = r.source_run_id
                    existing.source_url = r.source_url
                    existing.film_title_raw = r.film_title_raw
                    existing.film_title_ar_raw = r.film_title_ar_raw
                    existing.release_year_hint = r.release_year_hint
                    existing.record_semantics = r.record_semantics
                    existing.evidence_type = r.evidence_type
                    existing.period_label_raw = r.period_label_raw
                    existing.period_start_date = r.period_start_date
                    existing.period_end_date = r.period_end_date
                    existing.period_key = r.period_key
                    existing.record_granularity = r.record_granularity
                    existing.period_gross_local = r.period_gross_local
                    existing.admissions_actual = r.admissions_actual
                    existing.admissions_estimated = r.admissions_estimated
                    existing.cumulative_gross_local = r.cumulative_gross_local
                    existing.currency = r.currency
                    existing.rank = r.rank
                    existing.parser_confidence = r.parser_confidence
                    existing.source_confidence = r.source_confidence
                    existing.notes = r.notes
                    existing.raw_payload_json = r.raw_payload_json
                    # Remove stale normalized evidence so it gets re-created with updated values.
                    self.session.execute(
                        delete(NormalizedEvidence).where(NormalizedEvidence.raw_evidence_id == existing.id)
                    )
                    refreshed_rows.append(existing)
                else:
                    new_rows.append(r)
        else:
            new_rows = list(rows)

        # Deduplicate within the new batch (same id can appear twice in a backfill).
        seen_batch: set[str] = set()
        deduped: list[RawEvidence] = []
        for r in new_rows:
            rid = str(r.id) if getattr(r, "id", None) else ""
            if rid:
                if rid in seen_batch:
                    continue
                seen_batch.add(rid)
            deduped.append(r)
        new_rows = deduped

        if new_rows:
            self.session.add_all(new_rows)

        if new_rows or refreshed_rows:
            self.session.flush()

        return new_rows + refreshed_rows

    def add_normalized_records(self, records: Iterable[NormalizedEvidence]) -> int:
        rows = list(records)
        self.session.add_all(rows)
        self.session.flush()
        return len(rows)

    def add_review_item(self, item: ReviewQueue) -> None:
        self.session.add(item)

