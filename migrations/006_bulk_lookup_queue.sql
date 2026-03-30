-- Durable bulk lookup queue tables (additive; does not alter existing acquisition engine).

create table if not exists public.bulk_lookup_batches (
  id uuid not null default extensions.uuid_generate_v4(),
  status text not null default 'running',
  total_items integer not null default 0,
  processed_items integer not null default 0,
  success_items integer not null default 0,
  failed_items integer not null default 0,
  created_at timestamp with time zone not null default now(),
  started_at timestamp with time zone null,
  completed_at timestamp with time zone null,
  notes_json jsonb not null default '{}'::jsonb,
  constraint bulk_lookup_batches_pkey primary key (id)
);

create table if not exists public.bulk_lookup_items (
  id uuid not null default extensions.uuid_generate_v4(),
  batch_id uuid not null,
  queue_index integer not null,
  query_text text not null,
  release_year_hint integer null,
  status text not null default 'queued',
  attempts integer not null default 0,
  lookup_job_id uuid null,
  resolved_film_id uuid null,
  matched_title text null,
  coverage_summary text null,
  ratings_summary text null,
  error_message text null,
  meta_json jsonb not null default '{}'::jsonb,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  completed_at timestamp with time zone null,
  constraint bulk_lookup_items_pkey primary key (id),
  constraint bulk_lookup_items_batch_id_fkey foreign key (batch_id) references public.bulk_lookup_batches (id) on delete cascade,
  constraint bulk_lookup_items_lookup_job_id_fkey foreign key (lookup_job_id) references public.lookup_jobs (id) on delete set null,
  constraint bulk_lookup_items_resolved_film_id_fkey foreign key (resolved_film_id) references public.films (id) on delete set null
);

create index if not exists ix_bulk_lookup_items_batch_idx
  on public.bulk_lookup_items using btree (batch_id, queue_index);

create index if not exists ix_bulk_lookup_items_batch_status
  on public.bulk_lookup_items using btree (batch_id, status);
