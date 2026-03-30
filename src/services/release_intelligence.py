"""
Release Intelligence Engine
============================
Converts historical box-office evidence + ratings into decision-grade
film intelligence for MENA release planning.

This module is **purely additive** — it reads from existing data structures
(ReconciledEvidence rows, RatingsMetric rows, Film metadata) and returns
a self-contained intelligence payload. It never mutates the database or
alters existing report fields.

Design: 11-step analytical pipeline
  1. Film-Market Run Profiles
  2. Market Cohorts (historical quantiles)
  3. Market Performance Scoring
  4. Core Market Performance Score
  5. Audience Validation
  6. Interest Heat
  7. Confidence
  8. Benchmark Usefulness
  9. Commercial Potential (composite)
 10. Forecast Bands
 11. Analyst Commentary
"""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date
from typing import Any, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import Film, RatingsMetric, ReconciledEvidence
from src.services.admissions_estimation import admissions_estimated_for_evidence

# ── Constants ──────────────────────────────────────────────────────────────────

CORE_MARKETS = ["EG", "AE", "SA"]
CORE_MARKET_WEIGHTS = {"EG": 0.40, "AE": 0.35, "SA": 0.25}

# Admissions in run profiles use admissions_estimated_for_evidence with the same
# ticket baselines as Film Report (load_prices / config — not MarketReference DB overrides).

# Market score dimension weights
W_OPENING_POWER = 0.25
W_PEAK_STRENGTH = 0.10
W_TRACKED_VOLUME = 0.20
W_HOLD_QUALITY = 0.20
W_RUN_DEPTH = 0.15
W_STABILITY = 0.10

# Final composite weights
W_CORE_MKT = 0.55
W_AUD_VAL = 0.15
W_INTEREST = 0.10
W_CONFIDENCE = 0.10
W_BENCHMARK = 0.10


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _percentile_rank(value: float, cohort: list[float]) -> float:
    """Where does *value* sit in *cohort*? Returns 0-1."""
    if not cohort:
        return 0.5
    below = sum(1 for c in cohort if c <= value)
    return below / len(cohort)


def _quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = q * (len(sorted_vals) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return sorted_vals[lo]
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def _period_sort_key(row: dict) -> tuple:
    """Sort reconciled rows chronologically."""
    psd = row.get("period_start_date") or ""
    pk = row.get("period_key") or ""
    return (psd, pk)


def _period_iso_week(row: dict) -> tuple[int, int] | None:
    """Return (ISO year, ISO week) from period_start_date, or None if unparseable."""
    psd = row.get("period_start_date") or ""
    if not psd:
        return None
    try:
        d = date.fromisoformat(str(psd)[:10])
    except ValueError:
        return None
    y, w, _ = d.isocalendar()
    return (y, w)


def _is_daily_granularity_row(row: dict) -> bool:
    g = (row.get("granularity") or "").lower()
    return g in ("day", "daily") or "daily" in g


def _dedupe_period_rows_by_iso_week(period_rows: list[dict]) -> list[dict]:
    """
    Collapse multiple reconciled rows that fall in the same ISO week when they are
    weekly / weekend-scale (not day-by-day). Reconciliation can emit both a
    Mon-start weekly row and a Thu/Sun weekend row for the same calendar week;
    a sparse row may carry admissions with gross=0 while another has the real BO
    gross — both must not appear as separate periods in the run curve.

    Daily series are left untouched (multiple rows per week are intentional).
    """
    undated: list[dict] = []
    by_week: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for r in period_rows:
        iw = _period_iso_week(r)
        if iw is None:
            undated.append(r)
        else:
            by_week[iw].append(r)

    out: list[dict] = []
    for group in by_week.values():
        if len(group) == 1:
            out.append(group[0])
            continue
        if any(_is_daily_granularity_row(r) for r in group):
            out.extend(group)
            continue
        with_gross = [r for r in group if _safe_float(r.get("period_gross_local")) > 0]
        if with_gross:
            out.append(max(with_gross, key=lambda r: _safe_float(r.get("period_gross_local"))))
        else:
            out.append(group[0])
    out.extend(undated)
    return out


def _trim_leading_zero_gross_periods(period_rows: list[dict]) -> list[dict]:
    """
    Remove leading periods with no period gross when at least one later period has
    positive gross. ISO-week dedupe only collapses rows in the *same* week; a
    weekly/admissions-only row can still sit in an *earlier* week than the first
    BOM weekend row (different ISO weeks), producing a bogus opening point.
    """
    if len(period_rows) < 2:
        return period_rows
    if not any(_safe_float(r.get("period_gross_local")) > 0 for r in period_rows):
        return period_rows
    i = 0
    while i < len(period_rows) and _safe_float(period_rows[i].get("period_gross_local")) <= 0:
        i += 1
    return period_rows[i:]


def _detect_anomalies(
    admissions_series: list[float],
    gross_series: list[float],
) -> tuple[list[dict], float]:
    """
    Detect suspicious patterns in a period-level market run.

    Returns
    -------
    (flags, overall_severity)
        flags            : list of per-period anomaly dicts
        overall_severity : 0.0 (clean) – 1.0 (highly suspicious)

    Three tests:

    1. admissions_gross_mismatch
       A period with meaningful admissions but gross = 0. Common cause:
       only one data source was ingested for that period, or the week
       was estimated without a corresponding gross record. Severity is
       higher when the anomalous period is also the peak.

    2. spike_then_collapse
       A single period climbs sharply vs the prior period (> 2.5x) then
       collapses back the next (< 0.35x). This is inconsistent with
       genuine audience build — it is more likely a reporting artifact
       or double-count. Severity scales with the sharpness of the spike.

    3. orphan_spike
       The peak period is > 2x both its immediate neighbours but was not
       already caught by spike_then_collapse (i.e., a different geometric
       pattern). Indicates the peak may not reflect true audience momentum.
    """
    flags: list[dict] = []
    n = len(admissions_series)
    if n == 0:
        return flags, 0.0

    peak_val = max(admissions_series) if admissions_series else 0.0

    # ── Test 1: admissions/gross mismatch ─────────────────────────────────
    for i, (adm, gross) in enumerate(zip(admissions_series, gross_series)):
        if adm > 100 and gross == 0.0:
            is_peak = (adm == peak_val)
            sev = 0.80 if is_peak else 0.45
            flags.append({
                "period_idx": i,
                "type": "admissions_gross_mismatch",
                "severity": round(sev, 2),
                "detail": f"period {i}: adm={round(adm):,} but gross=0",
            })

    # ── Test 2: spike-then-collapse ───────────────────────────────────────
    for i in range(1, n - 1):
        prev = admissions_series[i - 1]
        curr = admissions_series[i]
        nxt = admissions_series[i + 1]
        if prev > 0 and curr > 0 and nxt > 0:
            up = curr / prev
            down = nxt / curr
            if up > 2.5 and down < 0.35:
                # Severity: combination of spike magnitude and collapse sharpness
                sev = _clamp(0.50 + (up / max(down, 0.01)) / 100.0)
                flags.append({
                    "period_idx": i,
                    "type": "spike_then_collapse",
                    "severity": round(sev, 2),
                    "detail": (
                        f"period {i}: {round(up, 1)}x rise "
                        f"then {round(down, 2)}x drop"
                    ),
                })

    # ── Test 3: orphan spike (peak >> both neighbours, not a spike_then_collapse) ──
    if n >= 3:
        already_stc = {f["period_idx"] for f in flags if f["type"] == "spike_then_collapse"}
        for i in range(1, n - 1):
            if i in already_stc:
                continue
            prev = admissions_series[i - 1]
            curr = admissions_series[i]
            nxt = admissions_series[i + 1]
            if prev > 0 and curr > 0 and nxt > 0 and curr == peak_val:
                if curr > prev * 2.0 and curr > nxt * 2.0:
                    neighbour_max = max(prev, nxt)
                    sev = _clamp(0.40 + (curr / neighbour_max) / 25.0)
                    flags.append({
                        "period_idx": i,
                        "type": "orphan_spike",
                        "severity": round(sev, 2),
                        "detail": (
                            f"period {i}: peak={round(curr):,} "
                            f">> neighbours ({round(prev):,}, {round(nxt):,})"
                        ),
                    })

    if not flags:
        return flags, 0.0

    max_sev = max(f["severity"] for f in flags)
    # Small additive boost for multiple independent anomaly types
    distinct_types = len({f["type"] for f in flags})
    multi_boost = min(0.10, (distinct_types - 1) * 0.05)
    overall = round(_clamp(max_sev + multi_boost), 3)
    return flags, overall


# ── Step 1: Film-Market Run Profiles ──────────────────────────────────────────

@dataclass
class MarketRunProfile:
    """Analytical summary of a single film's theatrical run in one market."""
    film_id: str
    country_code: str
    currency: str = ""
    period_count: int = 0
    opening_admissions: float = 0.0
    opening_gross: float = 0.0
    peak_admissions: float = 0.0
    peak_gross: float = 0.0
    tracked_total_admissions: float = 0.0
    tracked_total_gross: float = 0.0
    cumulative_gross_available: bool = False
    admissions_source_mix: dict = field(default_factory=dict)  # actual/estimated/derived counts
    peak_period_idx: int = 0           # index in period_admissions where peak occurs
    hold_ratios: list[float] = field(default_factory=list)       # full series (build + decay)
    build_hold_ratios: list[float] = field(default_factory=list) # holds during growth leg only
    decay_hold_ratios: list[float] = field(default_factory=list) # holds during decay leg only
    weighted_hold: float = 0.0
    run_depth: float = 0.0
    stability: float = 0.0
    evidence_quality: float = 0.0
    anomaly_flags: list[dict] = field(default_factory=list)  # per-period anomaly events
    anomaly_severity: float = 0.0                            # 0.0 = clean, 1.0 = very suspicious
    period_admissions: list[float] = field(default_factory=list)
    period_grosses: list[float] = field(default_factory=list)
    run_shape_label: str = "unknown"


def _resolve_admissions(
    row: dict,
    country_code: str,
) -> tuple[float, str]:
    """
    Resolve admissions using the same formula as the Film Report table: ``estimate_admissions``
    / ``admissions_estimated_for_evidence`` with **config ticket prices only** (no
    MarketReference DB overrides), so Release Intelligence matches the film report UI.
    Stored reconciled admissions_estimated is not used (can be stale).
    """
    actual = row.get("admissions_actual")
    if actual is not None and _safe_float(actual) > 0:
        return _safe_float(actual), "actual"

    est = admissions_estimated_for_evidence(
        admissions_actual=None,
        record_semantics=(row.get("semantics") or ""),
        period_gross_local=row.get("period_gross_local"),
        cumulative_gross_local=row.get("cumulative_gross_local"),
        country_code=country_code,
        currency=(row.get("currency") or "").strip() or None,
        ticket_price_by_market_code=None,
    )
    if est is not None:
        return float(est), "estimated"

    return 0.0, "missing"


def build_market_run_profile(
    film_id: str,
    country_code: str,
    rows: list[dict],
) -> MarketRunProfile:
    """
    Transform reconciled rows for one film+market into an analytical run profile.
    Only uses period-level (non-cumulative) rows for the run curve.
    """
    profile = MarketRunProfile(film_id=film_id, country_code=country_code)

    if not rows:
        return profile

    # Separate period rows from cumulative rows
    period_rows = [
        r for r in rows
        if (r.get("semantics") or "").lower() not in ("cumulative", "total", "lifetime")
    ]
    cumul_rows = [
        r for r in rows
        if (r.get("semantics") or "").lower() in ("cumulative", "total", "lifetime")
    ]

    if cumul_rows:
        profile.cumulative_gross_available = True

    # Same ISO week can contain both a weekly and a weekend row (or a ghost row
    # with admissions but no gross). Keep one weekly-scale observation per week.
    period_rows = _dedupe_period_rows_by_iso_week(period_rows)

    if not period_rows:
        # Only cumulative data — use it for what we can
        if cumul_rows:
            best_cumul = max(cumul_rows, key=lambda r: _safe_float(r.get("period_gross_local")))
            profile.tracked_total_gross = _safe_float(best_cumul.get("period_gross_local"))
            profile.currency = best_cumul.get("currency") or ""
            adm, src = _resolve_admissions(best_cumul, country_code)
            profile.tracked_total_admissions = adm
            profile.admissions_source_mix[src] = 1
            profile.period_count = 1
            profile.evidence_quality = 0.3
        return profile

    # Sort chronologically
    period_rows.sort(key=_period_sort_key)
    period_rows = _trim_leading_zero_gross_periods(period_rows)

    profile.currency = period_rows[0].get("currency") or ""
    profile.period_count = len(period_rows)

    source_counts: dict[str, int] = {"actual": 0, "estimated": 0, "derived": 0, "missing": 0}
    admissions_series: list[float] = []
    gross_series: list[float] = []

    for r in period_rows:
        adm, src = _resolve_admissions(r, country_code)
        source_counts[src] += 1
        admissions_series.append(adm)
        gross_series.append(_safe_float(r.get("period_gross_local")))

    profile.admissions_source_mix = {k: v for k, v in source_counts.items() if v > 0}
    profile.period_admissions = admissions_series
    profile.period_grosses = gross_series

    # Opening = first period
    profile.opening_admissions = admissions_series[0] if admissions_series else 0.0
    profile.opening_gross = gross_series[0] if gross_series else 0.0

    # Peak
    profile.peak_admissions = max(admissions_series) if admissions_series else 0.0
    profile.peak_gross = max(gross_series) if gross_series else 0.0

    # Totals
    profile.tracked_total_admissions = sum(admissions_series)
    profile.tracked_total_gross = sum(gross_series)

    # ── Peak index — shared reference for holds, stability, and run shape ────────
    peak_idx = admissions_series.index(max(admissions_series)) if admissions_series else 0
    profile.peak_period_idx = peak_idx

    # ── Hold ratios: split into build leg and decay leg ───────────────────────
    # Build holds: week-over-week during the growth phase (before and up to peak).
    # These are informational only — including them in scoring biases the hold
    # quality metric upward because pre-peak ratios are typically > 1.
    build_holds: list[float] = []
    for i in range(1, peak_idx + 1):
        prev = admissions_series[i - 1]
        curr = admissions_series[i]
        if prev > 0:
            build_holds.append(curr / prev)

    # Decay holds: week-over-week from peak onward (the analytically meaningful leg).
    # These measure the film's ability to retain audience after its peak — the true
    # word-of-mouth / hold-quality signal.
    decay_holds: list[float] = []
    for i in range(peak_idx + 1, len(admissions_series)):
        prev = admissions_series[i - 1]
        curr = admissions_series[i]
        if prev > 0:
            decay_holds.append(curr / prev)

    profile.build_hold_ratios = build_holds
    profile.decay_hold_ratios = decay_holds
    profile.hold_ratios = build_holds + decay_holds  # full series kept for debug

    # Weighted hold: decay phase only to avoid inflation from build weeks
    if decay_holds:
        weights = [1.0 / (i + 1) for i in range(len(decay_holds))]
        total_w = sum(weights)
        profile.weighted_hold = sum(h * w for h, w in zip(decay_holds, weights)) / total_w
    else:
        # No decay observed (peak was the final period, or only one period)
        profile.weighted_hold = 0.0

    # ── Run depth: total / opening ─────────────────────────────────────────────
    if profile.opening_admissions > 0:
        profile.run_depth = profile.tracked_total_admissions / profile.opening_admissions
    else:
        profile.run_depth = 0.0

    # ── Anomaly detection ──────────────────────────────────────────────────────
    # Run before stability + evidence_quality so both can be informed by it.
    anomaly_flags, anomaly_severity = _detect_anomalies(admissions_series, gross_series)
    profile.anomaly_flags = anomaly_flags
    profile.anomaly_severity = anomaly_severity

    # ── Stability: curve coherence after peak ─────────────────────────────────
    #
    # Design goal: measure WHETHER the run decays predictably — not how
    # mathematically smooth it is. A long-tail run with minor tail wobbles
    # is stable; a run that bounces up and down with no envelope is not.
    #
    # Method: volume-weighted std dev of log-holds on the decay leg.
    #
    #   The key insight is that near-zero tail periods (e.g. 447→29 = log −2.73)
    #   should NOT dominate the variance calculation — they represent noise at
    #   near-zero attendance, not genuine instability. Volume-weighting ensures
    #   the high-admissions core weeks define the stability score; tiny tail
    #   periods contribute almost nothing.
    #
    #   Additionally:
    #   • Direction reversals (genuine admissions up-ticks after peak) are
    #     penalised — but only for periods above 5% of peak admissions, so
    #     tail noise isn't counted.
    #   • Anomalous peaks: skip the spike→normal first-decay transition
    #     (not a real hold), then apply a multiplicative discount so we
    #     preserve a non-zero floor for runs that DO have underlying structure.

    # Peak-level anomaly types — used for hold-skip routing and penalty choice
    peak_anomaly_types = {
        f["type"] for f in anomaly_flags
        if f["period_idx"] == peak_idx
        and f["type"] in ("spike_then_collapse", "orphan_spike", "admissions_gross_mismatch")
    }

    if len(decay_holds) >= 2:
        # If peak is anomalous, skip decay_holds[0] — it is the spike→normal
        # transition, which is an artifact, not a real audience-retention hold.
        # Guard: only skip when we have at least 2 holds left after skipping.
        skip = 1 if (peak_anomaly_types and len(decay_holds) > 2) else 0
        eval_holds = decay_holds[skip:]

        # Volume weights: min(adm_before, adm_after) for each hold.
        # This maps each period-to-period transition to the smaller of the two
        # adjacent admissions counts, so high-attendance weeks dominate.
        vol_weights: list[float] = []
        for k in range(len(eval_holds)):
            abs_k = skip + k
            from_adm = admissions_series[peak_idx + abs_k]
            to_idx = peak_idx + abs_k + 1
            to_adm = admissions_series[to_idx] if to_idx < len(admissions_series) else from_adm
            vol_weights.append(max(min(from_adm, to_adm), 1.0))

        total_vw = sum(vol_weights)
        if total_vw > 0 and eval_holds:
            log_h = [math.log(max(h, 1e-6)) for h in eval_holds]
            w_mean = sum(lh * w for lh, w in zip(log_h, vol_weights)) / total_vw
            w_var = sum(w * (lh - w_mean) ** 2 for lh, w in zip(log_h, vol_weights)) / total_vw
            sd_wt = math.sqrt(max(w_var, 0.0))

            # Direction reversals: penalise genuine admissions up-ticks after peak.
            # Only count reversals for periods with > 5% of peak admissions, so
            # noisy near-zero tail wobbles don't trigger false instability.
            peak_adm_val = admissions_series[peak_idx]
            sig_threshold = peak_adm_val * 0.05
            sig_dirs: list[int] = []
            for k in range(len(eval_holds)):
                to_idx = peak_idx + skip + k + 1
                if to_idx < len(admissions_series) and admissions_series[to_idx] >= sig_threshold:
                    sig_dirs.append(1 if eval_holds[k] >= 1.0 else -1)

            n_reversals = sum(
                1 for j in range(1, len(sig_dirs))
                if sig_dirs[j] != sig_dirs[j - 1]
            )
            n_sig_transitions = max(len(sig_dirs) - 1, 1)
            reversal_penalty = min(0.20, (n_reversals / n_sig_transitions) * 0.25)

            # sd_wt ≈ 0   → uniform decay → stability ~1.0
            # sd_wt ≈ 0.35 → moderate variance → stability ~0.475
            # sd_wt ≈ 0.65+ → volatile decay → stability ~0.0
            base_stability = _clamp(1.0 - sd_wt * 1.5 - reversal_penalty)
        else:
            base_stability = 0.60

    elif len(decay_holds) == 1:
        base_stability = 0.65   # single step — can't measure variance
    elif len(admissions_series) <= 1 or peak_idx == len(admissions_series) - 1:
        base_stability = 0.55   # no decay observed — sparse, not volatile
    else:
        base_stability = 0.50

    # Apply anomaly penalty.
    # Multiplicative for peak anomalies: preserves a non-zero floor because
    # the underlying decay curve (post-spike) may be coherent. Additive for
    # minor non-peak anomalies (gentler effect).
    if peak_anomaly_types:
        profile.stability = _clamp(base_stability * (1.0 - anomaly_severity * 0.40))
    else:
        profile.stability = _clamp(base_stability - anomaly_severity * 0.12)

    # ── Evidence quality ──────────────────────────────────────────────────────
    # Blends: source type quality, period depth, run continuity, and coherence.
    # Coherence is reduced by anomalies — suspicious peaks lower our trust in the
    # data even if we have many periods.
    total_obs = sum(source_counts.values())
    actual_pct    = source_counts.get("actual", 0)    / total_obs if total_obs else 0
    estimated_pct = source_counts.get("estimated", 0) / total_obs if total_obs else 0
    missing_pct   = source_counts.get("missing", 0)   / total_obs if total_obs else 0
    # Source quality: actual = gold (1.0), estimated = silver (0.7), derived = bronze (0.3)
    source_quality = (
        actual_pct * 1.0
        + estimated_pct * 0.7
        + (1 - actual_pct - estimated_pct - missing_pct) * 0.3
    )
    # Period depth: 6+ weeks = full confidence
    period_depth_factor = _clamp(len(period_rows) / 6.0)
    # Continuity: fraction of rows with usable admissions
    continuity_factor = 1.0 - missing_pct * 0.6
    # Coherence: anomalies reduce trust in the run's structural integrity
    coherence_factor = 1.0 - anomaly_severity * 0.45
    profile.evidence_quality = _clamp(
        source_quality       * 0.45
        + period_depth_factor  * 0.28
        + continuity_factor    * 0.12
        + coherence_factor     * 0.15,
        0.0, 1.0,
    )

    # ── Run shape label ────────────────────────────────────────────────────────
    # Priority: anomaly-driven labels are assigned first, then structural labels.
    anomaly_at_peak = any(
        f["period_idx"] == peak_idx
        and f["type"] in ("spike_then_collapse", "orphan_spike")
        for f in anomaly_flags
    )
    if len(admissions_series) >= 3 and profile.opening_admissions > 0:
        front_ratio = (
            admissions_series[0] / profile.tracked_total_admissions
            if profile.tracked_total_admissions > 0 else 1.0
        )
        if anomaly_at_peak:
            # Structural peak is driven by a reporting artifact — label it clearly
            profile.run_shape_label = "anomalous_spike"
        elif anomaly_severity > 0.50:
            # High overall anomaly load without a single clean spike pattern
            profile.run_shape_label = "irregular"
        elif peak_idx > 0:
            # Genuine audience build before peak
            if profile.stability >= 0.55 and anomaly_severity < 0.20:
                profile.run_shape_label = "clean_build_then_decay"
            else:
                profile.run_shape_label = "irregular_build_then_decay"
        elif front_ratio > 0.55:
            profile.run_shape_label = "front_loaded"
        elif front_ratio < 0.25 and profile.weighted_hold >= 0.65:
            profile.run_shape_label = "resilient"
        elif profile.weighted_hold >= 0.40:
            profile.run_shape_label = "standard_decay"
        else:
            profile.run_shape_label = "standard_decay"
    elif len(admissions_series) == 2:
        if build_holds:
            profile.run_shape_label = "clean_build_then_decay (short)"
        elif decay_holds and decay_holds[0] >= 0.6:
            profile.run_shape_label = "standard_decay (healthy, short)"
        else:
            profile.run_shape_label = "standard_decay (short)"
    elif len(admissions_series) == 1:
        profile.run_shape_label = "single-period"
    else:
        profile.run_shape_label = "sparse"

    return profile


# ── Step 2: Market Cohorts ────────────────────────────────────────────────────

@dataclass
class MarketCohort:
    """Historical distribution of opening/peak/total admissions in a market."""
    country_code: str
    opening_admissions: list[float] = field(default_factory=list)
    peak_admissions: list[float] = field(default_factory=list)
    total_admissions: list[float] = field(default_factory=list)
    film_count: int = 0


def build_market_cohorts(session: Session) -> dict[str, MarketCohort]:
    """
    Build historical cohorts for each core market from ALL films in the DB.
    Each cohort has sorted lists of opening/peak/total admissions.
    """
    cohorts: dict[str, MarketCohort] = {}

    for cc in CORE_MARKETS:
        rows = list(session.execute(
            select(ReconciledEvidence)
            .where(
                ReconciledEvidence.country_code == cc,
            )
        ).scalars().all())

        # Group by film_id
        by_film: dict[str, list[dict]] = {}
        for r in rows:
            by_film.setdefault(r.film_id, []).append({
                "period_start_date": r.period_start_date.isoformat() if r.period_start_date else None,
                "period_key": r.period_key,
                "period_gross_local": float(r.period_gross_local or 0),
                "cumulative_gross_local": float(r.cumulative_gross_local)
                if r.cumulative_gross_local is not None
                else None,
                "currency": r.currency,
                "admissions_actual": float(r.admissions_actual) if r.admissions_actual is not None else None,
                "admissions_estimated": float(r.admissions_estimated) if r.admissions_estimated is not None else None,
                "granularity": r.record_granularity,
                "semantics": r.record_semantics,
            })

        cohort = MarketCohort(country_code=cc)
        for fid, frows in by_film.items():
            profile = build_market_run_profile(fid, cc, frows)
            if profile.period_count == 0:
                continue
            if profile.opening_admissions > 0:
                cohort.opening_admissions.append(profile.opening_admissions)
            if profile.peak_admissions > 0:
                cohort.peak_admissions.append(profile.peak_admissions)
            if profile.tracked_total_admissions > 0:
                cohort.total_admissions.append(profile.tracked_total_admissions)
            cohort.film_count += 1

        # Sort for quantile computation
        cohort.opening_admissions.sort()
        cohort.peak_admissions.sort()
        cohort.total_admissions.sort()
        cohorts[cc] = cohort

    return cohorts


# ── Step 3: Market Performance Scoring ────────────────────────────────────────

@dataclass
class MarketIntelligence:
    country_code: str
    opening_admissions: float = 0.0
    opening_gross: float = 0.0
    tracked_total_admissions: float = 0.0
    tracked_total_gross: float = 0.0
    currency: str = ""
    period_count: int = 0
    opening_power_score: float = 0.0
    peak_strength_score: float = 0.0
    tracked_volume_strength_score: float = 0.0
    hold_quality_score: float = 0.0
    run_depth_score: float = 0.0
    stability_score: float = 0.0
    market_score: float = 0.0
    run_shape_label: str = "unknown"
    evidence_quality: float = 0.0


def score_market_run(
    profile: MarketRunProfile,
    cohort: MarketCohort | None,
) -> MarketIntelligence:
    """Score a single film-market run against its historical cohort."""
    mi = MarketIntelligence(
        country_code=profile.country_code,
        opening_admissions=profile.opening_admissions,
        opening_gross=profile.opening_gross,
        tracked_total_admissions=profile.tracked_total_admissions,
        tracked_total_gross=profile.tracked_total_gross,
        currency=profile.currency,
        period_count=profile.period_count,
        run_shape_label=profile.run_shape_label,
        evidence_quality=profile.evidence_quality,
    )

    if profile.period_count == 0:
        return mi

    # If no cohort data, use absolute heuristics
    if cohort and cohort.film_count >= 2:
        mi.opening_power_score = _clamp(
            _percentile_rank(profile.opening_admissions, cohort.opening_admissions)
        )
        mi.peak_strength_score = _clamp(
            _percentile_rank(profile.peak_admissions, cohort.peak_admissions)
        )
        mi.tracked_volume_strength_score = _clamp(
            _percentile_rank(profile.tracked_total_admissions, cohort.total_admissions)
        )
    else:
        # Heuristic: assume mid-range with slight penalty for no context
        mi.opening_power_score = 0.50 if profile.opening_admissions > 0 else 0.0
        mi.peak_strength_score = 0.50 if profile.peak_admissions > 0 else 0.0
        mi.tracked_volume_strength_score = 0.50 if profile.tracked_total_admissions > 0 else 0.0

    # Hold quality: decay-phase weighted hold only.
    # weighted_hold is already computed from decay_hold_ratios in build_market_run_profile,
    # so this correctly reflects audience retention post-peak.
    # 0.7 hold/week = very strong legs; 0.5 = decent; < 0.3 = sharp drop.
    if profile.decay_hold_ratios:
        mi.hold_quality_score = _clamp(profile.weighted_hold / 0.75)
    else:
        # No decay data available (single-period or peak was final week)
        mi.hold_quality_score = 0.35  # conservative neutral — can't score what we can't see

    # Run depth: >3x opening is strong, >5x is excellent
    if profile.run_depth > 0:
        mi.run_depth_score = _clamp(math.log(max(profile.run_depth, 1.0)) / math.log(6.0))
    else:
        mi.run_depth_score = 0.0

    mi.stability_score = profile.stability

    # Composite market score
    raw_score = _clamp(
        mi.opening_power_score * W_OPENING_POWER
        + mi.peak_strength_score * W_PEAK_STRENGTH
        + mi.tracked_volume_strength_score * W_TRACKED_VOLUME
        + mi.hold_quality_score * W_HOLD_QUALITY
        + mi.run_depth_score * W_RUN_DEPTH
        + mi.stability_score * W_STABILITY
    )

    # Anomaly discount: when the structural peak is suspicious the market score
    # may be inflated by a ghost number. Apply a proportional discount so the
    # composite isn't anchored to an unreliable opening/peak figure.
    # Max discount = 25% at anomaly_severity = 1.0.
    anomaly_discount = profile.anomaly_severity * 0.25
    mi.market_score = _clamp(raw_score * (1.0 - anomaly_discount))

    return mi


# ── Step 4: Core Market Performance ───────────────────────────────────────────

def compute_core_market_performance(
    market_intels: dict[str, MarketIntelligence],
) -> float:
    """
    Weighted blend of core market scores.
    Renormalizes weights when some markets are missing.
    """
    total_weight = 0.0
    weighted_sum = 0.0
    for cc, w in CORE_MARKET_WEIGHTS.items():
        mi = market_intels.get(cc)
        if mi and mi.period_count > 0:
            weighted_sum += mi.market_score * w
            total_weight += w
    if total_weight == 0:
        return 0.0
    return _clamp(weighted_sum / total_weight)


# ── Step 5: Audience Validation ───────────────────────────────────────────────

def compute_audience_validation(ratings: list[dict]) -> float:
    """
    Blend IMDb (scale 10) and elCinema (scale 10) ratings with vote-count
    confidence weighting. Letterboxd is supplemental.
    """
    imdb_score: float | None = None
    imdb_votes: int = 0
    elc_score: float | None = None
    elc_votes: int = 0
    lb_score: float | None = None
    lb_count: int = 0

    for r in ratings:
        src = (r.get("source_name") or "").lower()
        rv = _safe_float(r.get("rating_value"))
        vc = int(r.get("vote_count") or 0)
        if rv <= 0:
            continue
        if "imdb" in src:
            if imdb_score is None or vc > imdb_votes:
                imdb_score = rv
                imdb_votes = vc
        elif "elcinema" in src or "el cinema" in src:
            if elc_score is None or vc > elc_votes:
                elc_score = rv
                elc_votes = vc
        elif "letterboxd" in src:
            if lb_score is None or vc > lb_count:
                lb_score = rv * 2  # Scale 5 → 10 for blending
                lb_count = vc

    if imdb_score is None and elc_score is None and lb_score is None:
        return 0.0

    # Vote-count confidence multiplier (reduces impact of very sparse ratings)
    def _vote_conf(votes: int) -> float:
        if votes >= 5000:
            return 1.0
        if votes >= 1000:
            return 0.9
        if votes >= 200:
            return 0.75
        if votes >= 50:
            return 0.55
        if votes > 0:
            return 0.35
        return 0.15  # rating with no vote info

    # Weighted blend of available sources
    sources: list[tuple[float, float]] = []  # (normalized_score, weight)
    if imdb_score is not None:
        norm = _clamp(imdb_score / 10.0)
        sources.append((norm, 0.50 * _vote_conf(imdb_votes)))
    if elc_score is not None:
        norm = _clamp(elc_score / 10.0)
        sources.append((norm, 0.35 * _vote_conf(elc_votes)))
    if lb_score is not None:
        norm = _clamp(lb_score / 10.0)
        sources.append((norm, 0.15 * _vote_conf(lb_count)))

    total_w = sum(w for _, w in sources)
    if total_w == 0:
        return 0.0
    blended = sum(s * w for s, w in sources) / total_w
    return _clamp(blended)


# ── Step 6: Interest Heat ─────────────────────────────────────────────────────

def compute_interest_heat(ratings: list[dict]) -> float:
    """
    Interest/buzz signal from popularity_rank (lower = hotter) and vote volume.
    """
    best_rank: int | None = None
    total_votes = 0

    for r in ratings:
        pr = r.get("popularity_rank")
        if pr is not None and pr > 0:
            if best_rank is None or pr < best_rank:
                best_rank = pr
        total_votes += int(r.get("vote_count") or 0)

    rank_score = 0.0
    if best_rank is not None:
        # Top 100 = excellent, top 1000 = good, 5000+ = low
        if best_rank <= 50:
            rank_score = 1.0
        elif best_rank <= 200:
            rank_score = 0.85
        elif best_rank <= 1000:
            rank_score = 0.65
        elif best_rank <= 5000:
            rank_score = 0.40
        else:
            rank_score = 0.20

    # Vote volume proxy for buzz
    vote_score = 0.0
    if total_votes >= 50000:
        vote_score = 1.0
    elif total_votes >= 10000:
        vote_score = 0.80
    elif total_votes >= 2000:
        vote_score = 0.55
    elif total_votes >= 500:
        vote_score = 0.35
    elif total_votes > 0:
        vote_score = 0.15

    if best_rank is not None:
        return _clamp(rank_score * 0.6 + vote_score * 0.4)
    else:
        return _clamp(vote_score)


# ── Step 7: Confidence ────────────────────────────────────────────────────────

def compute_confidence(
    market_profiles: dict[str, MarketRunProfile],
    ratings: list[dict],
) -> tuple[float, str]:
    """
    How trustworthy is the evidence? Returns (score, label).
    """
    # Market coverage
    covered_markets = sum(1 for p in market_profiles.values() if p.period_count > 0)
    market_cov = _clamp(covered_markets / 3.0)  # 3 core markets

    # Total periods tracked
    total_periods = sum(p.period_count for p in market_profiles.values())
    period_depth = _clamp(total_periods / 15.0)  # 15 periods ~= 5 per market

    # Admissions source quality (weighted average across markets)
    eq_values = [p.evidence_quality for p in market_profiles.values() if p.period_count > 0]
    avg_eq = statistics.mean(eq_values) if eq_values else 0.0

    # Opening presence
    has_opening = any(p.opening_admissions > 0 for p in market_profiles.values())
    opening_present = 1.0 if has_opening else 0.4

    # Run consistency: do we see multi-period runs?
    multi_period = any(p.period_count >= 3 for p in market_profiles.values())
    run_consistency = 1.0 if multi_period else 0.6

    # Ratings data presence
    has_ratings = any(_safe_float(r.get("rating_value")) > 0 for r in ratings)
    ratings_present = 1.0 if has_ratings else 0.5

    score = _clamp(
        market_cov * 0.25
        + period_depth * 0.20
        + avg_eq * 0.20
        + opening_present * 0.10
        + run_consistency * 0.10
        + ratings_present * 0.15
    )

    if score >= 0.70:
        label = "High"
    elif score >= 0.40:
        label = "Medium"
    else:
        label = "Low"

    return score, label


# ── Step 8: Benchmark Usefulness ──────────────────────────────────────────────

def compute_benchmark_usefulness(
    market_profiles: dict[str, MarketRunProfile],
    confidence_score: float,
    audience_validation_score: float,
) -> float:
    """
    Is this title reliable enough to be used as a comparable for future films?
    """
    # Market coverage breadth
    covered = sum(1 for p in market_profiles.values() if p.period_count > 0)
    breadth = _clamp(covered / 3.0)

    # Run completeness: need multi-period runs, ideally in >1 market
    multi_period_markets = sum(1 for p in market_profiles.values() if p.period_count >= 3)
    completeness = _clamp(multi_period_markets / 2.0)

    # Structural consistency: evidence quality + stability
    eq_vals = [p.evidence_quality for p in market_profiles.values() if p.period_count > 0]
    stab_vals = [p.stability for p in market_profiles.values() if p.period_count >= 2]
    consistency = statistics.mean(eq_vals + stab_vals) if (eq_vals or stab_vals) else 0.0

    # Ratings completeness
    ratings_factor = 1.0 if audience_validation_score > 0.1 else 0.5

    return _clamp(
        breadth * 0.20
        + completeness * 0.25
        + consistency * 0.20
        + confidence_score * 0.20
        + ratings_factor * 0.15
    )


# ── Step 9: Commercial Potential (Composite) ──────────────────────────────────

def compute_commercial_potential(
    core_market_score: float,
    audience_validation: float,
    interest_heat: float,
    confidence: float,
    benchmark_usefulness: float,
) -> float:
    return _clamp(
        core_market_score * W_CORE_MKT
        + audience_validation * W_AUD_VAL
        + interest_heat * W_INTEREST
        + confidence * W_CONFIDENCE
        + benchmark_usefulness * W_BENCHMARK
    )


# ── Step 10: Forecast Bands ──────────────────────────────────────────────────

@dataclass
class ForecastBand:
    floor: float = 0.0
    base: float = 0.0
    stretch: float = 0.0


def build_forecast_bands(
    market_intels: dict[str, MarketIntelligence],
    cohorts: dict[str, MarketCohort],
    confidence_score: float,
) -> dict[str, dict[str, float]]:
    """
    Derive first-watch expectation ranges per market and total MENA.
    Uses historical opening quantiles mapped through market scores.
    """
    forecasts: dict[str, dict[str, float]] = {}
    mena_floor = 0.0
    mena_base = 0.0
    mena_stretch = 0.0

    for cc in CORE_MARKETS:
        mi = market_intels.get(cc)
        cohort = cohorts.get(cc)

        if not mi or mi.period_count == 0 or not cohort or not cohort.opening_admissions:
            forecasts[cc] = {"floor": 0, "base": 0, "stretch": 0}
            continue

        # Use opening-week admissions distribution for forecasting opening potential.
        # Total-run quantiles inflate the forecast; opening quantiles are the right anchor.
        opening_sorted = sorted(cohort.opening_admissions)
        q25 = _quantile(opening_sorted, 0.25)
        q50 = _quantile(opening_sorted, 0.50)
        q75 = _quantile(opening_sorted, 0.75)
        q90 = _quantile(opening_sorted, 0.90)

        # Map market score into the quantile distribution
        ms = mi.market_score
        if ms >= 0.75:
            base = q75 + (q90 - q75) * ((ms - 0.75) / 0.25)
        elif ms >= 0.50:
            base = q50 + (q75 - q50) * ((ms - 0.50) / 0.25)
        elif ms >= 0.25:
            base = q25 + (q50 - q25) * ((ms - 0.25) / 0.25)
        else:
            base = q25 * (ms / 0.25)

        # Confidence-tiered band width:
        #   High confidence  (> 0.80) → tight band  ±15%
        #   Medium confidence (0.60–0.80) → medium band ±25%
        #   Low confidence   (< 0.60) → wide band   −40% / +40%
        if confidence_score > 0.80:
            floor = base * 0.85
            stretch = base * 1.15
        elif confidence_score >= 0.60:
            floor = base * 0.75
            stretch = base * 1.25
        else:
            floor = base * 0.60
            stretch = base * 1.40

        forecasts[cc] = {
            "floor": round(floor),
            "base": round(base),
            "stretch": round(stretch),
        }
        mena_floor += floor
        mena_base += base
        mena_stretch += stretch

    forecasts["MENA_TOTAL"] = {
        "floor": round(mena_floor),
        "base": round(mena_base),
        "stretch": round(mena_stretch),
    }
    return forecasts


# ── Step 11: Analyst Commentary ───────────────────────────────────────────────

def generate_analyst_commentary(
    film_title: str,
    market_intels: dict[str, MarketIntelligence],
    core_market_score: float,
    audience_validation: float,
    interest_heat: float,
    confidence_score: float,
    confidence_label: str,
    benchmark_usefulness: float,
    commercial_potential: float,
    market_profiles: dict[str, MarketRunProfile],
) -> str:
    """Generate a concise analyst-style summary."""
    parts: list[str] = []

    # Overall assessment
    if commercial_potential >= 0.70:
        parts.append(f"{film_title} demonstrates strong commercial potential across tracked MENA markets.")
    elif commercial_potential >= 0.45:
        parts.append(f"{film_title} shows moderate commercial potential with selective strengths.")
    else:
        parts.append(f"{film_title} has limited observed commercial traction in available data.")

    # Strongest / weakest market
    scored_markets = [
        (cc, mi) for cc, mi in market_intels.items()
        if mi.period_count > 0
    ]
    if scored_markets:
        scored_markets.sort(key=lambda x: x[1].market_score, reverse=True)
        strongest_cc, strongest = scored_markets[0]
        market_names = {"EG": "Egypt", "AE": "UAE", "SA": "Saudi Arabia"}
        parts.append(
            f"Strongest market: {market_names.get(strongest_cc, strongest_cc)} "
            f"(score {strongest.market_score:.0%})."
        )
        if len(scored_markets) >= 2:
            weakest_cc, weakest = scored_markets[-1]
            if weakest.market_score < strongest.market_score - 0.1:
                parts.append(
                    f"Weakest: {market_names.get(weakest_cc, weakest_cc)} "
                    f"({weakest.market_score:.0%})."
                )

    # Run shape commentary
    shapes = {cc: p.run_shape_label for cc, p in market_profiles.items() if p.period_count >= 2}
    dominant_shape = ""
    if shapes:
        shape_counts: dict[str, int] = {}
        for s in shapes.values():
            shape_counts[s] = shape_counts.get(s, 0) + 1
        dominant_shape = max(shape_counts, key=lambda k: shape_counts[k])
        if dominant_shape == "clean_build_then_decay":
            parts.append("The run built audience traction cleanly after opening before declining — a strong word-of-mouth pattern.")
        elif "build_then_decay" in dominant_shape:
            parts.append("The run shows a build-then-decay pattern with some structural irregularities.")
        elif dominant_shape == "anomalous_spike":
            parts.append("A suspicious admissions spike was detected — the structural peak may not reflect genuine audience build. Interpret market scores with caution.")
        elif dominant_shape == "irregular":
            parts.append("The run pattern is irregular with multiple anomalous periods — evidence reliability is reduced.")
        elif "front_loaded" in dominant_shape or "front-loaded" in dominant_shape:
            parts.append("The run is front-loaded — most revenue concentrated in the opening period.")
        elif "resilient" in dominant_shape:
            parts.append("The run shows resilient legs with strong sustained audience interest.")
        elif "standard_decay" in dominant_shape:
            parts.append("The run shows standard decay — typical declining attendance after opening.")

    # Audience validation
    if audience_validation >= 0.65:
        parts.append("Audience ratings strongly validate the commercial performance.")
    elif audience_validation >= 0.45:
        parts.append("Audience ratings provide moderate support for the box-office story.")
    elif audience_validation > 0.1:
        parts.append("Audience ratings are mixed, tempering the commercial outlook.")
    else:
        parts.append("Ratings data is unavailable, limiting audience validation.")

    # Benchmark usefulness
    if benchmark_usefulness >= 0.65:
        parts.append("This title is a strong benchmark for comparable future acquisitions.")
    elif benchmark_usefulness >= 0.40:
        parts.append("Benchmark value is moderate — usable as a comparable with caveats.")
    else:
        parts.append("Limited benchmark utility due to sparse or inconsistent evidence.")

    # Confidence
    if confidence_label == "Low":
        parts.append(f"Confidence: {confidence_label} — interpret scores with caution.")
    else:
        parts.append(f"Evidence confidence: {confidence_label}.")

    return " ".join(parts)


# ── Main Orchestrator ─────────────────────────────────────────────────────────

@dataclass
class ReleaseIntelligence:
    """Complete intelligence payload for one film."""
    commercial_potential_score: float = 0.0
    core_market_performance_score: float = 0.0
    audience_validation_score: float = 0.0
    interest_heat_score: float = 0.0
    confidence_score: float = 0.0
    confidence_label: str = "Low"
    benchmark_usefulness_score: float = 0.0
    market_breakdown: dict[str, dict] = field(default_factory=dict)
    forecast: dict[str, dict[str, float]] = field(default_factory=dict)
    analyst_commentary: str = ""


def compute_release_intelligence(
    session: Session,
    film_id: str,
    *,
    reconciled_rows: list[dict] | None = None,
    ratings: list[dict] | None = None,
    film_title: str = "",
) -> dict[str, Any]:
    """
    Main entry point. Computes full release intelligence for a film.

    Parameters
    ----------
    session : SQLAlchemy session (for cohort building)
    film_id : UUID of the film
    reconciled_rows : pre-built reconciled row dicts (from FilmReport.reconciled)
    ratings : pre-built rating dicts (from FilmReport.ratings)
    film_title : display title for commentary

    Returns
    -------
    dict : the full intelligence payload (safe to serialize as JSON)
    """
    try:
        return _compute_release_intelligence_inner(
            session, film_id,
            reconciled_rows=reconciled_rows,
            ratings=ratings or [],
            film_title=film_title,
        )
    except Exception:  # noqa: BLE001
        # Never crash the report
        return asdict(ReleaseIntelligence())


def _compute_release_intelligence_inner(
    session: Session,
    film_id: str,
    *,
    reconciled_rows: list[dict] | None,
    ratings: list[dict],
    film_title: str,
) -> dict[str, Any]:
    # ── Load reconciled rows if not provided ──────────────────────────────
    if reconciled_rows is None:
        raw = list(session.execute(
            select(ReconciledEvidence).where(ReconciledEvidence.film_id == film_id)
        ).scalars().all())
        reconciled_rows = [
            {
                "country_code": r.country_code,
                "period_key": r.period_key,
                "period_start_date": r.period_start_date.isoformat() if r.period_start_date else None,
                "period_end_date": r.period_end_date.isoformat() if r.period_end_date else None,
                "period_gross_local": float(r.period_gross_local or 0),
                "cumulative_gross_local": float(r.cumulative_gross_local)
                if r.cumulative_gross_local is not None
                else None,
                "currency": r.currency,
                "granularity": r.record_granularity,
                "semantics": r.record_semantics,
                "admissions_actual": float(r.admissions_actual) if r.admissions_actual is not None else None,
                "admissions_estimated": float(r.admissions_estimated) if r.admissions_estimated is not None else None,
            }
            for r in raw
        ]

    if not ratings:
        raw_ratings = list(session.execute(
            select(RatingsMetric).where(RatingsMetric.film_id == film_id)
        ).scalars().all())
        ratings = [
            {
                "source_name": x.source_name,
                "rating_value": float(x.rating_value or 0),
                "vote_count": x.vote_count,
                "popularity_rank": x.popularity_rank,
            }
            for x in raw_ratings
        ]

    if not film_title:
        film = session.get(Film, film_id)
        if film:
            film_title = film.canonical_title or ""

    # ── Step 1: Build market run profiles ─────────────────────────────────
    by_market: dict[str, list[dict]] = {}
    for row in reconciled_rows:
        cc = row.get("country_code") or "??"
        by_market.setdefault(cc, []).append(row)

    market_profiles: dict[str, MarketRunProfile] = {}
    for cc, rows in by_market.items():
        market_profiles[cc] = build_market_run_profile(film_id, cc, rows)

    # ── Step 2: Build cohorts ─────────────────────────────────────────────
    cohorts = build_market_cohorts(session)

    # ── Step 3: Score each market ─────────────────────────────────────────
    market_intels: dict[str, MarketIntelligence] = {}
    for cc in set(list(CORE_MARKETS) + list(market_profiles.keys())):
        profile = market_profiles.get(cc)
        if not profile or profile.period_count == 0:
            continue
        cohort = cohorts.get(cc)
        market_intels[cc] = score_market_run(profile, cohort)

    # ── Step 4: Core market performance ───────────────────────────────────
    core_market_score = compute_core_market_performance(market_intels)

    # ── Step 5: Audience validation ───────────────────────────────────────
    audience_validation = compute_audience_validation(ratings)

    # ── Step 6: Interest heat ─────────────────────────────────────────────
    interest_heat = compute_interest_heat(ratings)

    # ── Step 7: Confidence ────────────────────────────────────────────────
    confidence_score, confidence_label = compute_confidence(market_profiles, ratings)

    # ── Step 8: Benchmark usefulness ──────────────────────────────────────
    benchmark_usefulness = compute_benchmark_usefulness(
        market_profiles, confidence_score, audience_validation
    )

    # ── Step 9: Commercial potential ──────────────────────────────────────
    commercial_potential = compute_commercial_potential(
        core_market_score, audience_validation, interest_heat,
        confidence_score, benchmark_usefulness,
    )

    # ── Step 10: Forecast bands ───────────────────────────────────────────
    forecast = build_forecast_bands(market_intels, cohorts, confidence_score)

    # ── Step 11: Analyst commentary ───────────────────────────────────────
    commentary = generate_analyst_commentary(
        film_title, market_intels, core_market_score,
        audience_validation, interest_heat, confidence_score,
        confidence_label, benchmark_usefulness, commercial_potential,
        market_profiles,
    )

    # ── Assemble output ──────────────────────────────────────────────────
    market_breakdown: dict[str, dict] = {}
    for cc, mi in market_intels.items():
        profile = market_profiles.get(cc)
        cohort = cohorts.get(cc)
        # Build debug block: intermediate values for transparency/debugging
        debug_block: dict = {}
        if profile:
            debug_block["period_admissions"] = [round(a) for a in profile.period_admissions]
            debug_block["period_grosses"] = [round(g) for g in profile.period_grosses]
            debug_block["peak_period_idx"] = profile.peak_period_idx
            debug_block["build_hold_ratios"] = [round(h, 3) for h in profile.build_hold_ratios]
            debug_block["decay_hold_ratios"] = [round(h, 3) for h in profile.decay_hold_ratios]
            debug_block["weighted_hold_decay_only"] = round(profile.weighted_hold, 3)
            debug_block["run_depth_raw"] = round(profile.run_depth, 3)
            debug_block["admissions_source_mix"] = profile.admissions_source_mix
            debug_block["stability_raw"] = round(profile.stability, 3)
            debug_block["anomaly_severity"] = round(profile.anomaly_severity, 3)
            debug_block["anomaly_flags"] = profile.anomaly_flags
        if cohort:
            oa = sorted(cohort.opening_admissions)
            debug_block["cohort_film_count"] = cohort.film_count
            debug_block["cohort_opening_q25"] = round(_quantile(oa, 0.25)) if oa else None
            debug_block["cohort_opening_q50"] = round(_quantile(oa, 0.50)) if oa else None
            debug_block["cohort_opening_q75"] = round(_quantile(oa, 0.75)) if oa else None
            debug_block["cohort_opening_q90"] = round(_quantile(oa, 0.90)) if oa else None

        market_breakdown[cc] = {
            "opening_admissions": round(mi.opening_admissions),
            "opening_gross": round(mi.opening_gross),
            "tracked_total_admissions": round(mi.tracked_total_admissions),
            "tracked_total_gross": round(mi.tracked_total_gross),
            "currency": mi.currency,
            "period_count": mi.period_count,
            "opening_power_score": round(mi.opening_power_score, 3),
            "peak_strength_score": round(mi.peak_strength_score, 3),
            "tracked_volume_strength_score": round(mi.tracked_volume_strength_score, 3),
            "hold_quality_score": round(mi.hold_quality_score, 3),
            "run_depth_score": round(mi.run_depth_score, 3),
            "stability_score": round(mi.stability_score, 3),
            "market_score": round(mi.market_score, 3),
            "run_shape_label": mi.run_shape_label,
            "evidence_quality": round(mi.evidence_quality, 3),
            "_debug": debug_block,
        }

    return {
        "commercial_potential_score": round(commercial_potential, 3),
        "core_market_performance_score": round(core_market_score, 3),
        "audience_validation_score": round(audience_validation, 3),
        "interest_heat_score": round(interest_heat, 3),
        "confidence_score": round(confidence_score, 3),
        "confidence_label": confidence_label,
        "benchmark_usefulness_score": round(benchmark_usefulness, 3),
        "market_breakdown": market_breakdown,
        "forecast": forecast,
        "analyst_commentary": commentary,
    }
