"""
Google Apps Script parity for title identity resolution.

Mirrors legacy helpers:
- normalizeLatinTitle_ / normalizeArabicTitle_
- titleSimilarity_
- scoreResolvedTitle_
- titleMatchesQuery_ threshold behavior (used for release gating)
"""
from __future__ import annotations

import re
import unicodedata

from rapidfuzz import fuzz as _rfuzz

YEAR_BONUS = 0.06
YEAR_PENALTY = 0.05

# GS titleMatchesQuery_ returns true when built score >= 0.82
TITLE_MATCH_QUERY_MIN = 0.82
# GS fetchBoxOfficeMojoReleaseEvidence_: secondary gate when title match fails
RELEASE_TITLE_FALLBACK_MIN = 0.78


def normalize_latin_title(text: str | None) -> str:
    text = str(text or "").lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.replace("&", " and ")
    text = re.sub(r"\b(the|a|an)\b", " ", text, flags=re.I)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_arabic_title(text: str | None) -> str:
    text = str(text or "").strip()
    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)
    text = re.sub(r"[إأآا]", "ا", text)
    text = text.replace("ى", "ي")
    text = text.replace("ة", "ه")
    text = text.replace("ؤ", "و")
    text = text.replace("ئ", "ي")
    text = text.replace("ـ", "")
    text = re.sub(r"[^\u0600-\u06FF0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _unique_preserve(tokens: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for t in tokens:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _cross_lang_norm(text: str) -> str:
    """Transliterate Arabic to Latin then normalize, so Latin queries can match Arabic titles."""
    from src.services.text_utils import normalize_title_cross_language  # lazy to avoid circular at module load
    return normalize_title_cross_language(text)


def score_candidate_hit(query: str, text: str | None, release_year_hint: int | None) -> float:
    """Mirror scoreCandidateHit_: fuzzy on Latin+Arabic snippets, small year-in-text bonus.

    Also tries cross-language transliteration so a Latin query ("siko siko") can match
    an Arabic-titled chart entry ("سيكو سيكو") even when direct Latin/Arabic scores are 0.
    """
    text_s = text or ""
    s = max(
        title_similarity(normalize_latin_title(query), normalize_latin_title(text_s)),
        title_similarity(normalize_arabic_title(query), normalize_arabic_title(text_s)),
        _rfuzz.ratio(_cross_lang_norm(query), _cross_lang_norm(text_s)) / 100.0,
    )
    if release_year_hint and text and str(release_year_hint) in str(text):
        s += YEAR_BONUS
    return max(0.0, min(1.0, s))


def title_similarity(a: str | None, b: str | None) -> float:
    a = str(a or "").strip()
    b = str(b or "").strip()
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a in b or b in a:
        return 0.92
    a_tokens = [t for t in a.split() if t]
    b_tokens = [t for t in b.split() if t]
    if not a_tokens or not b_tokens:
        return 0.0
    inter = len([t for t in a_tokens if t in b_tokens])
    union = len(_unique_preserve(a_tokens + b_tokens))
    jaccard = inter / union if union else 0.0
    prefix = 0.08 if (len(a) >= 8 and len(b) >= 8 and a[:8] == b[:8]) else 0.0
    return max(0.0, min(1.0, jaccard + prefix))


def score_resolved_title(
    query: str,
    title_en: str | None,
    title_ar: str | None,
    candidate_year: int | None,
    release_year_hint: int | None,
) -> float:
    en_s = title_similarity(normalize_latin_title(query), normalize_latin_title(title_en or ""))
    ar_s = title_similarity(normalize_arabic_title(query), normalize_arabic_title(title_ar or ""))
    # Cross-language: Latin query against transliterated Arabic title (and vice-versa)
    q_cross = _cross_lang_norm(query)
    en_cross = _rfuzz.ratio(q_cross, _cross_lang_norm(title_en or "")) / 100.0
    ar_cross = _rfuzz.ratio(q_cross, _cross_lang_norm(title_ar or "")) / 100.0
    s = max(en_s, ar_s, en_cross, ar_cross)
    q_l = normalize_latin_title(query)
    if title_en and q_l == normalize_latin_title(title_en):
        s = max(s, 0.98)
    q_a = normalize_arabic_title(query)
    if title_ar and q_a and q_a == normalize_arabic_title(title_ar):
        s = max(s, 0.99)
    if release_year_hint and candidate_year and int(release_year_hint) == int(candidate_year):
        s += YEAR_BONUS
    if release_year_hint and candidate_year and int(release_year_hint) != int(candidate_year):
        s -= YEAR_PENALTY
    return max(0.0, min(1.0, s))


def title_matches_query(
    query: str,
    candidate_en: str | None,
    candidate_ar: str | None,
    candidate_year: int | None,
    release_year_hint: int | None,
) -> bool:
    q_en = normalize_latin_title(query)
    q_ar = normalize_arabic_title(query)
    c_en = normalize_latin_title(candidate_en or "")
    c_ar = normalize_arabic_title(candidate_ar or "")
    score = 0.0
    if q_en and c_en:
        if q_en == c_en:
            score = max(score, 1.0)
        if q_en in c_en or c_en in q_en:
            score = max(score, 0.93)
        score = max(score, title_similarity(q_en, c_en))
    if q_ar and c_ar:
        if q_ar == c_ar:
            score = max(score, 1.0)
        if q_ar in c_ar or c_ar in q_ar:
            score = max(score, 0.95)
        score = max(score, title_similarity(q_ar, c_ar))
    q_compact = q_en.replace(" ", "")
    c_compact = c_en.replace(" ", "")
    if q_compact and c_compact and (q_compact == c_compact or q_compact in c_compact or c_compact in q_compact):
        score = max(score, 0.95)
    if release_year_hint and candidate_year and str(release_year_hint) == str(candidate_year):
        score += YEAR_BONUS
    if release_year_hint and candidate_year and str(release_year_hint) != str(candidate_year):
        score -= YEAR_PENALTY
    return score >= TITLE_MATCH_QUERY_MIN


def year_contradicts_hint(release_year_hint: int | None, candidate_year: int | None) -> bool:
    if release_year_hint is None or candidate_year is None:
        return False
    return int(candidate_year) != int(release_year_hint)


def row_accept_with_parent(
    query: str,
    release_year_hint: int | None,
    row_title_en: str | None,
    row_title_ar: str | None,
    row_year: int | None,
    parent_resolved_score: float,
    *,
    strong_threshold: float,
    review_threshold: float,
    moderate_threshold: float,
) -> bool:
    """
    Candidate-aware acceptance (A + C):
    - strong parent: keep rows unless year contradicts or row is clearly unrelated
    - moderate parent: require row-level check too
    - weak parent: discard
    """
    row_score = score_resolved_title(query, row_title_en, row_title_ar, row_year, release_year_hint)
    if year_contradicts_hint(release_year_hint, row_year):
        return False

    # Systemic fallback: when parent candidate confidence is weak, still accept
    # rows that are themselves very strong title matches (e.g. transliteration
    # variants where discovery/meta scoring is weaker than row title scoring).
    if parent_resolved_score < review_threshold:
        return row_score >= moderate_threshold

    if parent_resolved_score >= strong_threshold:
        if row_score < 0.35:
            return False
        return True

    if parent_resolved_score >= moderate_threshold:
        return row_score >= review_threshold

    return row_score >= review_threshold
