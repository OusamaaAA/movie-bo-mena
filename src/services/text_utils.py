import re


def normalize_title(value: str) -> str:
    text = value.strip().lower()
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


_ARABIC_TO_LATIN = {
    "ا": "a",
    "أ": "a",
    "إ": "a",
    "آ": "aa",
    "ب": "b",
    "ت": "t",
    "ث": "th",
    "ج": "j",
    "ح": "h",
    "خ": "kh",
    "د": "d",
    "ذ": "dh",
    "ر": "r",
    "ز": "z",
    "س": "s",
    "ش": "sh",
    "ص": "s",
    "ض": "d",
    "ط": "t",
    "ظ": "z",
    "ع": "a",
    "غ": "gh",
    "ف": "f",
    "ق": "q",
    "ك": "k",
    "ل": "l",
    "م": "m",
    "ن": "n",
    "ه": "h",
    "ة": "a",
    "و": "w",
    "ي": "y",
    "ى": "a",
}


def contains_arabic(text: str) -> bool:
    return bool(re.search(r"[\u0600-\u06FF]", text or ""))


def transliterate_arabic_to_latin(text: str) -> str:
    out: list[str] = []
    for ch in text:
        if ch in _ARABIC_TO_LATIN:
            out.append(_ARABIC_TO_LATIN[ch])
        elif ch.strip():
            # Keep whitespace/other symbols to later be normalized by normalize_title.
            out.append(ch)
    return "".join(out)


def normalize_title_cross_language(value: str) -> str:
    """
    Transliteration-aware normalization for Arabic <-> English identity matching.
    - Exact match is still handled upstream.
    - This is only for normalized/fuzzy stages.
    """
    raw = value or ""
    if contains_arabic(raw):
        raw = transliterate_arabic_to_latin(raw)

    # Now run the same normalization we use everywhere else.
    norm = normalize_title(raw)

    # Common Latinization variance: qot <-> kot.
    norm = re.sub(r"\bq", "k", norm)

    # Domain-specific transliteration harmonization (Arabic titles written in Latin).
    token_map = {
        "project": "mashroa",
        "mashroua": "mashroa",
        "mashroo3": "mashroa",
        "mashrua": "mashroa",
        "game": "leab",
        "laeb": "leab",
        "liab": "leab",
        "silm": "selm",
        "selem": "selm",
    }
    toks = [token_map.get(t, t) for t in norm.split()]
    norm = " ".join(toks)
    return norm

