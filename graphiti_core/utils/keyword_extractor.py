"""
YAKE-based keyword extraction for fulltext query building (e.g. Oracle PG CONTAINS).
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

ORACLE_TEXT_RESERVED: frozenset[str] = frozenset(
    {
        "ABOUT",
        "ACCUM",
        "AND",
        "BT",
        "BTG",
        "BTI",
        "BTP",
        "EQUIV",
        "FUZZY",
        "HASPATH",
        "INPATH",
        "MDATA",
        "MINUS",
        "NEAR",
        "NOT",
        "NT",
        "NTG",
        "NTI",
        "NTP",
        "OR",
        "PT",
        "RT",
        "SQE",
        "SYN",
        "TR",
        "TRSYN",
        "TT",
        "UF",
        "WITHIN",
    }
)

KEYWORD_EXTRACTOR_MAX = 128
_MAX_FALLBACK_QUERY_CHARS = 1000

_RESERVED_ALT = '|'.join(
    re.escape(w) for w in sorted(ORACLE_TEXT_RESERVED, key=len, reverse=True)
)
_RESERVED_PATTERN = re.compile(rf'(?<!\{{)\b({_RESERVED_ALT})\b', re.IGNORECASE)

_CONTAINS_SPECIAL = frozenset('&|!(){}[]^~*?:"\\')
_SINGLE_QUOTE_PATTERN = re.compile(r"[\'’]")


def _contains_quote(text: str) -> str:
    if any(ch.isspace() for ch in text) or any(c in text for c in _CONTAINS_SPECIAL):
        return '"' + text.replace('"', '""') + '"'
    return text


def _escape_reserved_words(text: str) -> str:
    return _RESERVED_PATTERN.sub(lambda m: '{' + m.group(1) + '}', text)


def _remove_single_quotes(text: str) -> str:
    return _SINGLE_QUOTE_PATTERN.sub('', text)


def escape_oracle_reserved_in_text(text: str) -> str:
    stripped = (text or '').strip()
    if not stripped:
        return text or ''
    return _escape_reserved_words(text)


def escape_oracle_reserved_token(token: str) -> str:
    stripped = (token or '').strip()
    if not stripped:
        return stripped
    return '{' + stripped + '}' if stripped.upper() in ORACLE_TEXT_RESERVED else stripped


def format_contains_term(value: str) -> str:
    text = _remove_single_quotes((value or '').strip())
    text = _escape_reserved_words(text)
    return _contains_quote(text) if text else ''


def extract_keywords_yake(
    query: str,
    *,
    lan: str = 'en',
    n: int = 3,
    keyword_max: int = KEYWORD_EXTRACTOR_MAX,
) -> list[str]:
    stripped = (query or '').strip()
    if not stripped:
        return []

    top = max(1, int(keyword_max) - 2)
    try:
        import yake as yake_mod
    except ImportError:
        logger.warning('yake is not installed; keyword extraction skipped')
        return []

    extractor: Any = yake_mod.KeywordExtractor(lan=lan, n=n, top=top)
    raw = extractor.extract_keywords(stripped)
    return [phrase for item in raw if item and (phrase := str(item[0]).strip())]


def build_fulltext_terms_from_query(
    query: str,
    *,
    lan: str = 'en',
    n: int = 3,
    keyword_max: int = KEYWORD_EXTRACTOR_MAX,
) -> str:
    keywords = extract_keywords_yake(query, lan=lan, n=n, keyword_max=keyword_max)
    if keywords:
        return ' AND '.join(format_contains_term(keyword) for keyword in keywords)

    raw = (query or '').strip()
    if len(raw) > _MAX_FALLBACK_QUERY_CHARS:
        raw = raw[:_MAX_FALLBACK_QUERY_CHARS]
    return format_contains_term(raw)
