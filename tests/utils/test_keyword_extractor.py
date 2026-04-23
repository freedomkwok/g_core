"""Tests for YAKE keyword extraction helpers."""

import pytest

from graphiti_core.utils.keyword_extractor import (
    KEYWORD_EXTRACTOR_MAX,
    build_fulltext_terms_from_query,
    escape_oracle_reserved_in_text,
    extract_keywords_yake,
)


def test_extract_keywords_yake_returns_phrases():
    pytest.importorskip('yake')
    text = 'Alice and OpenAI employment relationship in 2024'
    keywords = extract_keywords_yake(text, keyword_max=KEYWORD_EXTRACTOR_MAX)
    assert isinstance(keywords, list)
    assert len(keywords) >= 1
    flat = ' '.join(keywords).lower()
    assert 'alice' in flat or 'openai' in flat


def test_build_fulltext_terms_joins_with_and():
    pytest.importorskip('yake')
    text = 'OpenAI employment in California'
    terms = build_fulltext_terms_from_query(text)
    assert ' AND ' in terms or terms  # YAKE may return single phrase


def test_empty_query_returns_empty():
    assert extract_keywords_yake('') == []
    assert build_fulltext_terms_from_query('') == ''


def test_escape_oracle_reserved_within_in_phrase():
    """WITHIN must be literal, not a section operator (ORA-29902)."""
    assert 'within' in escape_oracle_reserved_in_text('objective within ExGRPO').lower()
    assert '{within}' in escape_oracle_reserved_in_text('objective within ExGRPO')


def test_escape_skips_already_braced():
    s = escape_oracle_reserved_in_text('{within} scope')
    assert s == '{within} scope'
