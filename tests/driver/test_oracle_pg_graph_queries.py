"""Tests for Oracle PG graph_queries index builders."""

from graphiti_core.driver.oracle_pg.graph_queries import get_vector_indices
from graphiti_core.driver.oracle_pg.vector_index_params import OraclePGVectorIndexParams


def test_get_vector_indices_returns_three_blocks():
    blocks = get_vector_indices('AcmeGraph')
    assert len(blocks) == 3
    joined = '\n'.join(blocks)
    assert 'FROM user_indexes' in joined
    assert "WHERE index_name = '" in joined
    assert 'CREATE VECTOR INDEX ' in joined
    assert 'IF NOT EXISTS' not in joined
    assert 'NAME_EMBEDDING' in joined
    assert 'FACT_EMBEDDING' in joined
    assert 'DISTANCE COSINE' in joined
    assert 'WITH TARGET ACCURACY 90' in joined
    assert 'PARAMETERS (type IVF, neighbor partitions 10)' in joined
    assert 'ACMEGRAPH_ENTITY_NODES' in joined
    assert 'ACMEGRAPH_COMMUNITY_NODES' in joined
    assert 'ACMEGRAPH_ENTITY_EDGES' in joined
    assert '_VIVF_IDX' in joined or 'VIVF_IDX' in joined


def test_get_vector_indices_custom_params():
    params = OraclePGVectorIndexParams(
        index_type='IVF',
        distance_metric='EUCLIDEAN',
        target_accuracy=85,
        neighbor_partitions=100,
    )
    blocks = get_vector_indices('G', params)
    joined = '\n'.join(blocks)
    assert 'DISTANCE EUCLIDEAN' in joined
    assert 'WITH TARGET ACCURACY 85' in joined
    assert 'neighbor partitions 100)' in joined


def test_vector_index_params_invalid_index_type():
    try:
        OraclePGVectorIndexParams(index_type='HNSW').resolved_sql_tokens()
    except ValueError as e:
        assert 'index_type' in str(e).lower()
    else:
        raise AssertionError('expected ValueError')


def test_vector_index_params_invalid_distance():
    try:
        OraclePGVectorIndexParams(distance_metric='L2').resolved_sql_tokens()
    except ValueError as e:
        assert 'distance_metric' in str(e).lower()
    else:
        raise AssertionError('expected ValueError')
