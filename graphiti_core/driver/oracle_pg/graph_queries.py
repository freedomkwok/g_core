"""
Oracle PG-specific index query builders.
"""

from __future__ import annotations

from graphiti_core.driver.oracle_pg.sql_utils import build_table_name, sanitize_graph_id
from graphiti_core.driver.oracle_pg.vector_index_params import OraclePGVectorIndexParams

VECTOR_INDEX_SUFFIX_TOKEN = 'VIVF_IDX'


def _safe_identifier(value: str, *, suffix: str, max_length: int = 30) -> str:
    normalized = f'{value}_{suffix}'.upper().replace('-', '_')
    if len(normalized) <= max_length:
        return normalized
    return normalized[:max_length]


def _create_btree_index_block(index_name: str, table_name: str, column_name: str) -> str:
    return f"""
    BEGIN
      EXECUTE IMMEDIATE 'CREATE INDEX {index_name} ON {table_name} ({column_name})';
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
          RAISE;
        END IF;
    END;
    """


def _create_vector_index_block(
    index_name: str,
    table_name: str,
    column_name: str,
    index_type: str,
    distance_metric: str,
    target_accuracy: int,
    neighbor_partitions: int,
) -> str:
    ddl = (
        f'CREATE VECTOR INDEX {index_name} ON {table_name} ({column_name}) '
        f'ORGANIZATION NEIGHBOR PARTITIONS '
        f'DISTANCE {distance_metric} '
        f'WITH TARGET ACCURACY {target_accuracy} '
        f'PARAMETERS (type {index_type}, neighbor partitions {neighbor_partitions})'
    )
    ddl_escaped = ddl.replace("'", "''")
    index_name_literal = index_name.replace("'", "''")
    return f"""
    DECLARE
      v_index_exists NUMBER := 0;
    BEGIN
      SELECT COUNT(*)
      INTO v_index_exists
      FROM user_indexes
      WHERE index_name = '{index_name_literal}';

      IF v_index_exists = 0 THEN
        BEGIN
          EXECUTE IMMEDIATE '{ddl_escaped}';
        EXCEPTION
          WHEN OTHERS THEN
            IF SQLCODE != -955 THEN
              RAISE;
            END IF;
        END;
      END IF;
    END;
    """


def get_vector_indices(
    graph_id: str, params: OraclePGVectorIndexParams | None = None
) -> list[str]:
    """IVF vector indexes for tables that define VECTOR embedding columns."""
    index_type, distance_metric, target_accuracy, neighbor_partitions = (
        (params or OraclePGVectorIndexParams()).resolved_sql_tokens()
    )
    prefix = sanitize_graph_id(graph_id)
    targets: list[tuple[str, str]] = [
        ('entity_nodes', 'NAME_EMBEDDING'),
        ('community_nodes', 'NAME_EMBEDDING'),
        ('entity_edges', 'FACT_EMBEDDING'),
    ]
    blocks: list[str] = []
    for base_name, column_name in targets:
        table_name = build_table_name(prefix, base_name)
        index_name = _safe_identifier(
            prefix,
            suffix=f'{base_name}_{column_name.lower()}_{VECTOR_INDEX_SUFFIX_TOKEN.lower()}',
            max_length=120,
        )
        blocks.append(
            _create_vector_index_block(
                index_name,
                table_name,
                column_name,
                index_type,
                distance_metric,
                target_accuracy,
                neighbor_partitions,
            )
        )
    return blocks


def _create_ctx_index_block(
    preference_name: str,
    index_name: str,
    table_name: str,
    index_column: str,
    datastore_columns: str,
) -> str:
    return f"""
    DECLARE
      v_pref_count NUMBER := 0;
    BEGIN
      SELECT COUNT(*)
      INTO v_pref_count
      FROM CTX_PREFERENCES
      WHERE PRE_NAME = UPPER('{preference_name}');

      IF v_pref_count = 0 THEN
        CTX_DDL.CREATE_PREFERENCE('{preference_name}', 'MULTI_COLUMN_DATASTORE');
      END IF;

      CTX_DDL.SET_ATTRIBUTE('{preference_name}', 'COLUMNS', '{datastore_columns}');

      BEGIN
      EXECUTE IMMEDIATE
        'CREATE INDEX {index_name} ON {table_name} ({index_column}) '
        || 'INDEXTYPE IS CTXSYS.CONTEXT '
        || 'PARAMETERS (''DATASTORE {preference_name}'')';
      EXCEPTION
        WHEN OTHERS THEN
          IF SQLCODE != -955 THEN
            RAISE;
          END IF;
      END;
    END;
    """


def get_range_indices(graph_id: str) -> list[str]:
    prefix = sanitize_graph_id(graph_id)
    table_defs = {
        'entity_nodes': ['GROUP_ID', 'NAME'],
        'episodic_nodes': ['GROUP_ID', 'VALID_AT'],
        'community_nodes': ['GROUP_ID', 'NAME'],
        'saga_nodes': ['GROUP_ID', 'NAME'],
        'entity_edges': ['GROUP_ID', 'SRC_UUID', 'DST_UUID'],
        'episodic_edges': ['GROUP_ID', 'SOURCE_NODE_UUID', 'TARGET_NODE_UUID'],
        'community_edges': ['GROUP_ID', 'SOURCE_NODE_UUID', 'TARGET_NODE_UUID'],
        'has_episode_edges': ['GROUP_ID', 'SOURCE_NODE_UUID', 'TARGET_NODE_UUID'],
        'next_episode_edges': ['GROUP_ID', 'SOURCE_NODE_UUID', 'TARGET_NODE_UUID'],
    }

    blocks: list[str] = []
    for base_name, columns in table_defs.items():
        table_name = build_table_name(prefix, base_name)
        for column in columns:
            index_name = _safe_identifier(prefix, suffix=f'{base_name}_{column}_idx', max_length=120)
            blocks.append(_create_btree_index_block(index_name, table_name, column))
    return blocks


def get_fulltext_indices(graph_id: str) -> list[str]:
    prefix = sanitize_graph_id(graph_id)
    entity_nodes = build_table_name(prefix, 'entity_nodes')
    community_nodes = build_table_name(prefix, 'community_nodes')
    episodic_nodes = build_table_name(prefix, 'episodic_nodes')
    entity_edges = build_table_name(prefix, 'entity_edges')

    return [
        _create_ctx_index_block(
            preference_name=_safe_identifier(prefix, suffix='entity_mcds'),
            index_name=_safe_identifier(prefix, suffix='node_name_and_summary_ctx', max_length=120),
            table_name=entity_nodes,
            index_column='NAME',
            datastore_columns='NAME,SUMMARY',
        ),
        _create_ctx_index_block(
            preference_name=_safe_identifier(prefix, suffix='community_mcds'),
            index_name=_safe_identifier(prefix, suffix='community_name_ctx', max_length=120),
            table_name=community_nodes,
            index_column='NAME',
            datastore_columns='NAME',
        ),
        _create_ctx_index_block(
            preference_name=_safe_identifier(prefix, suffix='episodic_mcds'),
            index_name=_safe_identifier(prefix, suffix='episode_content_ctx', max_length=120),
            table_name=episodic_nodes,
            index_column='CONTENT',
            datastore_columns='CONTENT,SOURCE,SOURCE_DESCRIPTION',
        ),
        _create_ctx_index_block(
            preference_name=_safe_identifier(prefix, suffix='edge_mcds'),
            index_name=_safe_identifier(prefix, suffix='edge_name_and_fact_ctx', max_length=120),
            table_name=entity_edges,
            index_column='NAME',
            datastore_columns='NAME,FACT_TEXT',
        ),
    ]
