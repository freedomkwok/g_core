"""
Oracle-specific index query builders.
"""

from __future__ import annotations

from graphiti_core.driver.oracle.rdf_utils import sanitize_oracle_table_base
from graphiti_core.driver.query_executor import QueryExecutor


def _safe_identifier(value: str, *, suffix: str, max_length: int = 30) -> str:
    normalized = f'{value}_{suffix}'.upper().replace('-', '_')
    if len(normalized) <= max_length:
        return normalized
    return normalized[:max_length]


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
        'CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({index_column}) '
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


def get_range_indices(_executor: QueryExecutor) -> list[str]:
    # Oracle range index DDL remains backend-specific.
    return []


def get_fulltext_indices(executor: QueryExecutor) -> list[str]:
    table_prefix = sanitize_oracle_table_base(getattr(executor, '_rdf_graph_name', 'GRAPHITI'))
    entity_nodes = f'{table_prefix}_ENTITY_NODES'
    community_nodes = f'{table_prefix}_COMMUNITY_NODES'
    episodic_nodes = f'{table_prefix}_EPISODIC_NODES'
    entity_edges = f'{table_prefix}_ENTITY_EDGES'

    return [
        _create_ctx_index_block(
            preference_name=_safe_identifier(table_prefix, suffix='entity_mcds'),
            index_name=_safe_identifier(table_prefix, suffix='node_name_and_summary_ctx', max_length=120),
            table_name=entity_nodes,
            index_column='NAME',
            datastore_columns='NAME,SUMMARY',
        ),
        _create_ctx_index_block(
            preference_name=_safe_identifier(table_prefix, suffix='community_mcds'),
            index_name=_safe_identifier(table_prefix, suffix='community_name_ctx', max_length=120),
            table_name=community_nodes,
            index_column='NAME',
            datastore_columns='NAME',
        ),
        _create_ctx_index_block(
            preference_name=_safe_identifier(table_prefix, suffix='episodic_mcds'),
            index_name=_safe_identifier(table_prefix, suffix='episode_content_ctx', max_length=120),
            table_name=episodic_nodes,
            index_column='CONTENT',
            datastore_columns='CONTENT,SOURCE,SOURCE_DESCRIPTION',
        ),
        _create_ctx_index_block(
            preference_name=_safe_identifier(table_prefix, suffix='edge_mcds'),
            index_name=_safe_identifier(table_prefix, suffix='edge_name_and_fact_ctx', max_length=120),
            table_name=entity_edges,
            index_column='NAME',
            datastore_columns='NAME,FACT_TEXT',
        ),
    ]
