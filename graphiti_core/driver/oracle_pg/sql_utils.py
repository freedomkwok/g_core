"""
Shared SQL helpers for Oracle PG (table-backed) implementation.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any

from graphiti_core.driver.query_executor import QueryExecutor, Transaction

_TABLE_BASES = (
    'entity_nodes',
    'episodic_nodes',
    'community_nodes',
    'saga_nodes',
    'entity_edges',
    'episodic_edges',
    'community_edges',
    'has_episode_edges',
    'next_episode_edges',
)


def sanitize_graph_id(raw_value: str | None) -> str:
    value = (raw_value or '').strip()
    if value == '':
        return 'GRAPHITI'
    value = value.replace(' ', '_').replace('-', '_')
    value = re.sub(r'[^A-Za-z0-9_]', '_', value)
    value = re.sub(r'_+', '_', value).strip('_')
    if value == '':
        return 'GRAPHITI'
    if value[0].isdigit():
        value = f'G_{value}'
    return value.upper()


def get_graph_id_for_executor(executor: QueryExecutor) -> str:
    graph_id = (
        getattr(executor, 'graph_id', None)
        or getattr(executor, '_graph_id', None)
        or getattr(executor, 'rdf_graph_name', None)
        or getattr(executor, '_rdf_graph_name', None)
        or os.getenv('ORACLE_RDF_GRAPH_NAME')
        or os.getenv('ORACLE_RDF_GRAPH')
        or os.getenv('ORACLE_PG_GRAPH_ID')
        or 'GRAPHITI'
    )
    return sanitize_graph_id(graph_id)


def build_table_name(graph_id: str, base_name: str) -> str:
    return f'{sanitize_graph_id(graph_id)}_{base_name.upper()}'


def get_table_name(executor: QueryExecutor, base_name: str) -> str:
    return build_table_name(get_graph_id_for_executor(executor), base_name)


def parse_result_records(result: Any) -> list[dict[str, Any]]:
    if isinstance(result, tuple):
        first = result[0]
        return first if isinstance(first, list) else []
    if isinstance(result, list):
        return result
    return []


async def run_query(
    executor: QueryExecutor,
    query: str,
    tx: Transaction | None = None,
    **params: Any,
) -> list[dict[str, Any]]:
    if tx is not None:
        result = await tx.run(query, **params)
    else:
        result = await executor.execute_query(query, **params)
    return parse_result_records(result)


def to_json_text(value: Any, *, default: Any = None) -> str:
    resolved = default if value is None else value
    return json.dumps(resolved, default=str)


def parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == '':
            return []
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def parse_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == '':
            return {}
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def parse_float_list(value: Any) -> list[float] | None:
    if value is None:
        return None
    if isinstance(value, list):
        out: list[float] = []
        for item in value:
            try:
                out.append(float(item))
            except (TypeError, ValueError):
                continue
        return out
    if isinstance(value, str):
        parsed = parse_json_list(value)
        if not parsed:
            return None
        out: list[float] = []
        for item in parsed:
            try:
                out.append(float(item))
            except (TypeError, ValueError):
                continue
        return out
    return None


def _ddl_block(create_sql: str) -> str:
    return f"""
    BEGIN
      EXECUTE IMMEDIATE '{create_sql}';
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
          RAISE;
        END IF;
    END;
    """


def get_table_ddl_blocks(graph_id: str) -> list[str]:
    prefix = sanitize_graph_id(graph_id)
    entity_nodes = build_table_name(prefix, 'entity_nodes')
    episodic_nodes = build_table_name(prefix, 'episodic_nodes')
    community_nodes = build_table_name(prefix, 'community_nodes')
    saga_nodes = build_table_name(prefix, 'saga_nodes')
    entity_edges = build_table_name(prefix, 'entity_edges')
    episodic_edges = build_table_name(prefix, 'episodic_edges')
    community_edges = build_table_name(prefix, 'community_edges')
    has_episode_edges = build_table_name(prefix, 'has_episode_edges')
    next_episode_edges = build_table_name(prefix, 'next_episode_edges')

    ddls = [
        f"""
        CREATE TABLE {entity_nodes} (
          UUID VARCHAR2(64) PRIMARY KEY,
          GROUP_ID VARCHAR2(200) NOT NULL,
          NAME VARCHAR2(400),
          SUMMARY CLOB,
          LABELS CLOB,
          ATTRIBUTES CLOB,
          CREATED_AT TIMESTAMP,
          NAME_EMBEDDING VECTOR
        )
        """,
        f"""
        CREATE TABLE {episodic_nodes} (
          UUID VARCHAR2(64) PRIMARY KEY,
          GROUP_ID VARCHAR2(200) NOT NULL,
          NAME VARCHAR2(400),
          SOURCE VARCHAR2(64),
          SOURCE_DESCRIPTION CLOB,
          CONTENT CLOB,
          ENTITY_EDGES CLOB,
          CREATED_AT TIMESTAMP,
          VALID_AT TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE {community_nodes} (
          UUID VARCHAR2(64) PRIMARY KEY,
          GROUP_ID VARCHAR2(200) NOT NULL,
          NAME VARCHAR2(400),
          SUMMARY CLOB,
          CREATED_AT TIMESTAMP,
          NAME_EMBEDDING VECTOR
        )
        """,
        f"""
        CREATE TABLE {saga_nodes} (
          UUID VARCHAR2(64) PRIMARY KEY,
          GROUP_ID VARCHAR2(200) NOT NULL,
          NAME VARCHAR2(400),
          CREATED_AT TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE {entity_edges} (
          UUID VARCHAR2(64) PRIMARY KEY,
          GROUP_ID VARCHAR2(200) NOT NULL,
          SRC_UUID VARCHAR2(64) NOT NULL,
          DST_UUID VARCHAR2(64) NOT NULL,
          EDGE_TYPE VARCHAR2(50) NOT NULL,
          NAME VARCHAR2(400),
          FACT_TEXT CLOB,
          EPISODES CLOB,
          ATTRIBUTES CLOB,
          CREATED_AT TIMESTAMP,
          VALID_AT TIMESTAMP,
          INVALID_AT TIMESTAMP,
          EXPIRED_AT TIMESTAMP,
          FACT_EMBEDDING VECTOR
        )
        """,
        f"""
        CREATE TABLE {episodic_edges} (
          UUID VARCHAR2(64) PRIMARY KEY,
          GROUP_ID VARCHAR2(200) NOT NULL,
          SOURCE_NODE_UUID VARCHAR2(64) NOT NULL,
          TARGET_NODE_UUID VARCHAR2(64) NOT NULL,
          CREATED_AT TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE {community_edges} (
          UUID VARCHAR2(64) PRIMARY KEY,
          GROUP_ID VARCHAR2(200) NOT NULL,
          SOURCE_NODE_UUID VARCHAR2(64) NOT NULL,
          TARGET_NODE_UUID VARCHAR2(64) NOT NULL,
          CREATED_AT TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE {has_episode_edges} (
          UUID VARCHAR2(64) PRIMARY KEY,
          GROUP_ID VARCHAR2(200) NOT NULL,
          SOURCE_NODE_UUID VARCHAR2(64) NOT NULL,
          TARGET_NODE_UUID VARCHAR2(64) NOT NULL,
          CREATED_AT TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE {next_episode_edges} (
          UUID VARCHAR2(64) PRIMARY KEY,
          GROUP_ID VARCHAR2(200) NOT NULL,
          SOURCE_NODE_UUID VARCHAR2(64) NOT NULL,
          TARGET_NODE_UUID VARCHAR2(64) NOT NULL,
          CREATED_AT TIMESTAMP
        )
        """,
    ]
    return [_ddl_block(' '.join(ddl.split()).replace("'", "''")) for ddl in ddls]


def get_property_graph_name(graph_id: str) -> str:
    return f'{sanitize_graph_id(graph_id)}_PG'


def get_property_graph_create_block(graph_id: str) -> str:
    prefix = sanitize_graph_id(graph_id)
    graph_name = get_property_graph_name(prefix)
    entity_nodes = build_table_name(prefix, 'entity_nodes')
    episodic_nodes = build_table_name(prefix, 'episodic_nodes')
    community_nodes = build_table_name(prefix, 'community_nodes')
    saga_nodes = build_table_name(prefix, 'saga_nodes')
    entity_edges = build_table_name(prefix, 'entity_edges')
    episodic_edges = build_table_name(prefix, 'episodic_edges')
    community_edges = build_table_name(prefix, 'community_edges')
    has_episode_edges = build_table_name(prefix, 'has_episode_edges')
    next_episode_edges = build_table_name(prefix, 'next_episode_edges')

    create_sql = ' '.join(
        f"""
        CREATE PROPERTY GRAPH {graph_name}
          VERTEX TABLES (
            {entity_nodes}
              KEY (uuid)
              LABEL Entity
              PROPERTIES (uuid, group_id, name, summary, labels, attributes, created_at, name_embedding),
            {episodic_nodes}
              KEY (uuid)
              LABEL Episodic
              PROPERTIES (
                uuid, group_id, name, source, source_description, content, entity_edges, created_at, valid_at
              ),
            {community_nodes}
              KEY (uuid)
              LABEL Community
              PROPERTIES (uuid, group_id, name, summary, created_at, name_embedding),
            {saga_nodes}
              KEY (uuid)
              LABEL Saga
              PROPERTIES (uuid, group_id, name, created_at)
          )
          EDGE TABLES (
            {entity_edges}
              KEY (uuid)
              SOURCE KEY (src_uuid) REFERENCES {entity_nodes} (uuid)
              DESTINATION KEY (dst_uuid) REFERENCES {entity_nodes} (uuid)
              LABEL RELATES_TO
              PROPERTIES (
                uuid, group_id, edge_type, name, fact_text, episodes,
                attributes, created_at, valid_at, invalid_at, expired_at, fact_embedding
              ),
            {episodic_edges}
              KEY (uuid)
              SOURCE KEY (source_node_uuid) REFERENCES {episodic_nodes} (uuid)
              DESTINATION KEY (target_node_uuid) REFERENCES {entity_nodes} (uuid)
              LABEL MENTIONS
              PROPERTIES (uuid, group_id, source_node_uuid, target_node_uuid, created_at),
            {community_edges}
              KEY (uuid)
              SOURCE KEY (source_node_uuid) REFERENCES {community_nodes} (uuid)
              DESTINATION KEY (target_node_uuid) REFERENCES {entity_nodes} (uuid)
              LABEL HAS_MEMBER
              PROPERTIES (uuid, group_id, source_node_uuid, target_node_uuid, created_at),
            {has_episode_edges}
              KEY (uuid)
              SOURCE KEY (source_node_uuid) REFERENCES {saga_nodes} (uuid)
              DESTINATION KEY (target_node_uuid) REFERENCES {episodic_nodes} (uuid)
              LABEL HAS_EPISODE
              PROPERTIES (uuid, group_id, source_node_uuid, target_node_uuid, created_at),
            {next_episode_edges}
              KEY (uuid)
              SOURCE KEY (source_node_uuid) REFERENCES {episodic_nodes} (uuid)
              DESTINATION KEY (target_node_uuid) REFERENCES {episodic_nodes} (uuid)
              LABEL NEXT_EPISODE
              PROPERTIES (uuid, group_id, source_node_uuid, target_node_uuid, created_at)
          )
        """.split()
    )
    return f"""
    DECLARE
      v_exists NUMBER := 0;
    BEGIN
      BEGIN
        SELECT COUNT(*) INTO v_exists
        FROM USER_PROPERTY_GRAPHS
        WHERE GRAPH_NAME = '{graph_name}';
      EXCEPTION
        WHEN OTHERS THEN
          v_exists := 0;
      END;
      IF v_exists = 0 THEN
        EXECUTE IMMEDIATE q'[{create_sql}]';
      END IF;
    END;
    """


def get_property_graph_drop_block(graph_id: str) -> str:
    graph_name = get_property_graph_name(graph_id)
    return f"""
    BEGIN
      EXECUTE IMMEDIATE 'DROP PROPERTY GRAPH {graph_name}';
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
    """


def get_table_drop_blocks(graph_id: str) -> list[str]:
    prefix = sanitize_graph_id(graph_id)
    tables = [
        build_table_name(prefix, 'entity_edges'),
        build_table_name(prefix, 'episodic_edges'),
        build_table_name(prefix, 'community_edges'),
        build_table_name(prefix, 'has_episode_edges'),
        build_table_name(prefix, 'next_episode_edges'),
        build_table_name(prefix, 'community_nodes'),
        build_table_name(prefix, 'saga_nodes'),
        build_table_name(prefix, 'episodic_nodes'),
        build_table_name(prefix, 'entity_nodes'),
    ]
    return [
        f"""
        BEGIN
          EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE';
        EXCEPTION
          WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
              RAISE;
            END IF;
        END;
        """
        for table_name in tables
    ]


def validate_table_base(base_name: str) -> str:
    normalized = base_name.lower().strip()
    if normalized not in _TABLE_BASES:
        raise ValueError(f'Unknown Oracle PG table base: {base_name}')
    return normalized


def coerce_timestamp(value: datetime | None) -> datetime | None:
    return value


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_in_list(values: list[str]) -> str:
    if not values:
        return "('')"
    return '(' + ', '.join(sql_string_literal(v) for v in values) + ')'
