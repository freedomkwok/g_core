"""
Helpers for Oracle RDF/SPARQL update integration.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any

from graphiti_core.driver.query_executor import QueryExecutor, Transaction

_TRUTHY = {'1', 'true', 'yes', 'on'}


def rdf_mode_enabled() -> bool:
    return os.getenv('ORACLE_USE_RDF', 'false').strip().lower() in _TRUTHY


def rdf_mode_for_executor(executor: QueryExecutor) -> bool:
    executor_mode = getattr(executor, 'rdf_enabled', None)
    if isinstance(executor_mode, bool):
        return executor_mode
    return rdf_mode_enabled()


def get_rdf_identifiers() -> tuple[str, str, str]:
    owner = (os.getenv('ORACLE_RDF_NETWORK_OWNER') or os.getenv('ORACLE_USER') or '').upper()
    network = (
        os.getenv('ORACLE_RDF_NETWORK_NAME') or os.getenv('ORACLE_RDF_NETWORK') or 'NET1'
    ).upper()
    graph = os.getenv('ORACLE_RDF_GRAPH_NAME') or os.getenv('ORACLE_RDF_GRAPH') or 'GRAPHITI'
    return owner, network, sanitize_rdf_graph_name(graph)


def _normalized_identifier(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if stripped == '':
        return None
    return stripped


def sanitize_oracle_table_base(raw_value: str | None) -> str:
    """
    Normalize a user-provided table base into a safe Oracle identifier fragment.

    Spaces and hyphens are converted to underscores as requested by OCI naming
    conventions used by Graphiti's embedding side tables.
    """
    return sanitize_rdf_graph_name(raw_value).upper()


def sanitize_rdf_graph_name(raw_value: str | None) -> str:
    """
    Normalize RDF graph names to Oracle-safe identifier characters.

    Unlike table-base sanitization, this preserves letter case so existing graph
    naming remains stable while still replacing invalid characters.
    """
    value = (raw_value or '').strip()
    if value == '':
        return 'GRAPHITI'

    value = value.replace(' ', '_').replace('-', '_')
    value = re.sub(r'[^A-Za-z0-9_]', '_', value)
    value = re.sub(r'_+', '_', value).strip('_')
    if value == '':
        return 'GRAPHITI'
    if value[0].isdigit():
        value = f'T_{value}'
    return value


def get_rdf_identifiers_for_executor(executor: QueryExecutor) -> tuple[str, str, str]:
    owner, network, graph = get_rdf_identifiers()

    owner_override = _normalized_identifier(getattr(executor, 'rdf_network_owner', None))
    network_override = _normalized_identifier(getattr(executor, 'rdf_network_name', None))
    graph_override = _normalized_identifier(getattr(executor, 'rdf_graph_name', None))

    # Backward-compatible fallback for older executors that only expose private fields.
    owner_override = owner_override or _normalized_identifier(getattr(executor, '_rdf_network_owner', None))
    network_override = network_override or _normalized_identifier(
        getattr(executor, '_rdf_network_name', None)
    )
    graph_override = graph_override or _normalized_identifier(getattr(executor, '_rdf_graph_name', None))

    if owner_override is not None:
        owner = owner_override.upper()
    if network_override is not None:
        network = network_override.upper()
    if graph_override is not None:
        graph = graph_override

    graph = sanitize_rdf_graph_name(graph)

    return owner, network, graph


def get_rdf_table_base_for_executor(executor: QueryExecutor) -> str:
    table_base = (
        _normalized_identifier(getattr(executor, 'rdf_graph_name', None))
        or _normalized_identifier(getattr(executor, '_rdf_graph_name', None))
        or _normalized_identifier(os.getenv('ORACLE_RDF_GRAPH_NAME'))
        or _normalized_identifier(os.getenv('ORACLE_RDF_GRAPH'))
        or 'GRAPHITI'
    )
    return sanitize_oracle_table_base(table_base)


def get_rdf_namespace_prefix_for_executor(executor: QueryExecutor) -> str:
    explicit_prefix = (
        _normalized_identifier(getattr(executor, 'rdf_namespace_prefix', None))
        or _normalized_identifier(getattr(executor, '_rdf_namespace_prefix', None))
        or _normalized_identifier(os.getenv('ORACLE_RDF_NAMESPACE_PREFIX'))
    )
    if explicit_prefix:
        return explicit_prefix if explicit_prefix.endswith(':') else f'{explicit_prefix}:'
    return f'gti:{get_rdf_table_base_for_executor(executor)}:'


def _apply_rdf_namespace(query_text: str, executor: QueryExecutor) -> str:
    namespace_prefix = get_rdf_namespace_prefix_for_executor(executor)
    legacy_prefix = 'urn:' + 'graphiti:'
    return query_text.replace(legacy_prefix, namespace_prefix).replace('gti:', namespace_prefix)


def get_rdf_table_name(
    table_name: str | None = None,
    network_owner: str | None = None,
    network_name: str | None = None,
) -> str:
    owner, network, graph = get_rdf_identifiers()
    owner = (network_owner or owner).upper()
    network = (network_name or network).upper()
    rdf_table = (table_name or graph).upper()
    return f'{owner}.{network}#RDFT_{rdf_table}'


def _escape_sparql_string(value: str) -> str:
    return (
        value.replace('\\', '\\\\')
        .replace('"', '\\"')
        .replace('\n', '\\n')
        .replace('\r', '\\r')
    )


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def sparql_string_literal(value: str) -> str:
    return f'"{_escape_sparql_string(value)}"'


def sparql_datetime_literal(value: datetime) -> str:
    dt_value = value
    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=timezone.utc)
    return (
        f'"{_escape_sparql_string(dt_value.isoformat())}"'
        '^^<http://www.w3.org/2001/XMLSchema#dateTime>'
    )


def _parse_json_str(value: str) -> Any:
    text = value.strip()
    if text == '':
        return None
    return json.loads(text)


def parse_json_list_literal(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = _parse_json_str(value)
        except json.JSONDecodeError:
            if '|' in value:
                return [part for part in value.split('|') if part]
            return [value]
        if isinstance(parsed, list):
            return parsed
        if parsed is None:
            return []
        return [parsed]
    return [value]


def parse_json_dict_literal(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = _parse_json_str(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
        return {}
    return {}


def parse_float_list_literal(value: Any) -> list[float] | None:
    if value is None:
        return None
    if hasattr(value, 'tolist') and callable(value.tolist):
        try:
            value = value.tolist()
        except Exception:
            pass
    if isinstance(value, tuple):
        value = list(value)
    parsed = parse_json_list_literal(value)
    floats: list[float] = []
    for item in parsed:
        try:
            floats.append(float(item))
        except (TypeError, ValueError):
            continue
    return floats


async def execute_sem_match_select(
    executor: QueryExecutor,
    sparql_query: str,
    select_columns: list[str],
    *,
    options: str | None = None,
    order_by_sem_rownum: bool = False,
) -> list[dict[str, Any]]:
    network_owner, network_name, graph_name = get_rdf_identifiers_for_executor(executor)
    if network_owner == '':
        raise ValueError(
            'ORACLE_RDF_NETWORK_OWNER (or ORACLE_USER) must be set when ORACLE_USE_RDF=true.'
        )

    escaped_sparql = _escape_sql_literal(_apply_rdf_namespace(sparql_query, executor))
    escaped_graph = _escape_sql_literal(graph_name)
    escaped_owner = _escape_sql_literal(network_owner)
    escaped_network = _escape_sql_literal(network_name)
    options_sql = (
        f"'{_escape_sql_literal(options)}'" if options is not None else 'NULL'
    )
    sql_columns = ',\n                '.join(select_columns)

    query = f"""
    SELECT
        {sql_columns}
    FROM TABLE(
        SEM_MATCH(
            '{escaped_sparql}',
            SEM_MODELS('{escaped_graph}'),
            NULL,
            NULL,
            NULL,
            NULL,
            {options_sql},
            NULL,
            NULL,
            '{escaped_owner}',
            '{escaped_network}'
        )
    )
    """
    if order_by_sem_rownum:
        query += '\nORDER BY sem$rownum'

    records, _, _ = await executor.execute_query(query)
    return records


async def execute_sem_match_join_select(
    executor: QueryExecutor,
    sparql_query: str,
    select_columns: list[str],
    *,
    join_table: str,
    join_key: str = 'uuid',
    table_alias: str = 'e',
    sem_alias: str = 'm',
    options: str | None = None,
    order_by_sem_rownum: bool = False,
    limit: int | None = None,
    left_join: bool = False,
) -> list[dict[str, Any]]:
    network_owner, network_name, graph_name = get_rdf_identifiers_for_executor(executor)
    if network_owner == '':
        raise ValueError(
            'ORACLE_RDF_NETWORK_OWNER (or ORACLE_USER) must be set when ORACLE_USE_RDF=true.'
        )

    escaped_sparql = _escape_sql_literal(_apply_rdf_namespace(sparql_query, executor))
    escaped_graph = _escape_sql_literal(graph_name)
    escaped_owner = _escape_sql_literal(network_owner)
    escaped_network = _escape_sql_literal(network_name)
    options_sql = f"'{_escape_sql_literal(options)}'" if options is not None else 'NULL'
    sql_columns = ',\n        '.join(select_columns)

    query = f"""
    SELECT
        {sql_columns}
    FROM {join_table} {table_alias}
    {'LEFT JOIN' if left_join else 'JOIN'} TABLE(
        SEM_MATCH(
            '{escaped_sparql}',
            SEM_MODELS('{escaped_graph}'),
            NULL,
            NULL,
            NULL,
            NULL,
            {options_sql},
            NULL,
            NULL,
            '{escaped_owner}',
            '{escaped_network}'
        )
    ) {sem_alias}
    ON {table_alias}.{join_key} = {sem_alias}.{join_key}
    """
    if order_by_sem_rownum:
        query += f'\nORDER BY {sem_alias}.sem$rownum'
    if limit is not None:
        query += f'\nFETCH FIRST {int(limit)} ROWS ONLY'

    records, _, _ = await executor.execute_query(query)
    return records


def _to_sparql_literal(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return (
            f'"{_escape_sparql_string(value.isoformat())}"'
            '^^<http://www.w3.org/2001/XMLSchema#dateTime>'
        )
    if isinstance(value, bool):
        literal = 'true' if value else 'false'
        return f'"{literal}"^^<http://www.w3.org/2001/XMLSchema#boolean>'
    if isinstance(value, (int, float)):
        return f'"{value}"^^<http://www.w3.org/2001/XMLSchema#decimal>'
    if isinstance(value, (dict, list)):
        value = json.dumps(value, default=str)
    return f'"{_escape_sparql_string(str(value))}"'


def _predicate(name: str) -> str:
    return f'<gti:pred:{name}>'


def build_node_subject(node_kind: str, uuid: str) -> str:
    return f'<gti:node:{node_kind}:{uuid}>'


def build_edge_subject(edge_kind: str, uuid: str) -> str:
    return f'<gti:edge:{edge_kind}:{uuid}>'


def build_subject_upsert_update(subject: str, properties: dict[str, Any]) -> str:
    insert_lines: list[str] = []
    for key, value in properties.items():
        if value is None:
            continue
        insert_lines.append(f'{subject} {_predicate(key)} {_to_sparql_literal(value)} .')

    delete_query = f'DELETE WHERE {{ {subject} ?p ?o . }}'
    if len(insert_lines) == 0:
        return delete_query
    return delete_query + '; INSERT DATA { ' + ' '.join(insert_lines) + ' }'


def build_delete_subjects_update(subjects: list[str]) -> str:
    return '; '.join([f'DELETE WHERE {{ {subject} ?p ?o . }}' for subject in subjects])


def build_delete_by_property_update(property_name: str, property_value: Any) -> str:
    return (
        'DELETE WHERE { '
        f'?s {_predicate(property_name)} {_to_sparql_literal(property_value)} . '
        '?s ?p ?o . '
        '}'
    )


def _update_rdf_graph_block(options: str | None) -> str:
    if options is None:
        return """
            BEGIN
              sem_apis.update_rdf_graph(
                $graph_name,
                $update_query,
                network_owner=>$network_owner,
                network_name=>$network_name
              );
            END;
        """
    return """
        BEGIN
          sem_apis.update_rdf_graph(
            $graph_name,
            $update_query,
            options=>$options,
            network_owner=>$network_owner,
            network_name=>$network_name
          );
        END;
    """


async def execute_sparql_update(
    executor: QueryExecutor,
    update_query: str,
    tx: Transaction | None = None,
    options: str | None = None,
) -> None:
    network_owner, network_name, graph_name = get_rdf_identifiers_for_executor(executor)
    if network_owner == '':
        raise ValueError(
            'ORACLE_RDF_NETWORK_OWNER (or ORACLE_USER) must be set when ORACLE_USE_RDF=true.'
        )

    query = _update_rdf_graph_block(options)
    params: dict[str, Any] = {
        'graph_name': graph_name,
        'update_query': _apply_rdf_namespace(update_query, executor),
        'network_owner': network_owner,
        'network_name': network_name,
    }
    if options is not None:
        params['options'] = options

    if tx is not None:
        await tx.run(query, **params)
    else:
        await executor.execute_query(query, **params)


def get_embedding_table_name(executor: QueryExecutor, suffix: str) -> str:
    return f'{get_rdf_table_base_for_executor(executor)}_{suffix.upper()}'


def get_embedding_dimension_for_executor(executor: QueryExecutor) -> int:
    configured = (
        _normalized_identifier(getattr(executor, 'embedding_dimension', None))
        or _normalized_identifier(getattr(executor, '_embedding_dimension', None))
        or _normalized_identifier(os.getenv('ORACLE_EMBEDDING_DIMENSION'))
        or _normalized_identifier(os.getenv('ORACLE_EMBEDDING_DIM'))
    )
    if configured is None:
        return 3072
    try:
        dimension = int(configured)
        return dimension if dimension > 0 else 3072
    except ValueError:
        return 3072


async def upsert_episodic_node_embedding(
    executor: QueryExecutor,
    uuid: str,
    embedding: list[float] | None,
    content: str,
    tx: Transaction | None = None,
) -> None:
    await ensure_embedding_table(executor)
    embedding_table = get_embedding_table_name(executor, 'EPISODIC_NODES')
    embedding_json = json.dumps(embedding) if embedding is not None else None
    merge_sql = f"""
    MERGE INTO {embedding_table} t
    USING (
      SELECT $uuid AS uuid, TO_VECTOR($embedding_json) AS content_embedding, $content AS content
      FROM dual
    ) s
    ON (t.uuid = s.uuid)
    WHEN MATCHED THEN
      UPDATE SET
        t.content_embedding = s.content_embedding,
        t.content = s.content,
        t.updated_at = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
      INSERT (uuid, content_embedding, content, updated_at)
      VALUES (s.uuid, s.content_embedding, s.content, SYSTIMESTAMP)
    """
    params = {'uuid': uuid, 'embedding_json': embedding_json, 'content': content}
    if tx is not None:
        await tx.run(merge_sql, **params)
    else:
        await executor.execute_query(merge_sql, **params)


async def upsert_episodic_node_embeddings_bulk(
    executor: QueryExecutor,
    values: list[tuple[str, list[float] | None, str]],
    tx: Transaction | None = None,
) -> None:
    await ensure_embedding_table(executor)
    if not values:
        return
    embedding_table = get_embedding_table_name(executor, 'EPISODIC_NODES')
    payload = [
        {
            'uuid': uuid,
            'embedding_json': json.dumps(embedding) if embedding is not None else None,
            'content': content,
        }
        for uuid, embedding, content in values
    ]
    merge_sql = f"""
    MERGE INTO {embedding_table} t
    USING (
      SELECT jt.uuid, TO_VECTOR(jt.embedding_json) AS content_embedding, jt.content
      FROM JSON_TABLE(
        $payload_json,
        '$[*]' COLUMNS (
          uuid VARCHAR2(255) PATH '$.uuid',
          embedding_json CLOB PATH '$.embedding_json',
          content CLOB PATH '$.content'
        )
      ) jt
    ) s
    ON (t.uuid = s.uuid)
    WHEN MATCHED THEN
      UPDATE SET
        t.content_embedding = s.content_embedding,
        t.content = s.content,
        t.updated_at = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
      INSERT (uuid, content_embedding, content, updated_at)
      VALUES (s.uuid, s.content_embedding, s.content, SYSTIMESTAMP)
    """
    params = {'payload_json': json.dumps(payload)}
    if tx is not None:
        await tx.run(merge_sql, **params)
    else:
        await executor.execute_query(merge_sql, **params)


async def upsert_entity_node_embedding(
    executor: QueryExecutor,
    uuid: str,
    embedding: list[float] | None,
    summary: str | None,
    tx: Transaction | None = None,
) -> None:
    await ensure_embedding_table(executor)
    embedding_table = get_embedding_table_name(executor, 'ENTITY_NODES')
    embedding_json = json.dumps(embedding) if embedding is not None else None
    merge_sql = f"""
    MERGE INTO {embedding_table} t
    USING (
      SELECT $uuid AS uuid, TO_VECTOR($embedding_json) AS name_embedding, $summary AS summary
      FROM dual
    ) s
    ON (t.uuid = s.uuid)
    WHEN MATCHED THEN
      UPDATE SET
        t.name_embedding = s.name_embedding,
        t.summary = s.summary,
        t.updated_at = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
      INSERT (uuid, name_embedding, summary, updated_at)
      VALUES (s.uuid, s.name_embedding, s.summary, SYSTIMESTAMP)
    """
    params = {'uuid': uuid, 'embedding_json': embedding_json, 'summary': summary}
    if tx is not None:
        await tx.run(merge_sql, **params)
    else:
        await executor.execute_query(merge_sql, **params)


async def upsert_entity_node_embeddings_bulk(
    executor: QueryExecutor,
    values: list[tuple[str, list[float] | None, str | None]],
    tx: Transaction | None = None,
) -> None:
    await ensure_embedding_table(executor)
    if not values:
        return
    embedding_table = get_embedding_table_name(executor, 'ENTITY_NODES')
    payload = [
        {
            'uuid': uuid,
            'embedding_json': json.dumps(embedding) if embedding is not None else None,
            'summary': summary,
        }
        for uuid, embedding, summary in values
    ]
    merge_sql = f"""
    MERGE INTO {embedding_table} t
    USING (
      SELECT jt.uuid, TO_VECTOR(jt.embedding_json) AS name_embedding, jt.summary
      FROM JSON_TABLE(
        $payload_json,
        '$[*]' COLUMNS (
          uuid VARCHAR2(255) PATH '$.uuid',
          embedding_json CLOB PATH '$.embedding_json',
          summary CLOB PATH '$.summary'
        )
      ) jt
    ) s
    ON (t.uuid = s.uuid)
    WHEN MATCHED THEN
      UPDATE SET
        t.name_embedding = s.name_embedding,
        t.summary = s.summary,
        t.updated_at = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
      INSERT (uuid, name_embedding, summary, updated_at)
      VALUES (s.uuid, s.name_embedding, s.summary, SYSTIMESTAMP)
    """
    params = {'payload_json': json.dumps(payload)}
    if tx is not None:
        await tx.run(merge_sql, **params)
    else:
        await executor.execute_query(merge_sql, **params)


async def upsert_community_node_embedding(
    executor: QueryExecutor,
    uuid: str,
    embedding: list[float] | None,
    summary: str | None,
    tx: Transaction | None = None,
) -> None:
    await ensure_embedding_table(executor)
    embedding_table = get_embedding_table_name(executor, 'COMMUNITY_NODES')
    embedding_json = json.dumps(embedding) if embedding is not None else None
    merge_sql = f"""
    MERGE INTO {embedding_table} t
    USING (
      SELECT $uuid AS uuid, TO_VECTOR($embedding_json) AS name_embedding, $summary AS summary
      FROM dual
    ) s
    ON (t.uuid = s.uuid)
    WHEN MATCHED THEN
      UPDATE SET
        t.name_embedding = s.name_embedding,
        t.summary = s.summary,
        t.updated_at = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
      INSERT (uuid, name_embedding, summary, updated_at)
      VALUES (s.uuid, s.name_embedding, s.summary, SYSTIMESTAMP)
    """
    params = {'uuid': uuid, 'embedding_json': embedding_json, 'summary': summary}
    if tx is not None:
        await tx.run(merge_sql, **params)
    else:
        await executor.execute_query(merge_sql, **params)


async def upsert_community_node_embeddings_bulk(
    executor: QueryExecutor,
    values: list[tuple[str, list[float] | None, str | None]],
    tx: Transaction | None = None,
) -> None:
    await ensure_embedding_table(executor)
    if not values:
        return
    embedding_table = get_embedding_table_name(executor, 'COMMUNITY_NODES')
    payload = [
        {
            'uuid': uuid,
            'embedding_json': json.dumps(embedding) if embedding is not None else None,
            'summary': summary,
        }
        for uuid, embedding, summary in values
    ]
    merge_sql = f"""
    MERGE INTO {embedding_table} t
    USING (
      SELECT jt.uuid, TO_VECTOR(jt.embedding_json) AS name_embedding, jt.summary
      FROM JSON_TABLE(
        $payload_json,
        '$[*]' COLUMNS (
          uuid VARCHAR2(255) PATH '$.uuid',
          embedding_json CLOB PATH '$.embedding_json',
          summary CLOB PATH '$.summary'
        )
      ) jt
    ) s
    ON (t.uuid = s.uuid)
    WHEN MATCHED THEN
      UPDATE SET
        t.name_embedding = s.name_embedding,
        t.summary = s.summary,
        t.updated_at = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
      INSERT (uuid, name_embedding, summary, updated_at)
      VALUES (s.uuid, s.name_embedding, s.summary, SYSTIMESTAMP)
    """
    params = {'payload_json': json.dumps(payload)}
    if tx is not None:
        await tx.run(merge_sql, **params)
    else:
        await executor.execute_query(merge_sql, **params)


async def upsert_entity_edge_embedding(
    executor: QueryExecutor,
    uuid: str,
    embedding: list[float] | None,
    tx: Transaction | None = None,
) -> None:
    await ensure_embedding_table(executor)
    embedding_table = get_embedding_table_name(executor, 'ENTITY_EDGES')
    embedding_json = json.dumps(embedding) if embedding is not None else None
    merge_sql = f"""
    MERGE INTO {embedding_table} t
    USING (
      SELECT $uuid AS uuid, TO_VECTOR($embedding_json) AS fact_embedding
      FROM dual
    ) s
    ON (t.uuid = s.uuid)
    WHEN MATCHED THEN
      UPDATE SET
        t.fact_embedding = s.fact_embedding,
        t.updated_at = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
      INSERT (uuid, fact_embedding, updated_at)
      VALUES (s.uuid, s.fact_embedding, SYSTIMESTAMP)
    """
    params = {'uuid': uuid, 'embedding_json': embedding_json}
    if tx is not None:
        await tx.run(merge_sql, **params)
    else:
        await executor.execute_query(merge_sql, **params)


async def upsert_entity_edge_embeddings_bulk(
    executor: QueryExecutor,
    values: list[tuple[str, list[float] | None]],
    tx: Transaction | None = None,
) -> None:
    await ensure_embedding_table(executor)
    if not values:
        return
    embedding_table = get_embedding_table_name(executor, 'ENTITY_EDGES')
    payload = [
        {
            'uuid': uuid,
            'embedding_json': json.dumps(embedding) if embedding is not None else None,
        }
        for uuid, embedding in values
    ]
    merge_sql = f"""
    MERGE INTO {embedding_table} t
    USING (
      SELECT jt.uuid, TO_VECTOR(jt.embedding_json) AS fact_embedding
      FROM JSON_TABLE(
        $payload_json,
        '$[*]' COLUMNS (
          uuid VARCHAR2(255) PATH '$.uuid',
          embedding_json CLOB PATH '$.embedding_json'
        )
      ) jt
    ) s
    ON (t.uuid = s.uuid)
    WHEN MATCHED THEN
      UPDATE SET
        t.fact_embedding = s.fact_embedding,
        t.updated_at = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
      INSERT (uuid, fact_embedding, updated_at)
      VALUES (s.uuid, s.fact_embedding, SYSTIMESTAMP)
    """
    params = {'payload_json': json.dumps(payload)}
    if tx is not None:
        await tx.run(merge_sql, **params)
    else:
        await executor.execute_query(merge_sql, **params)


def _embedding_table_definitions(executor: QueryExecutor) -> dict[str, list[tuple[str, str]]]:
    entity_nodes_table = get_embedding_table_name(executor, 'ENTITY_NODES')
    community_nodes_table = get_embedding_table_name(executor, 'COMMUNITY_NODES')
    episodic_nodes_table = get_embedding_table_name(executor, 'EPISODIC_NODES')
    saga_nodes_table = get_embedding_table_name(executor, 'SAGA_NODES')
    entity_edges_table = get_embedding_table_name(executor, 'ENTITY_EDGES')
    community_edges_table = get_embedding_table_name(executor, 'COMMUNITY_EDGES')
    vector_type = f'VECTOR({get_embedding_dimension_for_executor(executor)})'

    return {
        entity_nodes_table: [
            ('UUID', 'VARCHAR2(255) PRIMARY KEY'),
            ('SUMMARY', 'CLOB'),
            ('NAME_EMBEDDING', vector_type),
        ],
        community_nodes_table: [
            ('UUID', 'VARCHAR2(255) PRIMARY KEY'),
            ('SUMMARY', 'CLOB'),
            ('NAME_EMBEDDING', vector_type),
        ],
        episodic_nodes_table: [
            ('UUID', 'VARCHAR2(255) PRIMARY KEY'),
            ('CONTENT', 'CLOB'),
            ('CONTENT_EMBEDDING', vector_type),
        ],
        saga_nodes_table: [
            ('UUID', 'VARCHAR2(255) PRIMARY KEY'),
            ('NAME', 'VARCHAR2(2000)'),
            ('SUMMARY', 'CLOB'),
            ('NAME_EMBEDDING', 'VECTOR'),
        ],
        entity_edges_table: [
            ('UUID', 'VARCHAR2(255) PRIMARY KEY'),
            ('FACT_EMBEDDING', vector_type),
        ],
        community_edges_table: [
            ('UUID', 'VARCHAR2(255) PRIMARY KEY'),
            ('FACT_EMBEDDING', vector_type),
        ],
    }


async def ensure_embedding_table(executor: QueryExecutor) -> None:
    initialized_tables = getattr(executor, '_rdf_embedding_tables_initialized', set())
    table_definitions = _embedding_table_definitions(executor)
    table_names = sorted(table_definitions.keys())
    if set(table_names).issubset(initialized_tables):
        return

    network_owner, _, _ = get_rdf_identifiers_for_executor(executor)
    owner = network_owner.strip().upper()
    tablespace = (
        _normalized_identifier(getattr(executor, 'rdf_tablespace', None))
        or _normalized_identifier(getattr(executor, '_rdf_tablespace', None))
        or _normalized_identifier(os.getenv('ORACLE_RDF_TABLESPACE'))
    )
    if tablespace is not None:
        tablespace = tablespace.upper()
    pending_tables = table_names
    in_clause = ', '.join(f"'{_escape_sql_literal(name)}'" for name in pending_tables)

    if owner != '':
        table_exists_query = f"""
        SELECT table_name, tablespace_name
        FROM all_tables
        WHERE owner = $owner
          AND table_name IN ({in_clause})
          {'AND tablespace_name = $tablespace' if tablespace is not None else ''}
        """
        owner_query_params: dict[str, Any] = {'owner': owner}
        if tablespace is not None:
            owner_query_params['tablespace'] = tablespace
        table_exists_records, _, _ = await executor.execute_query(
            table_exists_query, **owner_query_params
        )
    else:
        table_exists_query = f"""
        SELECT table_name, tablespace_name
        FROM user_tables
        WHERE table_name IN ({in_clause})
          {'AND tablespace_name = $tablespace' if tablespace is not None else ''}
        """
        user_query_params: dict[str, Any] = {}
        if tablespace is not None:
            user_query_params['tablespace'] = tablespace
        table_exists_records, _, _ = await executor.execute_query(
            table_exists_query, **user_query_params
        )

    existing_tables = {
        str(record.get('table_name')).upper()
        for record in table_exists_records
        if record.get('table_name') is not None
    }

    for pending_table in pending_tables:
        if pending_table not in existing_tables:
            required_columns = table_definitions[pending_table]
            column_sql = ',\n              '.join(
                f'{column_name} {column_type}' for column_name, column_type in required_columns
            )
            create_sql = f"""
            CREATE TABLE {pending_table} (
              {column_sql},
              UPDATED_AT TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP
            )
            """
            await executor.execute_query(create_sql)
            existing_tables.add(pending_table)

    if owner != '':
        column_query = f"""
        SELECT table_name, column_name
        FROM all_tab_columns
        WHERE owner = $owner
          AND table_name IN ({in_clause})
        """
        column_records, _, _ = await executor.execute_query(column_query, owner=owner)
    else:
        column_query = f"""
        SELECT table_name, column_name
        FROM user_tab_columns
        WHERE table_name IN ({in_clause})
        """
        column_records, _, _ = await executor.execute_query(column_query)

    existing_columns_by_table: dict[str, set[str]] = {}
    for record in column_records:
        record_table = record.get('table_name')
        record_column = record.get('column_name')
        if record_table is None or record_column is None:
            continue
        table_key = str(record_table).upper()
        existing_columns_by_table.setdefault(table_key, set()).add(str(record_column).upper())

    for pending_table, required_columns in table_definitions.items():
        existing_columns = existing_columns_by_table.get(pending_table, set())
        missing_columns = [
            (column_name, column_type)
            for column_name, column_type in required_columns
            if column_name.upper() not in existing_columns
        ]
        for column_name, column_type in missing_columns:
            add_column_sql = f'ALTER TABLE {pending_table} ADD ({column_name} {column_type})'
            await executor.execute_query(add_column_sql)

    initialized_tables.update(pending_tables)
    setattr(executor, '_rdf_embedding_tables_initialized', initialized_tables)  # noqa: B010


async def delete_embedding(
    executor: QueryExecutor,
    table_name: str,
    uuid: str,
    tx: Transaction | None = None,
) -> None:
    await ensure_embedding_table(executor)
    delete_sql = f'DELETE FROM {table_name} WHERE uuid = $uuid'
    params = {'uuid': uuid}
    if tx is not None:
        await tx.run(delete_sql, **params)
    else:
        await executor.execute_query(delete_sql, **params)


async def delete_embeddings_bulk(
    executor: QueryExecutor,
    table_name: str,
    uuids: list[str],
    tx: Transaction | None = None,
) -> None:
    for uuid in uuids:
        await delete_embedding(executor, table_name, uuid, tx=tx)


async def _fetch_embeddings_from_table(
    executor: QueryExecutor,
    table_name: str,
    uuids: list[str],
    embedding_column: str,
) -> dict[str, list[float] | None]:
    if not uuids:
        return {}
    await ensure_embedding_table(executor)
    escaped_uuid_values = ', '.join(f"'{_escape_sql_literal(str(uuid))}'" for uuid in uuids)
    query = f"""
    SELECT
        uuid,
        {embedding_column} AS embedding_value
    FROM {table_name}
    WHERE uuid IN ({escaped_uuid_values})
    """
    records, _, _ = await executor.execute_query(query)

    requested_uuid_set = {str(uuid) for uuid in uuids}
    returned_uuid_set = {
        str(record['uuid']) for record in records if record.get('uuid') is not None
    }
    if len(returned_uuid_set) < len(requested_uuid_set):
        missing_uuids = sorted(requested_uuid_set - returned_uuid_set)
        raise ValueError(
            'Embedding bulk fetch returned fewer rows than requested '
            f'(requested={len(requested_uuid_set)}, returned={len(returned_uuid_set)}). '
            f'Missing UUIDs: {missing_uuids}'
        )

    embedding_map: dict[str, list[float] | None] = {uuid: None for uuid in uuids}
    for record in records:
        uuid = record.get('uuid')
        if uuid is None:
            continue
        embedding_map[str(uuid)] = parse_float_list_literal(record.get('embedding_value'))
    return embedding_map


async def fetch_entity_node_embedding(
    executor: QueryExecutor,
    uuid: str,
) -> list[float] | None:
    return (await fetch_entity_node_embeddings_bulk(executor, [uuid])).get(uuid)


async def fetch_entity_node_embeddings_bulk(
    executor: QueryExecutor,
    uuids: list[str],
) -> dict[str, list[float] | None]:
    return await _fetch_embeddings_from_table(
        executor,
        get_embedding_table_name(executor, 'ENTITY_NODES'),
        uuids,
        'NAME_EMBEDDING',
    )


async def fetch_community_node_embedding(
    executor: QueryExecutor,
    uuid: str,
) -> list[float] | None:
    return (await fetch_community_node_embeddings_bulk(executor, [uuid])).get(uuid)


async def fetch_community_node_embeddings_bulk(
    executor: QueryExecutor,
    uuids: list[str],
) -> dict[str, list[float] | None]:
    return await _fetch_embeddings_from_table(
        executor,
        get_embedding_table_name(executor, 'COMMUNITY_NODES'),
        uuids,
        'NAME_EMBEDDING',
    )


async def fetch_episodic_node_embedding(
    executor: QueryExecutor,
    uuid: str,
) -> list[float] | None:
    return (await fetch_episodic_node_embeddings_bulk(executor, [uuid])).get(uuid)


async def fetch_episodic_node_embeddings_bulk(
    executor: QueryExecutor,
    uuids: list[str],
) -> dict[str, list[float] | None]:
    return await _fetch_embeddings_from_table(
        executor,
        get_embedding_table_name(executor, 'EPISODIC_NODES'),
        uuids,
        'CONTENT_EMBEDDING',
    )


async def fetch_entity_edge_fact_embedding(
    executor: QueryExecutor,
    uuid: str,
) -> list[float] | None:
    return (await fetch_entity_edge_fact_embeddings_bulk(executor, [uuid])).get(uuid)


async def fetch_entity_edge_fact_embeddings_bulk(
    executor: QueryExecutor,
    uuids: list[str],
) -> dict[str, list[float] | None]:
    return await _fetch_embeddings_from_table(
        executor,
        get_embedding_table_name(executor, 'ENTITY_EDGES'),
        uuids,
        'FACT_EMBEDDING',
    )
