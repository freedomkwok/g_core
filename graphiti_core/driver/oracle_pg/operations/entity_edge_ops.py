"""
Oracle PG implementation for entity edge operations.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from graphiti_core.driver.operations.entity_edge_ops import EntityEdgeOperations
from graphiti_core.driver.oracle_pg.sql_utils import (
    get_table_name,
    parse_float_list,
    parse_json_dict,
    parse_json_list,
    run_query,
    sql_in_list,
    sql_string_literal,
    to_json_text,
)
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.driver.record_parsers import entity_edge_from_record
from graphiti_core.edges import EntityEdge
from graphiti_core.errors import EdgeNotFoundError

logger = logging.getLogger(__name__)


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['episodes'] = [str(value) for value in parse_json_list(normalized.get('episodes'))]
    normalized['attributes'] = parse_json_dict(normalized.get('attributes'))
    normalized['name'] = normalized.get('name') or ''
    normalized['fact'] = normalized.get('fact') or ''
    normalized['fact_embedding'] = parse_float_list(normalized.get('fact_embedding'))
    return normalized


class OraclePGEntityEdgeOperations(EntityEdgeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        edge: EntityEdge,
        tx: Transaction | None = None,
    ) -> None:
        table = get_table_name(executor, 'entity_edges')
        query = f"""
        MERGE INTO {table} t
        USING (
          SELECT
            $uuid AS uuid,
            $group_id AS group_id,
            $source_node_uuid AS src_uuid,
            $target_node_uuid AS dst_uuid,
            $edge_type AS edge_type,
            $name AS name,
            $fact_text AS fact_text,
            $episodes AS episodes,
            $attributes AS attributes,
            $created_at AS created_at,
            $valid_at AS valid_at,
            $invalid_at AS invalid_at,
            $expired_at AS expired_at,
            CASE WHEN $fact_embedding_vector IS NULL THEN NULL ELSE TO_VECTOR($fact_embedding_vector) END AS fact_embedding
          FROM dual
        ) s
        ON (t.uuid = s.uuid)
        WHEN MATCHED THEN UPDATE SET
          t.group_id = s.group_id,
          t.src_uuid = s.src_uuid,
          t.dst_uuid = s.dst_uuid,
          t.edge_type = s.edge_type,
          t.name = s.name,
          t.fact_text = s.fact_text,
          t.episodes = s.episodes,
          t.attributes = s.attributes,
          t.created_at = s.created_at,
          t.valid_at = s.valid_at,
          t.invalid_at = s.invalid_at,
          t.expired_at = s.expired_at,
          t.fact_embedding = s.fact_embedding
        WHEN NOT MATCHED THEN INSERT (
          uuid, group_id, src_uuid, dst_uuid, edge_type, name, fact_text, episodes, attributes,
          created_at, valid_at, invalid_at, expired_at, fact_embedding
        )
        VALUES (
          s.uuid, s.group_id, s.src_uuid, s.dst_uuid, s.edge_type, s.name, s.fact_text, s.episodes,
          s.attributes, s.created_at, s.valid_at, s.invalid_at, s.expired_at, s.fact_embedding
        )
        """
        await run_query(
            executor,
            query,
            tx=tx,
            uuid=edge.uuid,
            group_id=edge.group_id,
            source_node_uuid=edge.source_node_uuid,
            target_node_uuid=edge.target_node_uuid,
            edge_type='RELATES_TO',
            name=edge.name,
            fact_text=edge.fact,
            episodes=to_json_text(edge.episodes, default=[]),
            attributes=to_json_text(edge.attributes, default={}),
            created_at=edge.created_at,
            valid_at=edge.valid_at,
            invalid_at=edge.invalid_at,
            expired_at=edge.expired_at,
            fact_embedding_vector=(
                json.dumps(edge.fact_embedding) if edge.fact_embedding is not None else None
            ),
        )

    async def save_bulk(
        self,
        executor: QueryExecutor,
        edges: list[EntityEdge],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for edge in edges:
            await self.save(executor, edge, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        edge: EntityEdge,
        tx: Transaction | None = None,
    ) -> None:
        table = get_table_name(executor, 'entity_edges')
        await run_query(executor, f'DELETE FROM {table} WHERE uuid = $uuid', tx=tx, uuid=edge.uuid)

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
    ) -> None:
        if not uuids:
            return
        table = get_table_name(executor, 'entity_edges')
        await run_query(executor, f'DELETE FROM {table} WHERE uuid IN {sql_in_list(uuids)}', tx=tx)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EntityEdge:
        table = get_table_name(executor, 'entity_edges')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              src_uuid AS source_node_uuid,
              dst_uuid AS target_node_uuid,
              group_id,
              created_at,
              name,
              fact_text AS fact,
              episodes,
              valid_at,
              invalid_at,
              expired_at,
              attributes
            FROM {table}
            WHERE uuid = $uuid
            """,
            uuid=uuid,
        )
        if not records:
            raise EdgeNotFoundError(uuid)
        return entity_edge_from_record(_normalize_record(records[0]))

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EntityEdge]:
        if not uuids:
            return []
        table = get_table_name(executor, 'entity_edges')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              src_uuid AS source_node_uuid,
              dst_uuid AS target_node_uuid,
              group_id,
              created_at,
              name,
              fact_text AS fact,
              episodes,
              valid_at,
              invalid_at,
              expired_at,
              attributes
            FROM {table}
            WHERE uuid IN {sql_in_list(uuids)}
            """,
        )
        return [entity_edge_from_record(_normalize_record(record)) for record in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EntityEdge]:
        if not group_ids:
            return []
        table = get_table_name(executor, 'entity_edges')
        cursor_clause = f' AND uuid < {sql_string_literal(uuid_cursor)}' if uuid_cursor else ''
        limit_clause = f' FETCH FIRST {int(limit)} ROWS ONLY' if limit is not None else ''
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              src_uuid AS source_node_uuid,
              dst_uuid AS target_node_uuid,
              group_id,
              created_at,
              name,
              fact_text AS fact,
              episodes,
              valid_at,
              invalid_at,
              expired_at,
              attributes
            FROM {table}
            WHERE group_id IN {sql_in_list(group_ids)}
            {cursor_clause}
            ORDER BY uuid DESC
            {limit_clause}
            """,
        )
        return [entity_edge_from_record(_normalize_record(record)) for record in records]

    async def get_between_nodes(
        self,
        executor: QueryExecutor,
        source_node_uuid: str,
        target_node_uuid: str,
    ) -> list[EntityEdge]:
        table = get_table_name(executor, 'entity_edges')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              src_uuid AS source_node_uuid,
              dst_uuid AS target_node_uuid,
              group_id,
              created_at,
              name,
              fact_text AS fact,
              episodes,
              valid_at,
              invalid_at,
              expired_at,
              attributes
            FROM {table}
            WHERE src_uuid = $source_node_uuid AND dst_uuid = $target_node_uuid
            """,
            source_node_uuid=source_node_uuid,
            target_node_uuid=target_node_uuid,
        )
        return [entity_edge_from_record(_normalize_record(record)) for record in records]

    async def get_by_node_uuid(
        self,
        executor: QueryExecutor,
        node_uuid: str,
    ) -> list[EntityEdge]:
        table = get_table_name(executor, 'entity_edges')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              src_uuid AS source_node_uuid,
              dst_uuid AS target_node_uuid,
              group_id,
              created_at,
              name,
              fact_text AS fact,
              episodes,
              valid_at,
              invalid_at,
              expired_at,
              attributes
            FROM {table}
            WHERE src_uuid = $node_uuid OR dst_uuid = $node_uuid
            """,
            node_uuid=node_uuid,
        )
        return [entity_edge_from_record(_normalize_record(record)) for record in records]

    async def load_embeddings(
        self,
        executor: QueryExecutor,
        edge: EntityEdge,
    ) -> None:
        table = get_table_name(executor, 'entity_edges')
        records = await run_query(
            executor,
            f"""
            SELECT fact_embedding
            FROM {table}
            WHERE uuid = $uuid
            """,
            uuid=edge.uuid,
        )
        if not records:
            raise EdgeNotFoundError(edge.uuid)
        edge.fact_embedding = parse_float_list(records[0].get('fact_embedding'))

    async def load_embeddings_bulk(
        self,
        executor: QueryExecutor,
        edges: list[EntityEdge],
        batch_size: int = 100,
    ) -> None:
        if not edges:
            return
        uuids = [edge.uuid for edge in edges]
        table = get_table_name(executor, 'entity_edges')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              fact_embedding
            FROM {table}
            WHERE uuid IN {sql_in_list(uuids)}
            """,
        )
        embedding_map = {record['uuid']: parse_float_list(record.get('fact_embedding')) for record in records}
        for edge in edges:
            if edge.uuid in embedding_map:
                edge.fact_embedding = embedding_map[edge.uuid]
