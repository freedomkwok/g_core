"""
Oracle PG implementation for entity node operations.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from graphiti_core.driver.operations.entity_node_ops import EntityNodeOperations
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
from graphiti_core.driver.record_parsers import entity_node_from_record
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.nodes import EntityNode

logger = logging.getLogger(__name__)


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['summary'] = normalized.get('summary') or ''
    normalized['labels'] = [str(value) for value in parse_json_list(normalized.get('labels'))]
    normalized['attributes'] = parse_json_dict(normalized.get('attributes'))
    normalized['name_embedding'] = parse_float_list(normalized.get('name_embedding'))
    return normalized


class OraclePGEntityNodeOperations(EntityNodeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        node: EntityNode,
        tx: Transaction | None = None,
    ) -> None:
        table = get_table_name(executor, 'entity_nodes')
        query = f"""
        MERGE INTO {table} t
        USING (
          SELECT
            $uuid AS uuid,
            $group_id AS group_id,
            $name AS name,
            $summary AS summary,
            $labels AS labels,
            $attributes AS attributes,
            $created_at AS created_at,
            CASE WHEN $name_embedding_vector IS NULL THEN NULL ELSE TO_VECTOR($name_embedding_vector) END AS name_embedding
          FROM dual
        ) s
        ON (t.uuid = s.uuid)
        WHEN MATCHED THEN UPDATE SET
          t.group_id = s.group_id,
          t.name = s.name,
          t.summary = s.summary,
          t.labels = s.labels,
          t.attributes = s.attributes,
          t.created_at = s.created_at,
          t.name_embedding = s.name_embedding
        WHEN NOT MATCHED THEN INSERT (
          uuid, group_id, name, summary, labels, attributes, created_at, name_embedding
        )
        VALUES (
          s.uuid, s.group_id, s.name, s.summary, s.labels, s.attributes, s.created_at, s.name_embedding
        )
        """
        await run_query(
            executor,
            query,
            tx=tx,
            uuid=node.uuid,
            group_id=node.group_id,
            name=node.name,
            summary=node.summary,
            labels=to_json_text(list(set(node.labels + ['Entity'])), default=[]),
            attributes=to_json_text(node.attributes, default={}),
            created_at=node.created_at,
            name_embedding_vector=(
                json.dumps(node.name_embedding) if node.name_embedding is not None else None
            ),
        )

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[EntityNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for node in nodes:
            await self.save(executor, node, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        node: EntityNode,
        tx: Transaction | None = None,
    ) -> None:
        entity_edges = get_table_name(executor, 'entity_edges')
        episodic_edges = get_table_name(executor, 'episodic_edges')
        community_edges = get_table_name(executor, 'community_edges')
        table = get_table_name(executor, 'entity_nodes')
        await run_query(
            executor,
            f'DELETE FROM {entity_edges} WHERE src_uuid = $uuid OR dst_uuid = $uuid',
            tx=tx,
            uuid=node.uuid,
        )
        await run_query(
            executor,
            f'DELETE FROM {episodic_edges} WHERE target_node_uuid = $uuid',
            tx=tx,
            uuid=node.uuid,
        )
        await run_query(
            executor,
            f'DELETE FROM {community_edges} WHERE target_node_uuid = $uuid',
            tx=tx,
            uuid=node.uuid,
        )
        await run_query(executor, f'DELETE FROM {table} WHERE uuid = $uuid', tx=tx, uuid=node.uuid)

    async def delete_by_group_id(
        self,
        executor: QueryExecutor,
        group_id: str,
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        table = get_table_name(executor, 'entity_nodes')
        records = await run_query(
            executor,
            f'SELECT uuid FROM {table} WHERE group_id = $group_id',
            tx=tx,
            group_id=group_id,
        )
        for record in records:
            await self.delete(executor, EntityNode(uuid=record['uuid'], name='', group_id=group_id), tx=tx)

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if not uuids:
            return
        table = get_table_name(executor, 'entity_nodes')
        records = await run_query(
            executor,
            f'SELECT uuid, group_id FROM {table} WHERE uuid IN {sql_in_list(uuids)}',
            tx=tx,
        )
        for record in records:
            await self.delete(
                executor,
                EntityNode(uuid=record['uuid'], name='', group_id=record.get('group_id') or ''),
                tx=tx,
            )

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EntityNode:
        table = get_table_name(executor, 'entity_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              name,
              group_id,
              created_at,
              summary,
              labels,
              attributes
            FROM {table}
            WHERE uuid = $uuid
            """,
            uuid=uuid,
        )
        if not records:
            raise NodeNotFoundError(uuid)
        return entity_node_from_record(_normalize_record(records[0]))

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EntityNode]:
        if not uuids:
            return []
        table = get_table_name(executor, 'entity_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              name,
              group_id,
              created_at,
              summary,
              labels,
              attributes
            FROM {table}
            WHERE uuid IN {sql_in_list(uuids)}
            """,
        )
        return [entity_node_from_record(_normalize_record(record)) for record in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EntityNode]:
        if not group_ids:
            return []
        table = get_table_name(executor, 'entity_nodes')
        cursor_clause = f' AND uuid < {sql_string_literal(uuid_cursor)}' if uuid_cursor else ''
        limit_clause = f' FETCH FIRST {int(limit)} ROWS ONLY' if limit is not None else ''
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              name,
              group_id,
              created_at,
              summary,
              labels,
              attributes
            FROM {table}
            WHERE group_id IN {sql_in_list(group_ids)}
            {cursor_clause}
            ORDER BY uuid DESC
            {limit_clause}
            """,
        )
        return [entity_node_from_record(_normalize_record(record)) for record in records]

    async def load_embeddings(
        self,
        executor: QueryExecutor,
        node: EntityNode,
    ) -> None:
        table = get_table_name(executor, 'entity_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT name_embedding
            FROM {table}
            WHERE uuid = $uuid
            """,
            uuid=node.uuid,
        )
        if not records:
            raise NodeNotFoundError(node.uuid)
        node.name_embedding = parse_float_list(records[0].get('name_embedding'))

    async def load_embeddings_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[EntityNode],
        batch_size: int = 100,
    ) -> None:
        if not nodes:
            return
        uuids = [node.uuid for node in nodes]
        table = get_table_name(executor, 'entity_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              name_embedding
            FROM {table}
            WHERE uuid IN {sql_in_list(uuids)}
            """,
        )
        embedding_map = {record['uuid']: parse_float_list(record.get('name_embedding')) for record in records}
        for node in nodes:
            if node.uuid in embedding_map:
                node.name_embedding = embedding_map[node.uuid]
