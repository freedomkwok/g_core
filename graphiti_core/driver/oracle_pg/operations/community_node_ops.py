"""
Oracle PG implementation for community node operations.
"""

from __future__ import annotations

import json
from typing import Any

from graphiti_core.driver.operations.community_node_ops import CommunityNodeOperations
from graphiti_core.driver.oracle_pg.sql_utils import (
    get_table_name,
    parse_float_list,
    run_query,
    sql_in_list,
    sql_string_literal,
)
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.driver.record_parsers import community_node_from_record
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.nodes import CommunityNode


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['summary'] = normalized.get('summary') or ''
    normalized['name_embedding'] = parse_float_list(normalized.get('name_embedding'))
    return normalized


class OraclePGCommunityNodeOperations(CommunityNodeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        node: CommunityNode,
        tx: Transaction | None = None,
    ) -> None:
        table = get_table_name(executor, 'community_nodes')
        query = f"""
        MERGE INTO {table} t
        USING (
          SELECT
            $uuid AS uuid,
            $group_id AS group_id,
            $name AS name,
            $summary AS summary,
            $created_at AS created_at,
            CASE WHEN $name_embedding_vector IS NULL THEN NULL ELSE TO_VECTOR($name_embedding_vector) END AS name_embedding
          FROM dual
        ) s
        ON (t.uuid = s.uuid)
        WHEN MATCHED THEN UPDATE SET
          t.group_id = s.group_id,
          t.name = s.name,
          t.summary = s.summary,
          t.created_at = s.created_at,
          t.name_embedding = s.name_embedding
        WHEN NOT MATCHED THEN INSERT (uuid, group_id, name, summary, created_at, name_embedding)
        VALUES (s.uuid, s.group_id, s.name, s.summary, s.created_at, s.name_embedding)
        """
        await run_query(
            executor,
            query,
            tx=tx,
            uuid=node.uuid,
            group_id=node.group_id,
            name=node.name,
            summary=node.summary,
            created_at=node.created_at,
            name_embedding_vector=(
                json.dumps(node.name_embedding) if node.name_embedding is not None else None
            ),
        )

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[CommunityNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for node in nodes:
            await self.save(executor, node, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        node: CommunityNode,
        tx: Transaction | None = None,
    ) -> None:
        edge_table = get_table_name(executor, 'community_edges')
        table = get_table_name(executor, 'community_nodes')
        await run_query(
            executor,
            f'DELETE FROM {edge_table} WHERE source_node_uuid = $uuid',
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
        table = get_table_name(executor, 'community_nodes')
        edge_table = get_table_name(executor, 'community_edges')
        await run_query(executor, f'DELETE FROM {edge_table} WHERE group_id = $group_id', tx=tx, group_id=group_id)
        await run_query(executor, f'DELETE FROM {table} WHERE group_id = $group_id', tx=tx, group_id=group_id)

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if not uuids:
            return
        edge_table = get_table_name(executor, 'community_edges')
        table = get_table_name(executor, 'community_nodes')
        await run_query(
            executor,
            f'DELETE FROM {edge_table} WHERE source_node_uuid IN {sql_in_list(uuids)}',
            tx=tx,
        )
        await run_query(executor, f'DELETE FROM {table} WHERE uuid IN {sql_in_list(uuids)}', tx=tx)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> CommunityNode:
        table = get_table_name(executor, 'community_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              summary,
              created_at,
              name_embedding
            FROM {table}
            WHERE uuid = $uuid
            """,
            uuid=uuid,
        )
        if not records:
            raise NodeNotFoundError(uuid)
        return community_node_from_record(_normalize_record(records[0]))

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[CommunityNode]:
        if not uuids:
            return []
        table = get_table_name(executor, 'community_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              summary,
              created_at,
              name_embedding
            FROM {table}
            WHERE uuid IN {sql_in_list(uuids)}
            """,
        )
        return [community_node_from_record(_normalize_record(record)) for record in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[CommunityNode]:
        if not group_ids:
            return []
        table = get_table_name(executor, 'community_nodes')
        cursor_clause = f' AND uuid < {sql_string_literal(uuid_cursor)}' if uuid_cursor else ''
        limit_clause = f' FETCH FIRST {int(limit)} ROWS ONLY' if limit is not None else ''
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              summary,
              created_at,
              name_embedding
            FROM {table}
            WHERE group_id IN {sql_in_list(group_ids)}
            {cursor_clause}
            ORDER BY uuid DESC
            {limit_clause}
            """,
        )
        return [community_node_from_record(_normalize_record(record)) for record in records]

    async def load_name_embedding(
        self,
        executor: QueryExecutor,
        node: CommunityNode,
    ) -> None:
        table = get_table_name(executor, 'community_nodes')
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
