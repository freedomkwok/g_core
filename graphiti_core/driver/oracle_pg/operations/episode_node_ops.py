"""
Oracle PG implementation for episodic node operations.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from graphiti_core.driver.operations.episode_node_ops import EpisodeNodeOperations
from graphiti_core.driver.oracle_pg.sql_utils import (
    get_table_name,
    parse_json_list,
    run_query,
    sql_in_list,
    sql_string_literal,
    to_json_text,
)
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.driver.record_parsers import episodic_node_from_record
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.nodes import EpisodeType, EpisodicNode


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['entity_edges'] = [str(value) for value in parse_json_list(normalized.get('entity_edges'))]
    return normalized


class OraclePGEpisodeNodeOperations(EpisodeNodeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        node: EpisodicNode,
        tx: Transaction | None = None,
    ) -> None:
        table = get_table_name(executor, 'episodic_nodes')
        query = f"""
        MERGE INTO {table} t
        USING (
          SELECT
            $uuid AS uuid,
            $group_id AS group_id,
            $name AS name,
            $source AS source,
            $source_description AS source_description,
            $content AS content,
            $entity_edges AS entity_edges,
            $created_at AS created_at,
            $valid_at AS valid_at
          FROM dual
        ) s
        ON (t.uuid = s.uuid)
        WHEN MATCHED THEN UPDATE SET
          t.group_id = s.group_id,
          t.name = s.name,
          t.source = s.source,
          t.source_description = s.source_description,
          t.content = s.content,
          t.entity_edges = s.entity_edges,
          t.created_at = s.created_at,
          t.valid_at = s.valid_at
        WHEN NOT MATCHED THEN INSERT (
          uuid, group_id, name, source, source_description, content, entity_edges, created_at, valid_at
        )
        VALUES (
          s.uuid, s.group_id, s.name, s.source, s.source_description, s.content, s.entity_edges, s.created_at, s.valid_at
        )
        """
        await run_query(
            executor,
            query,
            tx=tx,
            uuid=node.uuid,
            group_id=node.group_id,
            name=node.name,
            source=node.source.value,
            source_description=node.source_description,
            content=node.content,
            entity_edges=to_json_text(node.entity_edges, default=[]),
            created_at=node.created_at,
            valid_at=node.valid_at,
        )

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[EpisodicNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for node in nodes:
            await self.save(executor, node, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        node: EpisodicNode,
        tx: Transaction | None = None,
    ) -> None:
        mentions_table = get_table_name(executor, 'episodic_edges')
        next_table = get_table_name(executor, 'next_episode_edges')
        has_episode_table = get_table_name(executor, 'has_episode_edges')
        table = get_table_name(executor, 'episodic_nodes')
        await run_query(
            executor,
            f'DELETE FROM {mentions_table} WHERE source_node_uuid = $uuid',
            tx=tx,
            uuid=node.uuid,
        )
        await run_query(
            executor,
            (
                f'DELETE FROM {next_table} '
                'WHERE source_node_uuid = $uuid OR target_node_uuid = $uuid'
            ),
            tx=tx,
            uuid=node.uuid,
        )
        await run_query(
            executor,
            f'DELETE FROM {has_episode_table} WHERE target_node_uuid = $uuid',
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
        table = get_table_name(executor, 'episodic_nodes')
        records = await run_query(
            executor,
            f'SELECT uuid FROM {table} WHERE group_id = $group_id',
            tx=tx,
            group_id=group_id,
        )
        for record in records:
            await self.delete(
                executor,
                EpisodicNode(
                    uuid=record['uuid'],
                    group_id=group_id,
                    name='',
                    source=EpisodeType.message,
                    source_description='',
                    content='',
                    created_at=datetime.utcnow(),
                    valid_at=datetime.utcnow(),
                ),
                tx=tx,
            )

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if not uuids:
            return
        table = get_table_name(executor, 'episodic_nodes')
        records = await run_query(
            executor,
            f'SELECT uuid, group_id FROM {table} WHERE uuid IN {sql_in_list(uuids)}',
            tx=tx,
        )
        for record in records:
            await self.delete(
                executor,
                EpisodicNode(
                    uuid=record['uuid'],
                    group_id=record.get('group_id') or '',
                    name='',
                    source=EpisodeType.message,
                    source_description='',
                    content='',
                    created_at=datetime.utcnow(),
                    valid_at=datetime.utcnow(),
                ),
                tx=tx,
            )

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EpisodicNode:
        table = get_table_name(executor, 'episodic_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              source,
              source_description,
              content,
              entity_edges,
              created_at,
              valid_at
            FROM {table}
            WHERE uuid = $uuid
            """,
            uuid=uuid,
        )
        if not records:
            raise NodeNotFoundError(uuid)
        return episodic_node_from_record(_normalize_record(records[0]))

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EpisodicNode]:
        if not uuids:
            return []
        table = get_table_name(executor, 'episodic_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              source,
              source_description,
              content,
              entity_edges,
              created_at,
              valid_at
            FROM {table}
            WHERE uuid IN {sql_in_list(uuids)}
            """,
        )
        return [episodic_node_from_record(_normalize_record(record)) for record in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EpisodicNode]:
        if not group_ids:
            return []
        table = get_table_name(executor, 'episodic_nodes')
        cursor_clause = f' AND uuid < {sql_string_literal(uuid_cursor)}' if uuid_cursor else ''
        limit_clause = f' FETCH FIRST {int(limit)} ROWS ONLY' if limit is not None else ''
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              source,
              source_description,
              content,
              entity_edges,
              created_at,
              valid_at
            FROM {table}
            WHERE group_id IN {sql_in_list(group_ids)}
            {cursor_clause}
            ORDER BY uuid DESC
            {limit_clause}
            """,
        )
        return [episodic_node_from_record(_normalize_record(record)) for record in records]

    async def get_by_entity_node_uuid(
        self,
        executor: QueryExecutor,
        entity_node_uuid: str,
    ) -> list[EpisodicNode]:
        node_table = get_table_name(executor, 'episodic_nodes')
        edge_table = get_table_name(executor, 'episodic_edges')
        records = await run_query(
            executor,
            f"""
            SELECT
              n.uuid,
              n.group_id,
              n.name,
              n.source,
              n.source_description,
              n.content,
              n.entity_edges,
              n.created_at,
              n.valid_at
            FROM {node_table} n
            JOIN {edge_table} e ON e.source_node_uuid = n.uuid
            WHERE e.target_node_uuid = $entity_node_uuid
            ORDER BY n.valid_at DESC
            """,
            entity_node_uuid=entity_node_uuid,
        )
        return [episodic_node_from_record(_normalize_record(record)) for record in records]

    async def retrieve_episodes(
        self,
        executor: QueryExecutor,
        reference_time: datetime,
        last_n: int = 3,
        group_ids: list[str] | None = None,
        source: str | None = None,
        saga: str | None = None,
    ) -> list[EpisodicNode]:
        node_table = get_table_name(executor, 'episodic_nodes')
        has_episode_table = get_table_name(executor, 'has_episode_edges')
        saga_table = get_table_name(executor, 'saga_nodes')
        conditions = ['n.valid_at <= $reference_time']
        if group_ids:
            conditions.append(f'n.group_id IN {sql_in_list(group_ids)}')
        if source:
            conditions.append(f'n.source = {sql_string_literal(source)}')
        if saga:
            conditions.append(f's.name = {sql_string_literal(saga)}')
        saga_join = (
            f'LEFT JOIN {has_episode_table} h ON h.target_node_uuid = n.uuid '
            f'LEFT JOIN {saga_table} s ON s.uuid = h.source_node_uuid'
        )
        where_clause = ' AND '.join(conditions)
        records = await run_query(
            executor,
            f"""
            SELECT
              n.uuid,
              n.group_id,
              n.name,
              n.source,
              n.source_description,
              n.content,
              n.entity_edges,
              n.created_at,
              n.valid_at
            FROM {node_table} n
            {saga_join}
            WHERE {where_clause}
            ORDER BY n.valid_at DESC
            FETCH FIRST {int(last_n)} ROWS ONLY
            """,
            reference_time=reference_time,
        )
        return [episodic_node_from_record(_normalize_record(record)) for record in records]
