"""
Oracle PG implementation for saga node operations.
"""

from __future__ import annotations

from graphiti_core.driver.operations.saga_node_ops import SagaNodeOperations
from graphiti_core.driver.oracle_pg.sql_utils import (
    get_table_name,
    run_query,
    sql_in_list,
    sql_string_literal,
)
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.nodes import SagaNode, get_saga_node_from_record


class OraclePGSagaNodeOperations(SagaNodeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        node: SagaNode,
        tx: Transaction | None = None,
    ) -> None:
        table = get_table_name(executor, 'saga_nodes')
        query = f"""
        MERGE INTO {table} t
        USING (
          SELECT
            $uuid AS uuid,
            $group_id AS group_id,
            $name AS name,
            $created_at AS created_at
          FROM dual
        ) s
        ON (t.uuid = s.uuid)
        WHEN MATCHED THEN UPDATE SET
          t.group_id = s.group_id,
          t.name = s.name,
          t.created_at = s.created_at
        WHEN NOT MATCHED THEN INSERT (uuid, group_id, name, created_at)
        VALUES (s.uuid, s.group_id, s.name, s.created_at)
        """
        await run_query(
            executor,
            query,
            tx=tx,
            uuid=node.uuid,
            group_id=node.group_id,
            name=node.name,
            created_at=node.created_at,
        )

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[SagaNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for node in nodes:
            await self.save(executor, node, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        node: SagaNode,
        tx: Transaction | None = None,
    ) -> None:
        has_episode_table = get_table_name(executor, 'has_episode_edges')
        table = get_table_name(executor, 'saga_nodes')
        await run_query(
            executor,
            f'DELETE FROM {has_episode_table} WHERE source_node_uuid = $uuid',
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
        table = get_table_name(executor, 'saga_nodes')
        has_episode_table = get_table_name(executor, 'has_episode_edges')
        await run_query(
            executor,
            f'DELETE FROM {has_episode_table} WHERE group_id = $group_id',
            tx=tx,
            group_id=group_id,
        )
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
        has_episode_table = get_table_name(executor, 'has_episode_edges')
        table = get_table_name(executor, 'saga_nodes')
        await run_query(
            executor,
            f'DELETE FROM {has_episode_table} WHERE source_node_uuid IN {sql_in_list(uuids)}',
            tx=tx,
        )
        await run_query(executor, f'DELETE FROM {table} WHERE uuid IN {sql_in_list(uuids)}', tx=tx)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> SagaNode:
        table = get_table_name(executor, 'saga_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              created_at
            FROM {table}
            WHERE uuid = $uuid
            """,
            uuid=uuid,
        )
        if not records:
            raise NodeNotFoundError(uuid)
        return get_saga_node_from_record(records[0])

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[SagaNode]:
        if not uuids:
            return []
        table = get_table_name(executor, 'saga_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              created_at
            FROM {table}
            WHERE uuid IN {sql_in_list(uuids)}
            """,
        )
        return [get_saga_node_from_record(record) for record in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[SagaNode]:
        if not group_ids:
            return []
        table = get_table_name(executor, 'saga_nodes')
        cursor_clause = f' AND uuid < {sql_string_literal(uuid_cursor)}' if uuid_cursor else ''
        limit_clause = f' FETCH FIRST {int(limit)} ROWS ONLY' if limit is not None else ''
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              created_at
            FROM {table}
            WHERE group_id IN {sql_in_list(group_ids)}
            {cursor_clause}
            ORDER BY uuid DESC
            {limit_clause}
            """,
        )
        return [get_saga_node_from_record(record) for record in records]
