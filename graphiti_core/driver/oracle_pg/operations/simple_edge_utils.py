"""
Shared helpers for simple Oracle PG edge tables.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from graphiti_core.driver.oracle_pg.sql_utils import (
    get_table_name,
    run_query,
    sql_in_list,
    sql_string_literal,
)
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.errors import EdgeNotFoundError


class OraclePGSimpleEdgeStore:
    def __init__(self, table_base: str, parser: Callable[[dict[str, Any]], Any]):
        self.table_base = table_base
        self.parser = parser

    async def save(
        self,
        executor: QueryExecutor,
        edge: Any,
        tx: Transaction | None = None,
    ) -> None:
        table = get_table_name(executor, self.table_base)
        query = f"""
        MERGE INTO {table} t
        USING (
          SELECT
            $uuid AS uuid,
            $group_id AS group_id,
            $source_node_uuid AS source_node_uuid,
            $target_node_uuid AS target_node_uuid,
            $created_at AS created_at
          FROM dual
        ) s
        ON (t.uuid = s.uuid)
        WHEN MATCHED THEN UPDATE SET
          t.group_id = s.group_id,
          t.source_node_uuid = s.source_node_uuid,
          t.target_node_uuid = s.target_node_uuid,
          t.created_at = s.created_at
        WHEN NOT MATCHED THEN INSERT (uuid, group_id, source_node_uuid, target_node_uuid, created_at)
        VALUES (s.uuid, s.group_id, s.source_node_uuid, s.target_node_uuid, s.created_at)
        """
        await run_query(
            executor,
            query,
            tx=tx,
            uuid=edge.uuid,
            group_id=edge.group_id,
            source_node_uuid=edge.source_node_uuid,
            target_node_uuid=edge.target_node_uuid,
            created_at=edge.created_at,
        )

    async def save_bulk(
        self,
        executor: QueryExecutor,
        edges: list[Any],
        tx: Transaction | None = None,
    ) -> None:
        for edge in edges:
            await self.save(executor, edge, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        edge: Any,
        tx: Transaction | None = None,
    ) -> None:
        table = get_table_name(executor, self.table_base)
        await run_query(executor, f'DELETE FROM {table} WHERE uuid = $uuid', tx=tx, uuid=edge.uuid)

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
    ) -> None:
        if not uuids:
            return
        table = get_table_name(executor, self.table_base)
        await run_query(executor, f'DELETE FROM {table} WHERE uuid IN {sql_in_list(uuids)}', tx=tx)

    async def get_by_uuid(self, executor: QueryExecutor, uuid: str) -> Any:
        records = await self.get_by_uuids(executor, [uuid])
        if not records:
            raise EdgeNotFoundError(uuid)
        return records[0]

    async def get_by_uuids(self, executor: QueryExecutor, uuids: list[str]) -> list[Any]:
        if not uuids:
            return []
        table = get_table_name(executor, self.table_base)
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              source_node_uuid,
              target_node_uuid,
              created_at
            FROM {table}
            WHERE uuid IN {sql_in_list(uuids)}
            """,
        )
        return [self.parser(record) for record in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[Any]:
        if not group_ids:
            return []
        table = get_table_name(executor, self.table_base)
        cursor_clause = f' AND uuid < {sql_string_literal(uuid_cursor)}' if uuid_cursor else ''
        limit_clause = f' FETCH FIRST {int(limit)} ROWS ONLY' if limit is not None else ''
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              source_node_uuid,
              target_node_uuid,
              created_at
            FROM {table}
            WHERE group_id IN {sql_in_list(group_ids)}
            {cursor_clause}
            ORDER BY uuid DESC
            {limit_clause}
            """,
        )
        return [self.parser(record) for record in records]
