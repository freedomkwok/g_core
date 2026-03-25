"""
Oracle PG implementation for has-episode edge operations.
"""

from __future__ import annotations

from graphiti_core.driver.operations.has_episode_edge_ops import HasEpisodeEdgeOperations
from graphiti_core.driver.oracle_pg.operations.simple_edge_utils import OraclePGSimpleEdgeStore
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.edges import HasEpisodeEdge, get_has_episode_edge_from_record


class OraclePGHasEpisodeEdgeOperations(HasEpisodeEdgeOperations):
    def __init__(self) -> None:
        self._store = OraclePGSimpleEdgeStore('has_episode_edges', get_has_episode_edge_from_record)

    async def save(
        self,
        executor: QueryExecutor,
        edge: HasEpisodeEdge,
        tx: Transaction | None = None,
    ) -> None:
        await self._store.save(executor, edge, tx=tx)

    async def save_bulk(
        self,
        executor: QueryExecutor,
        edges: list[HasEpisodeEdge],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        await self._store.save_bulk(executor, edges, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        edge: HasEpisodeEdge,
        tx: Transaction | None = None,
    ) -> None:
        await self._store.delete(executor, edge, tx=tx)

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
    ) -> None:
        await self._store.delete_by_uuids(executor, uuids, tx=tx)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> HasEpisodeEdge:
        return await self._store.get_by_uuid(executor, uuid)

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[HasEpisodeEdge]:
        return await self._store.get_by_uuids(executor, uuids)

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[HasEpisodeEdge]:
        return await self._store.get_by_group_ids(executor, group_ids, limit, uuid_cursor)
