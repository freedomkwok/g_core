"""
Oracle PG implementation for episodic edge operations.
"""

from __future__ import annotations

from graphiti_core.driver.operations.episodic_edge_ops import EpisodicEdgeOperations
from graphiti_core.driver.oracle_pg.operations.simple_edge_utils import OraclePGSimpleEdgeStore
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.edges import EpisodicEdge, get_episodic_edge_from_record


class OraclePGEpisodicEdgeOperations(EpisodicEdgeOperations):
    def __init__(self) -> None:
        self._store = OraclePGSimpleEdgeStore('episodic_edges', get_episodic_edge_from_record)

    async def save(
        self,
        executor: QueryExecutor,
        edge: EpisodicEdge,
        tx: Transaction | None = None,
    ) -> None:
        await self._store.save(executor, edge, tx=tx)

    async def save_bulk(
        self,
        executor: QueryExecutor,
        edges: list[EpisodicEdge],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        await self._store.save_bulk(executor, edges, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        edge: EpisodicEdge,
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
    ) -> EpisodicEdge:
        return await self._store.get_by_uuid(executor, uuid)

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EpisodicEdge]:
        return await self._store.get_by_uuids(executor, uuids)

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EpisodicEdge]:
        return await self._store.get_by_group_ids(executor, group_ids, limit, uuid_cursor)
