"""
Oracle Property Graph (table-backed SQL) driver.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from graphiti_core.driver.driver import GraphProvider
from graphiti_core.driver.oracle_driver import (
    CloseRunner,
    OracleDriver,
    QueryResult,
    QueryRunner,
    QueryTransform,
)
from graphiti_core.driver.oracle_pg.legacy_adapter import OraclePGLegacyOperationsAdapter
from graphiti_core.driver.oracle_pg.operations import (
    OraclePGCommunityEdgeOperations,
    OraclePGCommunityNodeOperations,
    OraclePGEntityEdgeOperations,
    OraclePGEntityNodeOperations,
    OraclePGEpisodeNodeOperations,
    OraclePGEpisodicEdgeOperations,
    OraclePGGraphMaintenanceOperations,
    OraclePGHasEpisodeEdgeOperations,
    OraclePGNextEpisodeEdgeOperations,
    OraclePGSagaNodeOperations,
    OraclePGSearchOperations,
)
from graphiti_core.driver.oracle_pg.sql_utils import (
    build_table_name,
    get_property_graph_create_block,
    get_table_ddl_blocks,
    sanitize_graph_id,
)

logger = logging.getLogger(__name__)

_ORACLE_PG_PROVIDER = (
    GraphProvider.ORACLE_PG if hasattr(GraphProvider, 'ORACLE_PG') else GraphProvider.ORACLE
)


class OraclePGDriver(OracleDriver):
    provider = _ORACLE_PG_PROVIDER

    def __init__(
        self,
        query_runner: QueryRunner | None = None,
        close_runner: CloseRunner | None = None,
        uri: str | None = None,
        dsn: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str = 'default',
        graph_id: str | None = None,
        rdf_graph_name: str | None = None,
        supports_index_management: bool = True,
        connect_kwargs: dict[str, Any] | None = None,
        query_transform: QueryTransform | None = None,
        log_queries: bool | None = None,
    ):
        if not hasattr(GraphProvider, 'ORACLE_PG'):
            logger.warning(
                'GraphProvider.ORACLE_PG is unavailable in the current runtime; '
                'falling back to GraphProvider.ORACLE. '
                'Restart the Python process/kernel to pick up updated enum definitions.'
            )
        super().__init__(
            query_runner=query_runner,
            close_runner=close_runner,
            uri=uri,
            dsn=dsn,
            user=user,
            password=password,
            database=database,
            supports_index_management=supports_index_management,
            connect_kwargs=connect_kwargs,
            query_transform=query_transform,
            use_rdf=False,
            rdf_graph_name=rdf_graph_name or graph_id,
            log_queries=log_queries,
        )
        # Oracle PG table prefix follows the normalized rdf_graph_name source.
        self._graph_id = sanitize_graph_id(
            self._rdf_graph_name
            or graph_id
            or os.getenv('ORACLE_RDF_GRAPH_NAME')
            or os.getenv('ORACLE_RDF_GRAPH')
            or os.getenv('ORACLE_PG_GRAPH_ID')
            or 'GRAPHITI'
        )
        self._pg_tables_initialized = False
        self._pg_table_lock = asyncio.Lock()

        self._entity_node_ops = OraclePGEntityNodeOperations()
        self._episode_node_ops = OraclePGEpisodeNodeOperations()
        self._community_node_ops = OraclePGCommunityNodeOperations()
        self._saga_node_ops = OraclePGSagaNodeOperations()
        self._entity_edge_ops = OraclePGEntityEdgeOperations()
        self._episodic_edge_ops = OraclePGEpisodicEdgeOperations()
        self._community_edge_ops = OraclePGCommunityEdgeOperations()
        self._has_episode_edge_ops = OraclePGHasEpisodeEdgeOperations()
        self._next_episode_edge_ops = OraclePGNextEpisodeEdgeOperations()
        self._search_ops = OraclePGSearchOperations()
        self._graph_ops = OraclePGGraphMaintenanceOperations()

        # Route existing node/edge classmethod paths to SQL operations.
        self.graph_operations_interface = OraclePGLegacyOperationsAdapter(self)  # pyright: ignore[reportAttributeAccessIssue]

    @property
    def graph_id(self) -> str:
        return self._graph_id

    def table_name(self, base_name: str) -> str:
        return build_table_name(self._graph_id, base_name)

    async def _ensure_pg_tables_on_connection(self, connection: Any) -> None:
        if self._query_runner is not None or self._pg_tables_initialized:
            return
        async with self._pg_table_lock:
            if self._pg_tables_initialized:
                return
            with connection.cursor() as cursor:
                for ddl_block in get_table_ddl_blocks(self._graph_id):
                    await cursor.execute(ddl_block)
                await cursor.execute(get_property_graph_create_block(self._graph_id))
            self._pg_tables_initialized = True

    async def _execute_oracledb_query(self, query: str, params: dict[str, Any]) -> QueryResult:
        pool = await self._ensure_pool()
        oracle_query, oracle_params = self._prepare_oracle_query(query, params)
        self._log_query_if_enabled(oracle_query, oracle_params, 'native')

        try:
            async with pool.acquire() as connection:
                connection.autocommit = True
                await self._ensure_pg_tables_on_connection(connection)
                return await self._run_query_async(connection, oracle_query, oracle_params)
        except Exception as exc:
            logger.error(f'Error executing Oracle PG query: {exc}\n{oracle_query}\n{oracle_params}')
            raise

    async def build_indices_and_constraints(
        self, delete_existing: bool = False, drop_tables: bool = False
    ):
        if not self._supports_index_management:
            return
        graph_ops_any: Any = self.graph_ops
        await graph_ops_any.build_indices_and_constraints(
            self,
            delete_existing=delete_existing,
            drop_tables=drop_tables,
        )
        if self._query_runner is None:
            self._pg_tables_initialized = True

    async def _delete_all_indexes_impl(self) -> None:
        await self.graph_ops.delete_all_indexes(self)

    def clone(self, database: str) -> OraclePGDriver:
        if database == self._database:
            return self
        clone_connect_kwargs = dict(self._connect_kwargs)
        if not self._config_dir_explicit:
            clone_connect_kwargs.pop('config_dir', None)
        clone = OraclePGDriver(
            query_runner=self._query_runner,
            close_runner=self._close_runner,
            uri=self._uri,
            dsn=self._configured_dsn,
            user=self._user,
            password=self._password,
            database=database,
            graph_id=self._graph_id,
            rdf_graph_name=self._rdf_graph_name,
            supports_index_management=self._supports_index_management,
            connect_kwargs=clone_connect_kwargs,
            query_transform=self._query_transform,
            log_queries=self._log_queries,
        )
        if self.pool is not None and self._query_runner is not None:
            clone.pool = self.pool
        return clone
