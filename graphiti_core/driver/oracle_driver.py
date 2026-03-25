"""
Copyright 2024, Zep Software, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
from collections.abc import Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlparse

if TYPE_CHECKING:
    import oracledb
else:
    try:
        import oracledb
    except ImportError:
        oracledb = None  # type: ignore[assignment]

from graphiti_core.driver.driver import GraphDriver, GraphDriverSession, GraphProvider
from graphiti_core.driver.operations.community_edge_ops import CommunityEdgeOperations
from graphiti_core.driver.operations.community_node_ops import CommunityNodeOperations
from graphiti_core.driver.operations.entity_edge_ops import EntityEdgeOperations
from graphiti_core.driver.operations.entity_node_ops import EntityNodeOperations
from graphiti_core.driver.operations.episode_node_ops import EpisodeNodeOperations
from graphiti_core.driver.operations.episodic_edge_ops import EpisodicEdgeOperations
from graphiti_core.driver.operations.graph_ops import GraphMaintenanceOperations
from graphiti_core.driver.operations.has_episode_edge_ops import HasEpisodeEdgeOperations
from graphiti_core.driver.operations.next_episode_edge_ops import NextEpisodeEdgeOperations
from graphiti_core.driver.operations.saga_node_ops import SagaNodeOperations
from graphiti_core.driver.operations.search_ops import SearchOperations
from graphiti_core.driver.oracle.operations.community_edge_ops import OracleCommunityEdgeOperations
from graphiti_core.driver.oracle.operations.community_node_ops import OracleCommunityNodeOperations
from graphiti_core.driver.oracle.operations.entity_edge_ops import OracleEntityEdgeOperations
from graphiti_core.driver.oracle.operations.entity_node_ops import OracleEntityNodeOperations
from graphiti_core.driver.oracle.operations.episode_node_ops import OracleEpisodeNodeOperations
from graphiti_core.driver.oracle.operations.episodic_edge_ops import OracleEpisodicEdgeOperations
from graphiti_core.driver.oracle.operations.graph_ops import OracleGraphMaintenanceOperations
from graphiti_core.driver.oracle.operations.has_episode_edge_ops import (
    OracleHasEpisodeEdgeOperations,
)
from graphiti_core.driver.oracle.operations.next_episode_edge_ops import (
    OracleNextEpisodeEdgeOperations,
)
from graphiti_core.driver.oracle.operations.saga_node_ops import OracleSagaNodeOperations
from graphiti_core.driver.oracle.operations.search_ops import OracleSearchOperations
from graphiti_core.graph_queries import get_fulltext_indices, get_range_indices
from graphiti_core.helpers import semaphore_gather

logger = logging.getLogger(__name__)

QueryResult = tuple[list[dict[str, Any]], Any, Any] | list[dict[str, Any]]
QueryRunner = Callable[[str, dict[str, Any]], Awaitable[QueryResult]]
CloseRunner = Callable[[], Awaitable[None]]
QueryTransform = Callable[[str, dict[str, Any]], tuple[str, dict[str, Any]]]

_DOLLAR_BIND_PATTERN = re.compile(r'\$([A-Za-z_][A-Za-z0-9_]*)')


def _convert_dollar_binds_to_colon(query: str) -> str:
    """Translate `$param` bind placeholders into Oracle-style `:param` placeholders."""
    return _DOLLAR_BIND_PATTERN.sub(r':\1', query)


def _parse_oracle_uri(uri: str) -> tuple[str, str | None, str | None]:
    """
    Parse `oracle://user:pass@host:port/service` into `dsn`, `user`, `password`.

    For non-URL DSN strings, returns the original URI and no credentials.
    """
    parsed = urlparse(uri)
    if parsed.hostname is None:
        return uri, None, None

    dsn = parsed.hostname
    if parsed.port is not None:
        dsn += f':{parsed.port}'
    if parsed.path and parsed.path != '/':
        dsn += parsed.path

    parsed_user = unquote(parsed.username) if parsed.username else None
    parsed_password = unquote(parsed.password) if parsed.password else None
    return dsn, parsed_user, parsed_password


class OracleDriverSession(GraphDriverSession):
    provider = GraphProvider.ORACLE

    def __init__(self, driver: OracleDriver):
        self.driver = driver

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # No explicit session cleanup required by default.
        pass

    async def close(self):
        # Session-level close is a no-op; close the driver instead.
        pass

    async def execute_write(self, func, *args, **kwargs):
        return await func(self, *args, **kwargs)

    async def run(self, query: str, **kwargs: Any) -> Any:
        return await self.driver.execute_query(query, **kwargs)


class OracleDriver(GraphDriver):
    """
    Oracle-backed GraphDriver adapter.

    This driver supports two execution modes:
      1) Injected query runner (`query_runner`) for custom Oracle graph APIs/translators.
      2) Native `oracledb` execution using `uri`/`dsn`, `user`, `password` or env vars.

    Notes
    -----
    - Graphiti emits Cypher-like queries. For native `oracledb` mode, provide
      `query_transform` if your backend needs SQL/PGQL translation.
    - Native mode uses `oracledb.create_pool_async()` and async cursor execution.
    """

    provider = GraphProvider.ORACLE
    default_group_id: str = ''

    def __init__(
        self,
        query_runner: QueryRunner | None = None,
        close_runner: CloseRunner | None = None,
        uri: str | None = None,
        dsn: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str = 'default',
        supports_index_management: bool = False,
        connect_kwargs: dict[str, Any] | None = None,
        query_transform: QueryTransform | None = None,
    ):
        super().__init__()
        self._query_runner = query_runner
        self._close_runner = close_runner
        self._query_transform = query_transform
        self._database = database
        self._supports_index_management = supports_index_management
        self._connect_kwargs = dict(connect_kwargs or {})
        config_dir = os.getenv('ORACLE_CONFIG_DIR')
        if config_dir and 'config_dir' not in self._connect_kwargs:
            self._connect_kwargs['config_dir'] = config_dir

        configured_uri = uri or os.getenv('ORACLE_URI')
        configured_dsn = dsn or os.getenv('ORACLE_DSN')
        configured_user = user or os.getenv('ORACLE_USER')
        configured_password = password or os.getenv('ORACLE_PASSWORD')

        self._uri = configured_uri
        self._configured_dsn = configured_dsn
        self._dsn: str | None = None
        uri_user: str | None = None
        uri_password: str | None = None
        if configured_uri:
            parsed_dsn, uri_user, uri_password = _parse_oracle_uri(configured_uri)
            self._dsn = parsed_dsn

        if configured_dsn:
            self._dsn = configured_dsn

        configured_user = configured_user or uri_user
        configured_password = configured_password or uri_password

        self._user = configured_user
        self._password = configured_password
        self.client: Any = None
        self._connection_lock = asyncio.Lock()

        if self._query_runner is None:
            if self._dsn is None or self._user is None or self._password is None:
                raise ValueError(
                    'OracleDriver requires either a query_runner or Oracle credentials '
                    '(uri/dsn, user, password / ORACLE_URI, ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD).'
                )

            if oracledb is None:
                raise ImportError(
                    'oracledb is required for native OracleDriver mode. '
                    'Install it with: pip install graphiti-core[oracle]'
                )

        self.aoss_client = None

        self._entity_node_ops = OracleEntityNodeOperations()
        self._episode_node_ops = OracleEpisodeNodeOperations()
        self._community_node_ops = OracleCommunityNodeOperations()
        self._saga_node_ops = OracleSagaNodeOperations()
        self._entity_edge_ops = OracleEntityEdgeOperations()
        self._episodic_edge_ops = OracleEpisodicEdgeOperations()
        self._community_edge_ops = OracleCommunityEdgeOperations()
        self._has_episode_edge_ops = OracleHasEpisodeEdgeOperations()
        self._next_episode_edge_ops = OracleNextEpisodeEdgeOperations()
        self._search_ops = OracleSearchOperations()
        self._graph_ops = OracleGraphMaintenanceOperations()

    # --- Operations properties ---

    @property
    def entity_node_ops(self) -> EntityNodeOperations:
        return self._entity_node_ops

    @property
    def episode_node_ops(self) -> EpisodeNodeOperations:
        return self._episode_node_ops

    @property
    def community_node_ops(self) -> CommunityNodeOperations:
        return self._community_node_ops

    @property
    def saga_node_ops(self) -> SagaNodeOperations:
        return self._saga_node_ops

    @property
    def entity_edge_ops(self) -> EntityEdgeOperations:
        return self._entity_edge_ops

    @property
    def episodic_edge_ops(self) -> EpisodicEdgeOperations:
        return self._episodic_edge_ops

    @property
    def community_edge_ops(self) -> CommunityEdgeOperations:
        return self._community_edge_ops

    @property
    def has_episode_edge_ops(self) -> HasEpisodeEdgeOperations:
        return self._has_episode_edge_ops

    @property
    def next_episode_edge_ops(self) -> NextEpisodeEdgeOperations:
        return self._next_episode_edge_ops

    @property
    def search_ops(self) -> SearchOperations:
        return self._search_ops

    @property
    def graph_ops(self) -> GraphMaintenanceOperations:
        return self._graph_ops

    async def execute_query(self, cypher_query_: str, **kwargs: Any):
        params = kwargs.pop('params', None)
        if params is None:
            params = {}
        params.update(kwargs)

        # These are Neo4j-driver execution hints and not query parameters.
        params.pop('database_', None)
        params.pop('routing_', None)

        if self._query_runner is not None:
            try:
                result = await self._query_runner(cypher_query_, params)
            except Exception as exc:
                logger.error(f'Error executing Oracle query: {exc}\n{cypher_query_}\n{params}')
                raise

            if isinstance(result, tuple):
                return result

            return result, None, None

        return await self._execute_oracledb_query(cypher_query_, params)

    async def _ensure_pool(self):
        if self.client is None:
            if self._query_runner is not None:
                raise ValueError('Oracle native client is not available in query_runner mode.')

            if oracledb is None:
                raise ImportError(
                    'oracledb is required for native OracleDriver mode. '
                    'Install it with: pip install graphiti-core[oracle]'
                )

            create_pool_async = getattr(oracledb, 'create_pool_async', None)
            if create_pool_async is None:
                raise AttributeError(
                    'Installed oracledb package does not expose create_pool_async(). '
                    'Use a newer python-oracledb version in Thin mode.'
                )

            async with self._connection_lock:
                if self.client is None:
                    self.client = await create_pool_async(
                        user=self._user,
                        password=self._password,
                        dsn=self._dsn,
                        **self._connect_kwargs,
                    )

        return self.client

    def _prepare_oracle_query(
        self, query: str, params: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        if self._query_transform is not None:
            return self._query_transform(query, params)
        return _convert_dollar_binds_to_colon(query), params

    async def _run_query_async(
        self, connection: Any, query: str, params: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], list[str], None]:
        with connection.cursor() as cursor:
            if params:
                await cursor.execute(query, params)
            else:
                await cursor.execute(query)

            if cursor.description is None:
                return [], [], None

            header = [str(column[0]).lower() for column in cursor.description]
            rows = await cursor.fetchall()
            records = [dict(zip(header, row, strict=False)) for row in rows]
            return records, header, None

    async def _execute_oracledb_query(self, query: str, params: dict[str, Any]):
        pool = await self._ensure_pool()
        oracle_query, oracle_params = self._prepare_oracle_query(query, params)

        try:
            async with pool.acquire() as connection:
                # Keep behavior close to other Graphiti drivers.
                connection.autocommit = True
                return await self._run_query_async(connection, oracle_query, oracle_params)
        except Exception as exc:
            logger.error(f'Error executing Oracle query: {exc}\n{oracle_query}\n{oracle_params}')
            raise

    def session(self, database: str | None = None) -> GraphDriverSession:
        if database is not None and database != self._database:
            return OracleDriverSession(driver=self.clone(database))
        return OracleDriverSession(driver=self)

    async def close(self) -> None:
        if self.client is not None:
            pool = self.client
            self.client = None
            close_result = pool.close()
            if inspect.isawaitable(close_result):
                await close_result
        if self._close_runner is not None:
            await self._close_runner()

    def delete_all_indexes(self) -> Coroutine[Any, Any, None]:
        return self._delete_all_indexes_impl()

    async def _delete_all_indexes_impl(self) -> None:
        # Index deletion is backend-specific and intentionally left to the adapter
        # implementation. This method is a safe no-op by default.
        return

    async def build_indices_and_constraints(self, delete_existing: bool = False):
        if not self._supports_index_management:
            return

        if delete_existing:
            await self.delete_all_indexes()

        index_queries = get_range_indices(self.provider) + get_fulltext_indices(self.provider)
        await semaphore_gather(*[self.execute_query(query) for query in index_queries])

    def clone(self, database: str) -> OracleDriver:
        if database == self._database:
            return self
        return OracleDriver(
            query_runner=self._query_runner,
            close_runner=self._close_runner,
            uri=self._uri,
            dsn=self._configured_dsn,
            user=self._user,
            password=self._password,
            database=database,
            supports_index_management=self._supports_index_management,
            connect_kwargs=dict(self._connect_kwargs),
            query_transform=self._query_transform,
        )
