"""
Oracle Property Graph (table-backed SQL) driver.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, Coroutine
from time import perf_counter
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
from graphiti_core.driver.oracle_pg.graph_queries import get_fulltext_indices
from graphiti_core.driver.oracle_pg.vector_index_params import OraclePGVectorIndexParams
from graphiti_core.driver.oracle_pg.graph_operations_adapter import OraclePGGraphOperationsAdapter
from graphiti_core.driver.oracle_pg.sql_utils import (
    build_table_name,
    get_property_graph_create_block,
    get_table_ddl_blocks,
    sanitize_graph_id,
)
from graphiti_core.helpers import normalized_semaphore_limit
from graphiti_core.driver.search_interface.ops_backed_search_interface import (
    OpsBackedSearchInterface,
)

logger = logging.getLogger(__name__)

_ORACLE_PG_PROVIDER = (
    GraphProvider.ORACLE_PG if hasattr(GraphProvider, 'ORACLE_PG') else GraphProvider.ORACLE
)
_DOLLAR_BIND_PATTERN = re.compile(r'\$([A-Za-z_][A-Za-z0-9_]*)')

QueryResult = tuple[list[dict[str, Any]], Any, Any] | list[dict[str, Any]]
QueryRunner = Callable[[str, dict[str, Any]], Awaitable[QueryResult]]
CloseRunner = Callable[[], Awaitable[None]]
QueryTransform = Callable[[str, dict[str, Any]], tuple[str, dict[str, Any]]]


def _env_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {'1', 'true', 'yes', 'on'}


def _convert_dollar_binds_to_colon(query: str) -> str:
    return _DOLLAR_BIND_PATTERN.sub(r':\1', query)


def _parse_oracle_uri(uri: str) -> tuple[str, str | None, str | None]:
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


def _resolve_connect_kwargs(
    connect_kwargs: dict[str, Any],
    *,
    resolved_connection_input: bool,
    config_dir_explicit: bool,
) -> dict[str, Any]:
    resolved_connect_kwargs = dict(connect_kwargs)
    config_dir = os.getenv('ORACLE_CONFIG_DIR')
    has_config_dir = 'config_dir' in resolved_connect_kwargs

    if has_config_dir:
        if config_dir_explicit:
            logger.debug('OraclePGDriver using explicit connect_kwargs config_dir')
        else:
            logger.debug('OraclePGDriver has inherited config_dir in connect_kwargs')

    if not has_config_dir and config_dir:
        if not resolved_connection_input:
            resolved_connect_kwargs['config_dir'] = config_dir
            logger.debug('OraclePGDriver auto-applied ORACLE_CONFIG_DIR to connect_kwargs')
        else:
            logger.debug(
                'OraclePGDriver skipped ORACLE_CONFIG_DIR because DSN/URI was provided '
                '(argument or environment)'
            )

    if resolved_connection_input and 'config_dir' in resolved_connect_kwargs and not config_dir_explicit:
        resolved_connect_kwargs.pop('config_dir', None)
        logger.debug('OraclePGDriver removed non-explicit config_dir because DSN/URI was provided')

    return resolved_connect_kwargs


def _normalize_execute_query_params(
    params: dict[str, Any] | None, kwargs: dict[str, Any]
) -> dict[str, Any]:
    normalized_params = dict(params or {})
    normalized_params.update(kwargs)
    normalized_params.pop('database_', None)
    normalized_params.pop('routing_', None)
    return normalized_params


def _is_float_list(value: Any) -> bool:
    return isinstance(value, list) and len(value) > 0 and all(
        isinstance(item, float) for item in value
    )


def _is_numeric_list(value: Any) -> bool:
    return isinstance(value, list) and len(value) > 0 and all(
        isinstance(item, int | float) for item in value
    )


def _redact_vector_json_string(value: str) -> str | None:
    stripped = value.strip()
    if not (stripped.startswith('[') and stripped.endswith(']')):
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    if _is_numeric_list(parsed):
        return f'<redacted float_list len={len(parsed)}>'
    return None


def _sanitize_params_for_logging(value: Any) -> Any:
    """Recursively redact embedding-like parameters from logs."""
    if _is_float_list(value):
        return f'<redacted float_list len={len(value)}>'
    if isinstance(value, str):
        redacted = _redact_vector_json_string(value)
        if redacted is not None:
            return redacted
    if isinstance(value, dict):
        return {key: _sanitize_params_for_logging(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_params_for_logging(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_params_for_logging(item) for item in value)
    return value


def _pool_stats_for_logging(pool: Any) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for key in ('min', 'max', 'increment', 'opened', 'busy'):
        value = getattr(pool, key, None)
        if value is not None:
            stats[key] = value
    return stats


class OraclePGDriverSession(GraphDriverSession):
    provider = _ORACLE_PG_PROVIDER

    def __init__(self, driver: OraclePGDriver):
        self.driver = driver

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def close(self):
        pass

    async def execute_write(self, func, *args, **kwargs):
        return await func(self, *args, **kwargs)

    async def run(self, query: str, **kwargs: Any) -> Any:
        return await self.driver.execute_query(query, **kwargs)


class OraclePGDriver(GraphDriver):
    provider = _ORACLE_PG_PROVIDER
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
        graph_id: str | None = None,
        supports_index_management: bool = True,
        connect_kwargs: dict[str, Any] | None = None,
        query_transform: QueryTransform | None = None,
        fetch_job: bool = False,
        log_queries: bool | None = None,
        max_coroutines: int | None = None,
        vector_index_params: OraclePGVectorIndexParams | None = None,
    ):
        super().__init__()
        if not hasattr(GraphProvider, 'ORACLE_PG'):
            logger.warning(
                'GraphProvider.ORACLE_PG is unavailable in the current runtime; '
                'falling back to GraphProvider.ORACLE. '
                'Restart the Python process/kernel to pick up updated enum definitions.'
            )

        self._query_runner = query_runner
        self._close_runner = close_runner
        self._query_transform = query_transform
        self.max_coroutines = normalized_semaphore_limit(max_coroutines)
        self.vector_index_params = vector_index_params
        self._fetch_job = fetch_job
        self._database = database
        self._supports_index_management = supports_index_management
        input_connect_kwargs = dict(connect_kwargs or {})
        self._connect_kwargs = dict(input_connect_kwargs)
        self._config_dir_explicit = 'config_dir' in input_connect_kwargs

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

        resolved_connection_input = configured_uri is not None or configured_dsn is not None
        self._connect_kwargs = _resolve_connect_kwargs(
            self._connect_kwargs,
            resolved_connection_input=resolved_connection_input,
            config_dir_explicit=self._config_dir_explicit,
        )

        self._user = configured_user
        self._password = configured_password
        self.pool: Any = None
        self._connection_lock = asyncio.Lock()
        self._log_queries = log_queries if log_queries is not None else _env_bool(
            os.getenv('ORACLE_LOG_QUERIES')
        )
        if not self._fetch_job and oracledb is not None:
            oracledb.defaults.fetch_lobs = False
        if self._query_runner is None:
            if self._dsn is None or self._user is None or self._password is None:
                raise ValueError(
                    'OraclePGDriver requires either a query_runner or Oracle credentials '
                    '(uri/dsn, user, password / ORACLE_URI, ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD).'
                )
            if oracledb is None:
                raise ImportError(
                    'oracledb is required for native OraclePGDriver mode. '
                    'Install it with: pip install graphiti-core[oracle]'
                )

        # Preserve compatibility where helper code still introspects RDF naming fields.
        self._graph_id = sanitize_graph_id(
            graph_id
            or os.getenv('ORACLE_PG_GRAPH_ID')
            or os.getenv('ORACLE_RDF_GRAPH_NAME')
            or os.getenv('ORACLE_RDF_GRAPH')
            or 'GRAPHITI'
        )
        self._rdf_graph_name = self._graph_id
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
        self.search_interface = OpsBackedSearchInterface()
        self.graph_operations_interface = OraclePGGraphOperationsAdapter()

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

    @property
    def client(self) -> Any:
        return self.pool

    @client.setter
    def client(self, value: Any) -> None:
        self.pool = value

    @property
    def graph_id(self) -> str:
        return self._graph_id

    @property
    def rdf_graph_name(self) -> str:
        return self._rdf_graph_name

    def table_name(self, base_name: str) -> str:
        return build_table_name(self._graph_id, base_name)

    def _log_query_if_enabled(self, query: str, params: dict[str, Any], mode: str) -> None:
        if not self._log_queries:
            return
        logger.info(
            'Oracle query mode=%s\n%s\nparams=%s',
            mode,
            query,
            _sanitize_params_for_logging(params),
        )

    def _log_query_elapsed_if_enabled(self, mode: str, elapsed_ms: float, status: str) -> None:
        if not self._log_queries:
            return
        logger.info(
            'Oracle query mode=%s status=%s elapsed_ms=%.1f',
            mode,
            status,
            elapsed_ms,
        )

    async def _execute_with_query_runner(
        self, query: str, params: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], Any, Any]:
        query_runner = self._query_runner
        if query_runner is None:
            raise ValueError('Oracle query_runner is not configured.')

        start = perf_counter()
        try:
            self._log_query_if_enabled(query, params, 'query_runner')
            result = await query_runner(query, params)
            self._log_query_elapsed_if_enabled(
                'query_runner', (perf_counter() - start) * 1000, 'ok'
            )
        except Exception as exc:
            sanitized_params = _sanitize_params_for_logging(params)
            elapsed_ms = (perf_counter() - start) * 1000
            self._log_query_elapsed_if_enabled('query_runner', elapsed_ms, 'error')
            logger.error(
                f'Error executing Oracle query in {elapsed_ms:.1f} ms: '
                f'{exc}\n{query}\n{sanitized_params}'
            )
            raise

        if isinstance(result, tuple):
            return result

        return result, None, None

    async def execute_query(self, cypher_query_: str, **kwargs: Any):
        params = _normalize_execute_query_params(kwargs.pop('params', None), kwargs)
        if self._query_runner is not None:
            return await self._execute_with_query_runner(cypher_query_, params)
        return await self._execute_oracledb_query(cypher_query_, params)

    async def _ensure_pool(self):
        if self.pool is None:
            if self._query_runner is not None:
                raise ValueError('Oracle native client is not available in query_runner mode.')

            if oracledb is None:
                raise ImportError(
                    'oracledb is required for native OraclePGDriver mode. '
                    'Install it with: pip install graphiti-core[oracle]'
                )

            create_pool_async = getattr(oracledb, 'create_pool_async', None)
            if create_pool_async is None:
                raise AttributeError(
                    'Installed oracledb package does not expose create_pool_async(). '
                    'Use a newer python-oracledb version in Thin mode.'
                )

            async with self._connection_lock:
                if self.pool is None:
                    pool = create_pool_async(
                        user=self._user,
                        password=self._password,
                        dsn=self._dsn,
                        **self._connect_kwargs,
                    )
                    if inspect.isawaitable(pool):
                        pool = await pool
                    self.pool = pool
                    logger.info(
                        'OraclePGDriver pool initialized dsn=%s stats=%s',
                        self._dsn,
                        _pool_stats_for_logging(pool),
                    )

        return self.pool

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

        start = perf_counter()
        try:
            async with pool.acquire() as connection:
                connection.autocommit = True
                await self._ensure_pg_tables_on_connection(connection)
                result = await self._run_query_async(connection, oracle_query, oracle_params)
                self._log_query_elapsed_if_enabled('native', (perf_counter() - start) * 1000, 'ok')
                return result
        except Exception as exc:
            sanitized_params = _sanitize_params_for_logging(oracle_params)
            elapsed_ms = (perf_counter() - start) * 1000
            self._log_query_elapsed_if_enabled('native', elapsed_ms, 'error')
            logger.error(
                f'Error executing Oracle PG query in {elapsed_ms:.1f} ms: '
                f'{exc}\n{oracle_query}\n{sanitized_params}'
            )
            raise

    def session(self, database: str | None = None) -> GraphDriverSession:
        if database is not None and database != self._database:
            return OraclePGDriverSession(driver=self.clone(database))
        return OraclePGDriverSession(driver=self)

    async def close(self) -> None:
        if self.pool is not None:
            pool = self.pool
            self.pool = None
            close_result = pool.close()
            if inspect.isawaitable(close_result):
                await close_result
        if self._close_runner is not None:
            await self._close_runner()

    def delete_all_indexes(self) -> Coroutine[Any, Any, None]:
        return self._delete_all_indexes_impl()

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
        for query in get_fulltext_indices(self._graph_id):
            await self.execute_query(query)
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
            supports_index_management=self._supports_index_management,
            connect_kwargs=clone_connect_kwargs,
            query_transform=self._query_transform,
            fetch_job=self._fetch_job,
            log_queries=self._log_queries,
            max_coroutines=self.max_coroutines,
            vector_index_params=self.vector_index_params,
        )
        if self.pool is not None and self._query_runner is not None:
            clone.pool = self.pool
        return clone
