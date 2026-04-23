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
from graphiti_core.driver.oracle.graph_queries import (
    get_fulltext_indices as get_oracle_fulltext_indices,
    get_range_indices as get_oracle_range_indices,
)
from graphiti_core.driver.oracle.rdf_utils import (
    ensure_embedding_table,
    get_rdf_table_name,
    sanitize_oracle_table_base,
    sanitize_rdf_graph_name,
)
from graphiti_core.helpers import semaphore_gather
from graphiti_core.driver.search_interface.ops_backed_search_interface import (
    OpsBackedSearchInterface,
)

logger = logging.getLogger(__name__)

QueryResult = tuple[list[dict[str, Any]], Any, Any] | list[dict[str, Any]]
QueryRunner = Callable[[str, dict[str, Any]], Awaitable[QueryResult]]
CloseRunner = Callable[[], Awaitable[None]]
QueryTransform = Callable[[str, dict[str, Any]], tuple[str, dict[str, Any]]]

_DOLLAR_BIND_PATTERN = re.compile(r'\$([A-Za-z_][A-Za-z0-9_]*)')
_CYPHER_MARKERS = ('MATCH (', 'MERGE (', 'DETACH DELETE', 'OPTIONAL MATCH', 'UNWIND ')
_ORACLE_IDENTIFIER_PATTERN = re.compile(r'^[A-Z][A-Z0-9_$#]*$')
_RDF_OWNER_REQUIRED_ERROR = (
    'RDF mode requires ORACLE_RDF_NETWORK_OWNER or ORACLE_USER/rdf_network_owner.'
)
_NATIVE_RDF_CYPHER_ERROR = (
    'Oracle RDF mode cannot execute Cypher directly. '
    'Use RDF/SPARQL update helpers or provide query_transform/query_runner.'
)


def _env_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {'1', 'true', 'yes', 'on'}


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


def _looks_like_cypher(query: str) -> bool:
    normalized = query.upper()
    return any(marker in normalized for marker in _CYPHER_MARKERS)


def _is_ignorable_rdf_index_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return 'already exists' in text or 'ora-00955' in text or 'ora-55318' in text


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
            logger.debug('OracleDriver using explicit connect_kwargs config_dir')
        else:
            logger.debug('OracleDriver has inherited config_dir in connect_kwargs')

    if not has_config_dir and config_dir:
        if not resolved_connection_input:
            resolved_connect_kwargs['config_dir'] = config_dir
            logger.debug('OracleDriver auto-applied ORACLE_CONFIG_DIR to connect_kwargs')
        else:
            logger.debug(
                'OracleDriver skipped ORACLE_CONFIG_DIR because DSN/URI was provided '
                '(argument or environment)'
            )

    if resolved_connection_input and 'config_dir' in resolved_connect_kwargs and not config_dir_explicit:
        resolved_connect_kwargs.pop('config_dir', None)
        logger.debug('OracleDriver removed non-explicit config_dir because DSN/URI was provided')

    return resolved_connect_kwargs


def _normalize_execute_query_params(
    params: dict[str, Any] | None, kwargs: dict[str, Any]
) -> dict[str, Any]:
    normalized_params = dict(params or {})
    normalized_params.update(kwargs)
    # These are Neo4j-driver execution hints and not query parameters.
    normalized_params.pop('database_', None)
    normalized_params.pop('routing_', None)
    return normalized_params


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
        supports_index_management: bool = True,
        connect_kwargs: dict[str, Any] | None = None,
        query_transform: QueryTransform | None = None,
        use_rdf: bool | None = None,
        rdf_network_owner: str | None = None,
        rdf_network_name: str | None = None,
        rdf_graph_name: str | None = None,
        rdf_tablespace: str | None = None,
        log_queries: bool | None = None,
    ):
        super().__init__()
        self._query_runner = query_runner
        self._close_runner = close_runner
        self._query_transform = query_transform
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

        # Skip ORACLE_CONFIG_DIR auto-injection whenever a DSN/URI is resolved (from args or env).
        # This prevents forcing wallet/TNS config for easy-connect style DSNs.
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
        use_rdf_env = _env_bool(os.getenv('ORACLE_USE_RDF'))
        self._use_rdf = use_rdf if use_rdf is not None else use_rdf_env
        self._log_queries = log_queries if log_queries is not None else _env_bool(
            os.getenv('ORACLE_LOG_QUERIES')
        )
        self._rdf_network_owner = (
            rdf_network_owner
            or os.getenv('ORACLE_RDF_NETWORK_OWNER')
            or (configured_user.upper() if configured_user else None)
        )
        self._rdf_network_name = (
            rdf_network_name
            or os.getenv('ORACLE_RDF_NETWORK_NAME')
            or os.getenv('ORACLE_RDF_NETWORK')
            or 'NET1'
        )
        self._rdf_graph_name = sanitize_rdf_graph_name(
            rdf_graph_name
            or os.getenv('ORACLE_RDF_GRAPH_NAME')
            or os.getenv('ORACLE_RDF_GRAPH')
            or 'GRAPHITI'
        )
        self._rdf_tablespace = rdf_tablespace or os.getenv('ORACLE_RDF_TABLESPACE') or 'DATA'
        self._rdf_namespace_prefix = f'gti:{sanitize_oracle_table_base(self._rdf_graph_name)}:'
        self._rdf_lock = asyncio.Lock()
        self._rdf_initialized = False
        self._rdf_tables_init_lock = asyncio.Lock()
        self._rdf_tables_bootstrapping = False

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
        self.search_interface = OpsBackedSearchInterface()
        self.graph_operations_interface = None

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

    @property
    def client(self) -> Any:
        """Backward-compatible alias for the native Oracle pool."""
        return self.pool

    @property
    def rdf_enabled(self) -> bool:
        return self._use_rdf

    @property
    def rdf_network_owner(self) -> str | None:
        return self._rdf_network_owner

    @property
    def rdf_network_name(self) -> str:
        return self._rdf_network_name

    @property
    def rdf_graph_name(self) -> str:
        return self._rdf_graph_name

    @property
    def rdf_namespace_prefix(self) -> str:
        return self._rdf_namespace_prefix

    @property
    def log_queries_enabled(self) -> bool:
        return self._log_queries

    def _log_query_if_enabled(self, query: str, params: dict[str, Any], mode: str) -> None:
        if not self._log_queries:
            return
        logger.info('Oracle query mode=%s\n%s\nparams=%s', mode, query, params)

    def _require_rdf_network_owner(self) -> str:
        network_owner = self._rdf_network_owner
        if not network_owner:
            raise ValueError(_RDF_OWNER_REQUIRED_ERROR)
        return network_owner.upper()

    async def _execute_with_query_runner(
        self, query: str, params: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], Any, Any]:
        query_runner = self._query_runner
        if query_runner is None:
            raise ValueError('Oracle query_runner is not configured.')

        try:
            self._log_query_if_enabled(query, params, 'query_runner')
            result = await query_runner(query, params)
        except Exception as exc:
            logger.error(f'Error executing Oracle query: {exc}\n{query}\n{params}')
            raise

        if isinstance(result, tuple):
            return result

        return result, None, None

    def rdf_table_name(self, table_name: str | None = None) -> str:
        return get_rdf_table_name(
            table_name=table_name or self._rdf_graph_name,
            network_owner=self._rdf_network_owner,
            network_name=self._rdf_network_name,
        )

    async def _rdf_object_exists(
        self, cursor: Any, owner: str, object_name: str, object_type: str | None = None
    ) -> bool:
        query = """
            SELECT COUNT(*)
            FROM ALL_OBJECTS
            WHERE OWNER = :owner
              AND OBJECT_NAME = :object_name
        """
        params: dict[str, Any] = {'owner': owner.upper(), 'object_name': object_name.upper()}
        if object_type is not None:
            query += ' AND OBJECT_TYPE = :object_type'
            params['object_type'] = object_type.upper()
        await cursor.execute(query, params)
        rows = await cursor.fetchall()
        count = rows[0][0] if rows else 0
        return count > 0

    async def _ensure_rdf_network_and_graph(self, connection: Any) -> None:
        if not self._use_rdf or self._rdf_initialized:
            return

        network_owner = self._require_rdf_network_owner()

        async with self._rdf_lock:
            if self._rdf_initialized:
                return

            network_name = self._rdf_network_name.upper()
            graph_name = self._rdf_graph_name
            network_parameter_object = f'{network_name}#RDF_PARAMETER'
            graph_table_object = f'{network_name}#RDFT_{graph_name.upper()}'

            with connection.cursor() as cursor:
                has_network = await self._rdf_object_exists(
                    cursor, network_owner, network_parameter_object
                )
                if not has_network:
                    await cursor.execute(
                        """
                        BEGIN
                          sem_apis.create_rdf_network(
                            :tablespace,
                            network_owner=>:network_owner,
                            network_name=>:network_name
                          );
                        END;
                        """,
                        {
                            'tablespace': self._rdf_tablespace,
                            'network_owner': network_owner,
                            'network_name': network_name,
                        },
                    )

                has_graph = await self._rdf_object_exists(cursor, network_owner, graph_table_object)
                if not has_graph:
                    await cursor.execute(
                        """
                        BEGIN
                          sem_apis.create_rdf_graph(
                            :graph_name,
                            NULL,
                            NULL,
                            network_owner=>:network_owner,
                            network_name=>:network_name
                          );
                        END;
                        """,
                        {
                            'graph_name': graph_name,
                            'network_owner': network_owner,
                            'network_name': network_name,
                        },
                    )

            self._rdf_initialized = True

    async def _ensure_rdf_embedding_tables(self) -> None:
        if self._query_runner is not None:
            return
        if not self._use_rdf:
            return
        if self._rdf_tables_bootstrapping:
            return

        async with self._rdf_tables_init_lock:
            self._rdf_tables_bootstrapping = True
            try:
                await ensure_embedding_table(self)
            finally:
                self._rdf_tables_bootstrapping = False

    @client.setter
    def client(self, value: Any) -> None:
        self.pool = value

    async def execute_query(self, cypher_query_: str, **kwargs: Any):
        params = _normalize_execute_query_params(kwargs.pop('params', None), kwargs)

        if self._query_runner is not None:
            return await self._execute_with_query_runner(cypher_query_, params)

        if self._use_rdf and _looks_like_cypher(cypher_query_):
            raise ValueError(_NATIVE_RDF_CYPHER_ERROR)

        await self._ensure_rdf_embedding_tables()
        return await self._execute_oracledb_query(cypher_query_, params)

    async def _ensure_pool(self):
        if self.pool is None:
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
                if self.pool is None:
                    pool = create_pool_async(
                        user=self._user,
                        password=self._password,
                        dsn=self._dsn,
                        **self._connect_kwargs,
                    )
                    # Depending on python-oracledb version, create_pool_async()
                    # may return a pool directly or an awaitable resolving to one.
                    if inspect.isawaitable(pool):
                        pool = await pool
                    self.pool = pool

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

    async def _execute_oracledb_query(self, query: str, params: dict[str, Any]):
        pool = await self._ensure_pool()
        oracle_query, oracle_params = self._prepare_oracle_query(query, params)
        self._log_query_if_enabled(oracle_query, oracle_params, 'native')

        try:
            async with pool.acquire() as connection:
                # Keep behavior close to other Graphiti drivers.
                connection.autocommit = True
                await self._ensure_rdf_network_and_graph(connection)
                return await self._run_query_async(connection, oracle_query, oracle_params)
        except Exception as exc:
            logger.error(f'Error executing Oracle query: {exc}\n{oracle_query}\n{oracle_params}')
            raise

    def session(self, database: str | None = None) -> GraphDriverSession:
        if database is not None and database != self._database:
            return OracleDriverSession(driver=self.clone(database))
        return OracleDriverSession(driver=self)

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

    async def _delete_all_indexes_impl(self) -> None:
        # Index deletion is backend-specific and intentionally left to the adapter
        # implementation. This method is a safe no-op by default.
        return

    async def _create_rdf_datatype_indexes(self) -> None:
        if not self._use_rdf:
            return

        network_owner = self._require_rdf_network_owner()
        network_name = self._rdf_network_name.upper()
        datatype_uris = [
            'http://www.w3.org/2001/XMLSchema#decimal',
            'http://www.w3.org/2001/XMLSchema#string',
            'http://www.w3.org/2001/XMLSchema#time',
            'http://www.w3.org/2001/XMLSchema#dateTime',
            'http://www.w3.org/2001/XMLSchema#date',
            'http://xmlns.oracle.com/rdf/geo/WKTLiteral',
            'http://www.opengis.net/geosparql#wktLiteral',
            'http://www.opengis.net/geosparql#gmlLiteral',
            'http://xmlns.oracle.com/rdf/like'
        ]
        for datatype_uri in datatype_uris:
            index_exists = await self._rdf_datatype_index_exists(
                datatype_uri=datatype_uri,
                network_owner=network_owner,
                network_name=network_name,
            )
            if index_exists:
                continue
            try:
                await self.execute_query(
                    """
                    BEGIN
                      sem_apis.add_datatype_index(
                        $datatype_uri,
                        network_owner=>$network_owner,
                        network_name=>$network_name
                      );
                    END;
                    """,
                    datatype_uri=datatype_uri,
                    network_owner=network_owner,
                    network_name=network_name,
                )
            except Exception as exc:
                if _is_ignorable_rdf_index_error(exc):
                    continue
                raise

    async def _rdf_datatype_index_exists(
        self,
        datatype_uri: str,
        network_owner: str,
        network_name: str,
    ) -> bool:
        network_owner_upper = network_owner.upper()
        network_name_upper = network_name.upper()
        if not _ORACLE_IDENTIFIER_PATTERN.match(network_owner_upper):
            raise ValueError(f'Invalid Oracle network owner identifier: {network_owner}')
        if not _ORACLE_IDENTIFIER_PATTERN.match(network_name_upper):
            raise ValueError(f'Invalid Oracle network name identifier: {network_name}')
        dtype_index_info_table = f'{network_owner_upper}.{network_name_upper}#SEM_DTYPE_INDEX_INFO'

        records, _, _ = await self.execute_query(
            f"""
            SELECT COUNT(*) AS index_count
            FROM {dtype_index_info_table}
            WHERE datatype = $datatype_uri
            """,
            datatype_uri=datatype_uri,
        )
        if not records:
            return False

        first_record = records[0]
        count_value = first_record.get('index_count')
        if count_value is None:
            for key, value in first_record.items():
                if key.lower() == 'index_count':
                    count_value = value
                    break

        if count_value is None:
            return False

        try:
            return int(count_value) > 0
        except (TypeError, ValueError):
            return bool(count_value)

    async def build_indices_and_constraints(self, delete_existing: bool = False):
        if not self._supports_index_management:
            return

        if self._use_rdf:
            # RDF mode uses SEM_APIS datatype indexes, not Graphiti's Cypher index DDL.
            await self._create_rdf_datatype_indexes()
            return

        # Oracle Graph index management is backend-specific. Graphiti's built-in index
        # DDL is Cypher/Neo4j-oriented, so raw native mode should not execute it.
        # Use query_runner/query_transform for Oracle-specific DDL translation.
        if self._query_runner is None and self._query_transform is None:
            logger.info(
                'Skipping Oracle index creation in native mode because Graphiti index DDL is '
                'Neo4j/Cypher-oriented. Provide query_runner or query_transform for Oracle '
                'index translation.'
            )
            return

        if delete_existing:
            await self.delete_all_indexes()

        index_queries = get_oracle_range_indices(self) + get_oracle_fulltext_indices(self)
        await semaphore_gather(*[self.execute_query(query) for query in index_queries])

    def clone(self, database: str) -> OracleDriver:
        if database == self._database:
            return self
        clone_connect_kwargs = dict(self._connect_kwargs)
        if not self._config_dir_explicit:
            clone_connect_kwargs.pop('config_dir', None)
        return OracleDriver(
            query_runner=self._query_runner,
            close_runner=self._close_runner,
            uri=self._uri,
            dsn=self._configured_dsn,
            user=self._user,
            password=self._password,
            database=database,
            supports_index_management=self._supports_index_management,
            connect_kwargs=clone_connect_kwargs,
            query_transform=self._query_transform,
            use_rdf=self._use_rdf,
            rdf_network_owner=self._rdf_network_owner,
            rdf_network_name=self._rdf_network_name,
            rdf_graph_name=self._rdf_graph_name,
            rdf_tablespace=self._rdf_tablespace,
            log_queries=self._log_queries,
        )
