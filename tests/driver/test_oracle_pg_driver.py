"""
Tests for OraclePGDriver table-backed behavior.
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

import graphiti_core.driver.oracle_pg_driver as oracle_pg_driver_module
from graphiti_core.driver.driver import GraphProvider
from graphiti_core.driver.oracle_pg.graph_operations_adapter import OraclePGGraphOperationsAdapter
from graphiti_core.driver.oracle_pg.maintenance.graph_data_operations import (
    clear_data as clear_data_oracle_pg_maintenance,
    retrieve_episodes as retrieve_episodes_oracle_pg_maintenance,
)
from graphiti_core.driver.oracle_pg_driver import OraclePGDriver
from graphiti_core.nodes import EpisodeType
from graphiti_core.search.search_filters import SearchFilters
from graphiti_core.utils.maintenance.edge_operations import filter_existing_duplicate_of_edges


class _Cursor:
    def __init__(self, calls: list[tuple[str, dict[str, Any] | None]]):
        self.calls = calls
        self.description: list[tuple[str]] | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    async def execute(self, query: str, params: dict[str, Any] | None = None):
        self.calls.append((query, params))
        normalized = query.strip().upper()
        if normalized.startswith('SELECT'):
            self.description = [('UUID',)]
        else:
            self.description = None

    async def fetchall(self):
        return [('abc',)]


class _Connection:
    def __init__(self, calls: list[tuple[str, dict[str, Any] | None]]):
        self.calls = calls
        self.autocommit = False

    def cursor(self):
        return _Cursor(self.calls)


class _AcquireCtx:
    def __init__(self, connection: _Connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Pool:
    def __init__(self, connection: _Connection):
        self.connection = connection
        self.closed = False

    def acquire(self):
        return _AcquireCtx(self.connection)

    async def close(self):
        self.closed = True


class _OracleDb:
    def __init__(self, connection: _Connection):
        self.connection = connection
        self.calls: list[dict[str, Any]] = []
        self.pool = _Pool(connection)

    async def create_pool_async(self, **kwargs):
        self.calls.append(kwargs)
        return self.pool


class _OracleDefaults:
    def __init__(self, fetch_job: bool = True):
        self.fetch_job = fetch_job


class _OracleDbWithDefaults(_OracleDb):
    def __init__(self, connection: _Connection, defaults: _OracleDefaults):
        super().__init__(connection)
        self.defaults = defaults


@pytest.mark.asyncio
async def test_oracle_pg_driver_bootstraps_prefixed_tables(monkeypatch: pytest.MonkeyPatch):
    query_calls: list[tuple[str, dict[str, Any] | None]] = []
    fake_oracledb = _OracleDb(_Connection(query_calls))
    monkeypatch.setattr(oracle_pg_driver_module, 'oracledb', fake_oracledb)

    driver = OraclePGDriver(
        uri='dbhost:1521/service_name',
        user='scott',
        password='tiger',
        graph_id='my graph-01',
    )

    assert driver.provider == GraphProvider.ORACLE_PG
    assert driver.graph_id == 'MY_GRAPH_01'
    assert driver.table_name('entity_nodes') == 'MY_GRAPH_01_ENTITY_NODES'
    assert isinstance(driver.graph_operations_interface, OraclePGGraphOperationsAdapter)

    records, keys, summary = await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')
    assert records == [{'uuid': 'abc'}]
    assert keys == ['uuid']
    assert summary is None
    assert fake_oracledb.calls == [
        {'user': 'scott', 'password': 'tiger', 'dsn': 'dbhost:1521/service_name'}
    ]

    create_table_queries = [query for query, _ in query_calls if 'CREATE TABLE MY_GRAPH_01_' in query]
    assert len(create_table_queries) == 9
    assert any('CREATE PROPERTY GRAPH MY_GRAPH_01_PG' in query for query, _ in query_calls)
    assert any(
        query == 'SELECT :uuid AS uuid FROM dual' and params == {'uuid': 'abc'}
        for query, params in query_calls
    )

    ddl_calls_after_first_run = len(create_table_queries)
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')
    create_table_queries_after_second = [
        query for query, _ in query_calls if 'CREATE TABLE MY_GRAPH_01_' in query
    ]
    assert len(create_table_queries_after_second) == ddl_calls_after_first_run

    await driver.close()
    assert fake_oracledb.pool.closed


def test_oracle_pg_driver_clone_preserves_graph_id():
    async def _query_runner(_query: str, _params: dict[str, Any]):
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='Team One')
    clone = driver.clone('another_db')

    assert clone is not driver
    assert clone.provider == GraphProvider.ORACLE_PG
    assert clone.graph_id == 'TEAM_ONE'


@pytest.mark.asyncio
async def test_oracle_pg_driver_session_provider_and_run():
    query_calls: list[tuple[str, dict[str, Any]]] = []

    async def _query_runner(query: str, params: dict[str, Any]):
        query_calls.append((query, params))
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='session-demo')
    async with driver.session() as session:
        assert session.provider == GraphProvider.ORACLE_PG
        await session.run('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert query_calls == [('SELECT $uuid AS uuid FROM dual', {'uuid': 'abc'})]


@pytest.mark.asyncio
async def test_oracle_pg_driver_close_calls_close_runner():
    close_calls = 0

    async def _close_runner():
        nonlocal close_calls
        close_calls += 1

    async def _query_runner(_query: str, _params: dict[str, Any]):
        return []

    driver = OraclePGDriver(query_runner=_query_runner, close_runner=_close_runner, graph_id='close-demo')
    await driver.close()

    assert close_calls == 1


def test_oracle_pg_driver_sets_fetch_job_default_false(monkeypatch: pytest.MonkeyPatch):
    defaults = _OracleDefaults(fetch_job=True)
    fake_oracledb = _OracleDbWithDefaults(_Connection([]), defaults)
    monkeypatch.setattr(oracle_pg_driver_module, 'oracledb', fake_oracledb)

    OraclePGDriver(query_runner=lambda *_args, **_kwargs: None)  # type: ignore[arg-type]

    assert defaults.fetch_job is False


def test_oracle_pg_driver_sanitizes_query_vec_json_string():
    redacted = oracle_pg_driver_module._sanitize_params_for_logging(  # noqa: SLF001
        {
            'query_vec': '[0.1, 0.2, 0.3]',
            'min_score': 0.7,
        }
    )

    assert redacted == {
        'query_vec': '<redacted float_list len=3>',
        'min_score': 0.7,
    }


@pytest.mark.asyncio
async def test_oracle_pg_driver_build_indices_and_constraints_executes_pg_ddl():
    query_calls: list[tuple[str, dict[str, Any]]] = []

    async def _query_runner(query: str, params: dict[str, Any]):
        query_calls.append((query, params))
        if 'FROM user_indexes' in query:
            return []
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='idx-demo')
    await driver.build_indices_and_constraints()

    create_table_queries = [query for query, _ in query_calls if 'CREATE TABLE IDX_DEMO_' in query]
    create_index_queries = [query for query, _ in query_calls if 'CREATE INDEX' in query]

    assert len(create_table_queries) == 9
    assert any('CREATE PROPERTY GRAPH IDX_DEMO_PG' in query for query, _ in query_calls)
    assert len(create_index_queries) > 0


@pytest.mark.asyncio
async def test_oracle_pg_driver_build_indices_and_constraints_can_drop_tables():
    query_calls: list[tuple[str, dict[str, Any]]] = []

    async def _query_runner(query: str, params: dict[str, Any]):
        query_calls.append((query, params))
        if 'FROM user_indexes' in query:
            return []
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='drop-demo')
    await driver.build_indices_and_constraints(delete_existing=True, drop_tables=True)

    assert any('DROP PROPERTY GRAPH DROP_DEMO_PG' in query for query, _ in query_calls)
    assert any('DROP TABLE DROP_DEMO_ENTITY_NODES' in query for query, _ in query_calls)
    assert any('DROP TABLE DROP_DEMO_ENTITY_EDGES' in query for query, _ in query_calls)


@pytest.mark.asyncio
async def test_oracle_pg_maintenance_retrieve_episodes_without_saga_uses_maintenance_sql():
    query_calls: list[tuple[str, dict[str, Any]]] = []

    async def _query_runner(query: str, params: dict[str, Any]):
        query_calls.append((query, params))
        if 'FROM MAINT_DEMO_NO_SAGA_EPISODIC_NODES n' in query:
            return [
                {
                    'uuid': 'ep-2',
                    'group_id': 'group-1',
                    'name': 'Episode 2',
                    'source': 'message',
                    'source_description': '',
                    'content': 'newer',
                    'entity_edges': '[]',
                    'created_at': datetime(2026, 1, 2, 0, 0, 0),
                    'valid_at': datetime(2026, 1, 2, 0, 0, 0),
                },
                {
                    'uuid': 'ep-1',
                    'group_id': 'group-1',
                    'name': 'Episode 1',
                    'source': 'message',
                    'source_description': '',
                    'content': 'older',
                    'entity_edges': '[]',
                    'created_at': datetime(2026, 1, 1, 0, 0, 0),
                    'valid_at': datetime(2026, 1, 1, 0, 0, 0),
                },
            ]
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='maint-demo-no-saga')
    retrieve_mock = AsyncMock(return_value=[])
    driver.episode_node_ops.retrieve_episodes = retrieve_mock  # type: ignore[method-assign]

    episodes = await retrieve_episodes_oracle_pg_maintenance(
        driver=driver,
        reference_time=datetime(2026, 2, 1, 0, 0, 0),
        last_n=2,
        group_ids=['group-1'],
        source=EpisodeType.message,
        saga=None,
    )

    retrieve_mock.assert_not_awaited()
    assert [episode.uuid for episode in episodes] == ['ep-1', 'ep-2']
    assert any('FROM MAINT_DEMO_NO_SAGA_EPISODIC_NODES n' in query for query, _ in query_calls)


@pytest.mark.asyncio
async def test_oracle_pg_maintenance_retrieve_episodes_with_saga_uses_maintenance_sql():
    query_calls: list[tuple[str, dict[str, Any]]] = []

    async def _query_runner(query: str, params: dict[str, Any]):
        query_calls.append((query, params))
        if 'FROM MAINT_DEMO_SAGA_SAGA_NODES s' in query:
            return [
                {
                    'uuid': 'ep-2',
                    'group_id': 'group-1',
                    'name': 'Episode 2',
                    'source': 'message',
                    'source_description': '',
                    'content': 'newer',
                    'entity_edges': '[]',
                    'created_at': datetime(2026, 1, 2, 0, 0, 0),
                    'valid_at': datetime(2026, 1, 2, 0, 0, 0),
                },
                {
                    'uuid': 'ep-1',
                    'group_id': 'group-1',
                    'name': 'Episode 1',
                    'source': 'message',
                    'source_description': '',
                    'content': 'older',
                    'entity_edges': '[]',
                    'created_at': datetime(2026, 1, 1, 0, 0, 0),
                    'valid_at': datetime(2026, 1, 1, 0, 0, 0),
                },
            ]
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='maint-demo-saga')
    retrieve_mock = AsyncMock(return_value=[])
    driver.episode_node_ops.retrieve_episodes = retrieve_mock  # type: ignore[method-assign]

    episodes = await retrieve_episodes_oracle_pg_maintenance(
        driver=driver,
        reference_time=datetime(2026, 2, 1, 0, 0, 0),
        last_n=2,
        group_ids=['group-1'],
        source=EpisodeType.message,
        saga='my-saga',
    )

    retrieve_mock.assert_not_awaited()
    assert len(episodes) == 2
    assert [episode.uuid for episode in episodes] == ['ep-1', 'ep-2']
    assert any('FROM MAINT_DEMO_SAGA_SAGA_NODES s' in query for query, _ in query_calls)


@pytest.mark.asyncio
async def test_oracle_pg_maintenance_clear_data_group_ids_uses_node_ops():
    async def _query_runner(_query: str, _params: dict[str, Any]):
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='maint-clear-group')
    driver.entity_node_ops.delete_by_group_id = AsyncMock()  # type: ignore[method-assign]
    driver.episode_node_ops.delete_by_group_id = AsyncMock()  # type: ignore[method-assign]
    driver.community_node_ops.delete_by_group_id = AsyncMock()  # type: ignore[method-assign]
    driver.graph_ops.clear_data = AsyncMock()  # type: ignore[method-assign]

    await clear_data_oracle_pg_maintenance(driver, ['g1', 'g2'])

    driver.graph_ops.clear_data.assert_not_awaited()
    assert driver.entity_node_ops.delete_by_group_id.await_count == 2
    assert driver.episode_node_ops.delete_by_group_id.await_count == 2
    assert driver.community_node_ops.delete_by_group_id.await_count == 2


@pytest.mark.asyncio
async def test_oracle_pg_maintenance_clear_data_no_group_ids_uses_graph_ops():
    async def _query_runner(_query: str, _params: dict[str, Any]):
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='maint-clear-all')
    driver.entity_node_ops.delete_by_group_id = AsyncMock()  # type: ignore[method-assign]
    driver.episode_node_ops.delete_by_group_id = AsyncMock()  # type: ignore[method-assign]
    driver.community_node_ops.delete_by_group_id = AsyncMock()  # type: ignore[method-assign]
    driver.graph_ops.clear_data = AsyncMock()  # type: ignore[method-assign]

    await clear_data_oracle_pg_maintenance(driver, None)

    driver.graph_ops.clear_data.assert_awaited_once_with(driver, None)
    driver.entity_node_ops.delete_by_group_id.assert_not_awaited()
    driver.episode_node_ops.delete_by_group_id.assert_not_awaited()
    driver.community_node_ops.delete_by_group_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_oracle_pg_filter_existing_duplicate_of_edges_uses_entity_edge_ops():
    async def _query_runner(_query: str, _params: dict[str, Any]):
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='dup-demo')
    get_between_nodes_mock = AsyncMock(
        side_effect=[
            [SimpleNamespace(name='IS_DUPLICATE_OF')],
            [SimpleNamespace(name='RELATED_TO')],
        ]
    )
    driver.entity_edge_ops.get_between_nodes = get_between_nodes_mock  # type: ignore[method-assign]

    pair_1 = (SimpleNamespace(uuid='s1'), SimpleNamespace(uuid='t1'))
    pair_2 = (SimpleNamespace(uuid='s2'), SimpleNamespace(uuid='t2'))
    remaining = await filter_existing_duplicate_of_edges(driver, [pair_1, pair_2])  # type: ignore[arg-type]

    assert remaining == [pair_2]
    assert get_between_nodes_mock.await_count == 2
    assert get_between_nodes_mock.await_args_list[0].args == (driver, 's1', 't1')
    assert get_between_nodes_mock.await_args_list[1].args == (driver, 's2', 't2')


@pytest.mark.asyncio
async def test_oracle_pg_graph_operations_adapter_delegates_node_save_bulk():
    async def _query_runner(_query: str, _params: dict[str, Any]):
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='adapter-demo')
    adapter = driver.graph_operations_interface
    assert adapter is not None

    save_bulk_mock = AsyncMock()
    driver.entity_node_ops.save_bulk = save_bulk_mock  # type: ignore[method-assign]

    await adapter.node_save_bulk(None, driver, None, [], batch_size=25)

    save_bulk_mock.assert_awaited_once_with(driver, [], tx=None, batch_size=25)


@pytest.mark.asyncio
async def test_oracle_pg_search_interface_delegates_episode_fulltext_search():
    async def _query_runner(_query: str, _params: dict[str, Any]):
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='episode-search')
    delegated_search = AsyncMock(return_value=[])
    driver.search_ops.episode_fulltext_search = delegated_search  # type: ignore[method-assign]

    assert driver.search_interface is not None
    search_filter = SearchFilters()
    await driver.search_interface.episode_fulltext_search(
        driver, 'incident report', search_filter, ['group-1'], 4
    )

    assert delegated_search.await_args is not None
    delegated_args = delegated_search.await_args.args
    assert delegated_args == (driver, 'incident report', search_filter, ['group-1'], 4)


@pytest.mark.asyncio
async def test_oracle_pg_search_interface_delegates_node_similarity_search():
    async def _query_runner(_query: str, _params: dict[str, Any]):
        return []

    driver = OraclePGDriver(query_runner=_query_runner, graph_id='node-sim-search')
    delegated_search = AsyncMock(return_value=[])
    driver.search_ops.node_similarity_search = delegated_search  # type: ignore[method-assign]

    assert driver.search_interface is not None
    search_filter = SearchFilters()
    await driver.search_interface.node_similarity_search(
        driver, [0.1, 0.2, 0.3], search_filter, ['group-1'], 6, 0.42
    )

    assert delegated_search.await_args is not None
    delegated_args = delegated_search.await_args.args
    assert delegated_args == (driver, [0.1, 0.2, 0.3], search_filter, ['group-1'], 6, 0.42)
