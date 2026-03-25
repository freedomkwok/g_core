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

import logging
import os
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest

import graphiti_core.driver.oracle_driver as oracle_driver_module
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
from graphiti_core.driver.oracle_driver import OracleDriver, OracleDriverSession
from graphiti_core.nodes import EpisodeType
from graphiti_core.utils.maintenance.graph_data_operations import (
    retrieve_episodes as retrieve_episodes_maintenance,
)


class _FakeCursor:
    def __init__(self):
        self.description = [('UUID',)]
        self._rows = [('abc',)]
        self.executed: list[tuple[str, dict | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    async def execute(self, query: str, params: dict | None = None):
        self.executed.append((query, params))

    async def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return self._cursor

    async def close(self):
        self.closed = True


class _FakeAcquireContext:
    def __init__(self, pool: '_FakePool'):
        self._pool = pool

    async def __aenter__(self):
        self._pool.acquire_calls += 1
        return self._pool._connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def __init__(self, connection: _FakeConnection):
        self._connection = connection
        self.acquire_calls = 0
        self.closed = False

    def acquire(self):
        return _FakeAcquireContext(self)

    async def close(self):
        self.closed = True


class _FakeOracleDb:
    def __init__(self, connection: _FakeConnection):
        self._pool = _FakePool(connection)
        self.create_pool_async_calls: list[dict] = []

    async def create_pool_async(self, **kwargs):
        self.create_pool_async_calls.append(kwargs)
        return self._pool


class _SyncPoolFactoryOracleDb:
    def __init__(self, connection: _FakeConnection):
        self._pool = _FakePool(connection)
        self.create_pool_async_calls: list[dict] = []

    def create_pool_async(self, **kwargs):
        self.create_pool_async_calls.append(kwargs)
        return self._pool


class _FailingOracleDb:
    async def create_pool_async(self, **kwargs):
        raise RuntimeError('connect failed')


@pytest.mark.asyncio
async def test_execute_query_strips_neo4j_specific_hints():
    query_runner = AsyncMock(return_value=[{'ok': True}])
    driver = OracleDriver(query_runner=query_runner)

    records, keys, summary = await driver.execute_query(
        'RETURN $value AS ok',
        value=True,
        routing_='r',
        database_='ignored',
    )

    query_runner.assert_awaited_once_with('RETURN $value AS ok', {'value': True})
    assert records == [{'ok': True}]
    assert keys is None
    assert summary is None


@pytest.mark.asyncio
async def test_execute_query_accepts_tuple_results():
    query_runner = AsyncMock(return_value=([{'ok': 1}], ['ok'], {'summary': 'done'}))
    driver = OracleDriver(query_runner=query_runner)

    records, keys, summary = await driver.execute_query('RETURN 1 AS ok')

    assert records == [{'ok': 1}]
    assert keys == ['ok']
    assert summary == {'summary': 'done'}


@pytest.mark.asyncio
async def test_execute_query_merges_params_with_kwargs():
    query_runner = AsyncMock(return_value=[])
    driver = OracleDriver(query_runner=query_runner)

    await driver.execute_query('RETURN 1', params={'a': 1, 'b': 1}, b=2, c=3)

    query_runner.assert_awaited_once_with('RETURN 1', {'a': 1, 'b': 2, 'c': 3})


@pytest.mark.asyncio
async def test_execute_query_uses_oracledb_when_no_query_runner(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OracleDriver(uri='dbhost:1521/service_name', user='scott', password='tiger')
    assert fake_oracledb.create_pool_async_calls == []

    records, keys, summary = await driver.execute_query(
        'SELECT $uuid AS uuid FROM dual',
        uuid='abc',
    )

    assert fake_oracledb.create_pool_async_calls == [
        {'user': 'scott', 'password': 'tiger', 'dsn': 'dbhost:1521/service_name'}
    ]
    assert fake_cursor.executed == [('SELECT :uuid AS uuid FROM dual', {'uuid': 'abc'})]
    assert records == [{'uuid': 'abc'}]
    assert keys == ['uuid']
    assert summary is None

    await driver.close()
    assert fake_oracledb._pool.closed


@pytest.mark.asyncio
async def test_execute_query_supports_non_awaitable_pool_factory(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _SyncPoolFactoryOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OracleDriver(uri='dbhost:1521/service_name', user='scott', password='tiger')
    records, keys, summary = await driver.execute_query(
        'SELECT $uuid AS uuid FROM dual',
        uuid='abc',
    )

    assert fake_oracledb.create_pool_async_calls == [
        {'user': 'scott', 'password': 'tiger', 'dsn': 'dbhost:1521/service_name'}
    ]
    assert records == [{'uuid': 'abc'}]
    assert keys == ['uuid']
    assert summary is None


@pytest.mark.asyncio
async def test_execute_query_reuses_single_native_connection(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OracleDriver(uri='dbhost:1521/service_name', user='scott', password='tiger')

    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='def')

    assert len(fake_oracledb.create_pool_async_calls) == 1
    assert fake_oracledb._pool.acquire_calls == 2
    assert len(fake_cursor.executed) == 2


@pytest.mark.asyncio
async def test_execute_query_reconnects_after_close(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OracleDriver(uri='dbhost:1521/service_name', user='scott', password='tiger')
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')
    await driver.close()
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='def')

    assert len(fake_oracledb.create_pool_async_calls) == 2


@pytest.mark.asyncio
async def test_execute_query_uses_env_credentials_and_parsed_uri(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)
    monkeypatch.setenv('ORACLE_URI', 'oracle://env_user:env_pass@envhost:1522/envservice')
    monkeypatch.delenv('ORACLE_USER', raising=False)
    monkeypatch.delenv('ORACLE_PASSWORD', raising=False)

    driver = OracleDriver()
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert fake_oracledb.create_pool_async_calls == [
        {'user': 'env_user', 'password': 'env_pass', 'dsn': 'envhost:1522/envservice'}
    ]


@pytest.mark.asyncio
async def test_execute_query_uses_explicit_dsn(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    dsn = (
        '(DESCRIPTION=(RETRY_COUNT=20)(RETRY_DELAY=3)'
        '(ADDRESS=(PROTOCOL=TCPS)(HOST=myhost.oraclecloud.com)(PORT=1521))'
        '(CONNECT_DATA=(SERVICE_NAME=myservice.oraclecloud.com))'
        '(SECURITY=(SSL_SERVER_DN_MATCH=NO)))'
    )
    driver = OracleDriver(dsn=dsn, user='scott', password='tiger')
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert fake_oracledb.create_pool_async_calls == [{'user': 'scott', 'password': 'tiger', 'dsn': dsn}]


@pytest.mark.asyncio
async def test_execute_query_allows_alias_dsn_with_explicit_config_dir(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OracleDriver(
        dsn='MYDB_HIGH',
        user='scott',
        password='tiger',
        connect_kwargs={'config_dir': '/opt/oracle/network/admin'},
    )
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert fake_oracledb.create_pool_async_calls == [
        {
            'user': 'scott',
            'password': 'tiger',
            'dsn': 'MYDB_HIGH',
            'config_dir': '/opt/oracle/network/admin',
        }
    ]


@pytest.mark.asyncio
async def test_execute_query_does_not_auto_use_oracle_config_dir_for_explicit_dsn(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)
    monkeypatch.setenv('ORACLE_CONFIG_DIR', '/opt/oracle/wallet')

    dsn = 'dbhost:1521/service_name'
    driver = OracleDriver(dsn=dsn, user='scott', password='tiger')
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert fake_oracledb.create_pool_async_calls == [
        {'user': 'scott', 'password': 'tiger', 'dsn': dsn}
    ]


@pytest.mark.asyncio
async def test_execute_query_uses_oracle_dsn_env(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)
    monkeypatch.delenv('ORACLE_URI', raising=False)
    monkeypatch.setenv('ORACLE_DSN', 'envhost:1522/envservice')
    monkeypatch.setenv('ORACLE_USER', 'env_user')
    monkeypatch.setenv('ORACLE_PASSWORD', 'env_pass')

    driver = OracleDriver()
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert fake_oracledb.create_pool_async_calls == [
        {'user': 'env_user', 'password': 'env_pass', 'dsn': 'envhost:1522/envservice'}
    ]


@pytest.mark.asyncio
async def test_execute_query_passes_connect_kwargs_to_pool_creation(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OracleDriver(
        uri='dbhost:1521/service_name',
        user='scott',
        password='tiger',
        connect_kwargs={'events': True},
    )
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert fake_oracledb.create_pool_async_calls == [
        {'user': 'scott', 'password': 'tiger', 'dsn': 'dbhost:1521/service_name', 'events': True}
    ]


@pytest.mark.asyncio
async def test_execute_query_does_not_auto_use_oracle_config_dir_for_explicit_uri(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)
    monkeypatch.setenv('ORACLE_CONFIG_DIR', '/opt/oracle/wallet')

    driver = OracleDriver(uri='dbhost:1521/service_name', user='scott', password='tiger')
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert fake_oracledb.create_pool_async_calls == [
        {
            'user': 'scott',
            'password': 'tiger',
            'dsn': 'dbhost:1521/service_name',
        }
    ]


@pytest.mark.asyncio
async def test_execute_query_does_not_auto_use_oracle_config_dir_for_env_uri(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)
    monkeypatch.setenv('ORACLE_URI', 'oracle://env_user:env_pass@envhost:1522/envservice')
    monkeypatch.setenv('ORACLE_CONFIG_DIR', '/opt/oracle/wallet')

    driver = OracleDriver()
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert fake_oracledb.create_pool_async_calls == [
        {
            'user': 'env_user',
            'password': 'env_pass',
            'dsn': 'envhost:1522/envservice',
        }
    ]


@pytest.mark.asyncio
async def test_execute_query_does_not_auto_use_oracle_config_dir_for_env_dsn(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)
    monkeypatch.setenv('ORACLE_DSN', 'envhost:1522/envservice')
    monkeypatch.setenv('ORACLE_USER', 'env_user')
    monkeypatch.setenv('ORACLE_PASSWORD', 'env_pass')
    monkeypatch.setenv('ORACLE_CONFIG_DIR', '/opt/oracle/wallet')

    driver = OracleDriver()
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert fake_oracledb.create_pool_async_calls == [
        {
            'user': 'env_user',
            'password': 'env_pass',
            'dsn': 'envhost:1522/envservice',
        }
    ]


@pytest.mark.asyncio
async def test_execute_query_surfaces_pool_creation_errors(monkeypatch):
    monkeypatch.setattr(oracle_driver_module, 'oracledb', _FailingOracleDb())
    driver = OracleDriver(uri='dbhost:1521/service_name', user='scott', password='tiger')

    with pytest.raises(RuntimeError, match='connect failed'):
        await driver.execute_query('SELECT 1 FROM dual')


@pytest.mark.asyncio
async def test_execute_query_without_runner_or_credentials_raises(monkeypatch):
    monkeypatch.delenv('ORACLE_URI', raising=False)
    monkeypatch.delenv('ORACLE_DSN', raising=False)
    monkeypatch.delenv('ORACLE_USER', raising=False)
    monkeypatch.delenv('ORACLE_PASSWORD', raising=False)

    with pytest.raises(ValueError):
        OracleDriver()


def test_session_uses_cloned_driver_for_database_override():
    driver = OracleDriver(query_runner=AsyncMock(), database='default')

    session = driver.session(database='tenant_a')

    assert isinstance(session, OracleDriverSession)
    assert session.driver is not driver
    assert session.driver._database == 'tenant_a'
    assert driver._database == 'default'


def test_clone_reuses_runners_and_configuration():
    query_runner = AsyncMock()
    close_runner = AsyncMock()
    driver = OracleDriver(
        query_runner=query_runner,
        close_runner=close_runner,
        database='default',
        supports_index_management=True,
    )

    cloned = driver.clone('tenant_b')

    assert cloned is not driver
    assert isinstance(cloned, OracleDriver)
    assert cloned._database == 'tenant_b'
    assert cloned._supports_index_management is True
    assert cloned._query_runner is query_runner
    assert cloned._close_runner is close_runner


@pytest.mark.asyncio
async def test_close_calls_close_runner():
    close_runner = AsyncMock()
    driver = OracleDriver(query_runner=AsyncMock(), close_runner=close_runner)

    await driver.close()

    close_runner.assert_awaited_once()


@pytest.mark.asyncio
async def test_build_indices_noop_when_disabled():
    query_runner = AsyncMock(return_value=[])
    driver = OracleDriver(query_runner=query_runner, supports_index_management=False)

    await driver.build_indices_and_constraints()

    query_runner.assert_not_called()


@pytest.mark.asyncio
async def test_build_indices_skips_native_mode_without_transform(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OracleDriver(uri='dbhost:1521/service_name', user='scott', password='tiger')
    await driver.build_indices_and_constraints()

    assert fake_oracledb.create_pool_async_calls == []


@pytest.mark.asyncio
async def test_build_indices_runs_queries_when_enabled():
    query_runner = AsyncMock(return_value=[])
    driver = OracleDriver(query_runner=query_runner, supports_index_management=True)

    await driver.build_indices_and_constraints()

    assert query_runner.await_count == 0


@pytest.mark.asyncio
async def test_build_indices_runs_by_default_for_query_runner():
    query_runner = AsyncMock(return_value=[])
    driver = OracleDriver(query_runner=query_runner)

    await driver.build_indices_and_constraints()

    assert query_runner.await_count == 0


@pytest.mark.asyncio
async def test_execute_query_logs_query_runner_mode_when_enabled(caplog):
    query_runner = AsyncMock(return_value=[{'ok': True}])
    driver = OracleDriver(query_runner=query_runner, log_queries=True)

    with caplog.at_level(logging.INFO, logger='graphiti_core.driver.oracle_driver'):
        await driver.execute_query('RETURN $value AS ok', value=True)

    assert any('mode=query_runner' in record.message for record in caplog.records)
    assert any('RETURN $value AS ok' in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_execute_query_logs_native_mode_transformed_query_when_enabled(monkeypatch, caplog):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OracleDriver(
        uri='dbhost:1521/service_name',
        user='scott',
        password='tiger',
        log_queries=True,
    )
    with caplog.at_level(logging.INFO, logger='graphiti_core.driver.oracle_driver'):
        await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')

    assert any('mode=native' in record.message for record in caplog.records)
    assert any('SELECT :uuid AS uuid FROM dual' in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_execute_query_enables_logging_from_env(monkeypatch, caplog):
    monkeypatch.setenv('ORACLE_LOG_QUERIES', 'true')
    query_runner = AsyncMock(return_value=[{'ok': True}])
    driver = OracleDriver(query_runner=query_runner)

    with caplog.at_level(logging.INFO, logger='graphiti_core.driver.oracle_driver'):
        await driver.execute_query('RETURN 1 AS ok')

    assert any('Oracle query mode=' in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_execute_query_blocks_cypher_in_native_rdf_mode(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OracleDriver(
        uri='dbhost:1521/service_name',
        user='scott',
        password='tiger',
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='graphiti',
    )

    with pytest.raises(ValueError, match='cannot execute Cypher directly'):
        await driver.execute_query('MATCH (n) RETURN n')

    assert fake_oracledb.create_pool_async_calls == []


@pytest.mark.asyncio
async def test_build_indices_uses_sem_apis_datatype_indexes_in_rdf_mode():
    async def query_runner_side_effect(query: str, params: dict[str, Any]):
        if 'SEM_DTYPE_INDEX_INFO' in query:
            return [{'index_count': 0}]
        return []

    query_runner = AsyncMock(side_effect=query_runner_side_effect)
    driver = OracleDriver(
        query_runner=query_runner,
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='graphiti',
    )

    await driver.build_indices_and_constraints()

    assert query_runner.await_count == 6
    add_index_calls = [
        call
        for call in query_runner.await_args_list
        if 'sem_apis.add_datatype_index' in call.args[0]
    ]
    assert len(add_index_calls) == 3
    datatype_uris = {call.args[1]['datatype_uri'] for call in add_index_calls}
    assert datatype_uris == {
        'http://www.w3.org/2001/XMLSchema#decimal',
        'http://www.w3.org/2001/XMLSchema#string',
        'http://www.w3.org/2001/XMLSchema#dateTime',
        # 'http://xmlns.oracle.com/rdf/text',
    }
    existence_calls = [
        call for call in query_runner.await_args_list if 'SEM_DTYPE_INDEX_INFO' in call.args[0]
    ]
    assert len(existence_calls) == 3


@pytest.mark.asyncio
async def test_build_indices_skips_existing_sem_apis_datatype_indexes_in_rdf_mode():
    async def query_runner_side_effect(query: str, params: dict[str, Any]):
        if 'SEM_DTYPE_INDEX_INFO' in query:
            return [{'index_count': 1}]
        return []

    query_runner = AsyncMock(side_effect=query_runner_side_effect)
    driver = OracleDriver(
        query_runner=query_runner,
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='graphiti',
    )

    await driver.build_indices_and_constraints()

    assert query_runner.await_count == 3
    assert all('SEM_DTYPE_INDEX_INFO' in call.args[0] for call in query_runner.await_args_list)


def test_driver_exposes_operations_namespaces():
    driver = OracleDriver(query_runner=AsyncMock())

    assert isinstance(driver.entity_node_ops, OracleEntityNodeOperations)
    assert isinstance(driver.episode_node_ops, OracleEpisodeNodeOperations)
    assert isinstance(driver.community_node_ops, OracleCommunityNodeOperations)
    assert isinstance(driver.saga_node_ops, OracleSagaNodeOperations)
    assert isinstance(driver.entity_edge_ops, OracleEntityEdgeOperations)
    assert isinstance(driver.episodic_edge_ops, OracleEpisodicEdgeOperations)
    assert isinstance(driver.community_edge_ops, OracleCommunityEdgeOperations)
    assert isinstance(driver.has_episode_edge_ops, OracleHasEpisodeEdgeOperations)
    assert isinstance(driver.next_episode_edge_ops, OracleNextEpisodeEdgeOperations)
    assert isinstance(driver.search_ops, OracleSearchOperations)
    assert isinstance(driver.graph_ops, OracleGraphMaintenanceOperations)


@pytest.mark.asyncio
async def test_retrieve_episodes_uses_oracle_episode_ops():
    driver = OracleDriver(query_runner=AsyncMock())
    retrieve_mock = AsyncMock(return_value=['newest', 'older'])
    driver.episode_node_ops.retrieve_episodes = retrieve_mock  # type: ignore[method-assign]

    reference_time = datetime.now()
    episodes = await retrieve_episodes_maintenance(
        driver=driver,
        reference_time=reference_time,
        last_n=2,
        group_ids=['group-1'],
        source=EpisodeType.message,
        saga='saga-1',
    )

    retrieve_mock.assert_awaited_once_with(
        driver,
        reference_time,
        2,
        ['group-1'],
        EpisodeType.message.name,
        'saga-1',
    )
    assert episodes == ['older', 'newest']


@pytest.mark.asyncio
async def test_oracle_episode_ops_retrieve_episodes_uses_sem_match_in_rdf_mode():
    rdf_rows = [
        {
            'uuid': 'episode-1',
            'name': 'Episode 1',
            'group_id': 'group-1',
            'created_at': '2026-04-06T10:00:00+00:00',
            'source': 'message',
            'source_description': 'conversation',
            'content': 'Hello world',
            'valid_at': '2026-04-06T10:00:00+00:00',
            'entity_edges': '["edge-1","edge-2"]',
        }
    ]
    query_runner = AsyncMock(return_value=rdf_rows)
    driver = OracleDriver(
        query_runner=query_runner,
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='GRAPHITI',
    )

    episodes = await driver.episode_node_ops.retrieve_episodes(
        driver,
        reference_time=datetime.now(timezone.utc),
        last_n=5,
        group_ids=['group-1'],
        source='message',
        saga='saga-1',
    )

    assert len(episodes) == 1
    assert episodes[0].uuid == 'episode-1'
    assert episodes[0].entity_edges == ['edge-1', 'edge-2']

    query_runner.assert_awaited_once()
    await_args = query_runner.await_args
    assert await_args is not None
    called_query = await_args.args[0]
    called_params = await_args.args[1]
    assert 'SEM_MATCH' in called_query
    assert 'MATCH (e:Episodic)' not in called_query
    assert 'HAS_EPISODE' in called_query
    assert called_params == {}


@pytest.mark.asyncio
async def test_oracle_entity_node_get_by_uuid_uses_sem_match_in_rdf_mode():
    query_runner = AsyncMock(
        return_value=[
            {
                'uuid': 'entity-1',
                'name': 'Entity 1',
                'group_id': 'group-1',
                'created_at': '2026-04-06T10:00:00+00:00',
                'summary': 'entity summary',
                'labels': '["Entity"]',
                'attributes': '{"foo":"bar"}',
            }
        ]
    )
    driver = OracleDriver(
        query_runner=query_runner,
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='GRAPHITI',
    )

    node = await driver.entity_node_ops.get_by_uuid(driver, 'entity-1')
    assert node.uuid == 'entity-1'

    await_args = query_runner.await_args
    assert await_args is not None
    called_query = await_args.args[0]
    assert 'SEM_MATCH' in called_query
    assert 'MATCH (n:Entity' not in called_query


@pytest.mark.asyncio
async def test_oracle_community_node_get_by_uuid_uses_sem_match_in_rdf_mode():
    query_runner = AsyncMock(
        return_value=[
            {
                'uuid': 'community-1',
                'name': 'Community 1',
                'group_id': 'group-1',
                'created_at': '2026-04-06T10:00:00+00:00',
                'name_embedding': '[0.1, 0.2]',
                'summary': 'community summary',
            }
        ]
    )
    driver = OracleDriver(
        query_runner=query_runner,
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='GRAPHITI',
    )

    node = await driver.community_node_ops.get_by_uuid(driver, 'community-1')
    assert node.uuid == 'community-1'

    await_args = query_runner.await_args
    assert await_args is not None
    called_query = await_args.args[0]
    assert 'SEM_MATCH' in called_query
    assert 'MATCH (c:Community' not in called_query


@pytest.mark.asyncio
async def test_oracle_saga_node_get_by_uuid_uses_sem_match_in_rdf_mode():
    query_runner = AsyncMock(
        return_value=[
            {
                'uuid': 'saga-1',
                'name': 'Saga 1',
                'group_id': 'group-1',
                'created_at': '2026-04-06T10:00:00+00:00',
            }
        ]
    )
    driver = OracleDriver(
        query_runner=query_runner,
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='GRAPHITI',
    )

    node = await driver.saga_node_ops.get_by_uuid(driver, 'saga-1')
    assert node.uuid == 'saga-1'

    await_args = query_runner.await_args
    assert await_args is not None
    called_query = await_args.args[0]
    assert 'SEM_MATCH' in called_query
    assert 'MATCH (s:Saga' not in called_query


@pytest.mark.asyncio
async def test_oracle_entity_edge_get_by_uuid_uses_sem_match_in_rdf_mode():
    query_runner = AsyncMock(
        return_value=[
            {
                'uuid': 'edge-1',
                'source_node_uuid': 'entity-1',
                'target_node_uuid': 'entity-2',
                'group_id': 'group-1',
                'created_at': '2026-04-06T10:00:00+00:00',
                'name': 'RELATES_TO',
                'fact': 'fact text',
                'episodes': '["episode-1"]',
                'expired_at': None,
                'valid_at': '2026-04-06T10:00:00+00:00',
                'invalid_at': None,
                'attributes': '{"strength":"high"}',
            }
        ]
    )
    driver = OracleDriver(
        query_runner=query_runner,
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='GRAPHITI',
    )

    edge = await driver.entity_edge_ops.get_by_uuid(driver, 'edge-1')
    assert edge.uuid == 'edge-1'

    await_args = query_runner.await_args
    assert await_args is not None
    called_query = await_args.args[0]
    assert 'SEM_MATCH' in called_query
    assert 'MATCH (n:Entity)-[e:RELATES_TO' not in called_query


@pytest.mark.asyncio
async def test_oracle_episodic_edge_get_by_uuid_uses_sem_match_in_rdf_mode():
    query_runner = AsyncMock(
        return_value=[
            {
                'uuid': 'mention-1',
                'group_id': 'group-1',
                'source_node_uuid': 'episode-1',
                'target_node_uuid': 'entity-1',
                'created_at': '2026-04-06T10:00:00+00:00',
            }
        ]
    )
    driver = OracleDriver(
        query_runner=query_runner,
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='GRAPHITI',
    )

    edge = await driver.episodic_edge_ops.get_by_uuid(driver, 'mention-1')
    assert edge.uuid == 'mention-1'

    await_args = query_runner.await_args
    assert await_args is not None
    called_query = await_args.args[0]
    assert 'SEM_MATCH' in called_query
    assert 'MATCH (n:Episodic)-[e:MENTIONS' not in called_query


@pytest.mark.asyncio
async def test_oracle_community_edge_get_by_uuid_uses_sem_match_in_rdf_mode():
    query_runner = AsyncMock(
        return_value=[
            {
                'uuid': 'member-1',
                'group_id': 'group-1',
                'source_node_uuid': 'community-1',
                'target_node_uuid': 'entity-1',
                'created_at': '2026-04-06T10:00:00+00:00',
            }
        ]
    )
    driver = OracleDriver(
        query_runner=query_runner,
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='GRAPHITI',
    )

    edge = await driver.community_edge_ops.get_by_uuid(driver, 'member-1')
    assert edge.uuid == 'member-1'

    await_args = query_runner.await_args
    assert await_args is not None
    called_query = await_args.args[0]
    assert 'SEM_MATCH' in called_query
    assert 'MATCH (n:Community)-[e:HAS_MEMBER' not in called_query


@pytest.mark.asyncio
async def test_oracle_episode_mentions_reranker_uses_sem_match_in_rdf_mode():
    query_runner = AsyncMock(
        side_effect=[
            [{'uuid': 'entity-1', 'score': 3}],
            [
                {
                    'uuid': 'entity-1',
                    'name': 'Acme',
                    'group_id': 'group-1',
                    'created_at': '2026-04-06T10:00:00+00:00',
                    'summary': 'entity summary',
                    'labels': '["Entity"]',
                    'attributes': '{}',
                }
            ],
        ]
    )
    driver = OracleDriver(
        query_runner=query_runner,
        use_rdf=True,
        rdf_network_owner='RDFUSER',
        rdf_network_name='NET1',
        rdf_graph_name='GRAPHITI',
    )

    nodes = await driver.search_ops.episode_mentions_reranker(driver, ['entity-1'])

    assert len(nodes) == 1
    assert nodes[0].uuid == 'entity-1'
    assert query_runner.await_count == 2
    for await_args in query_runner.await_args_list:
        called_query = await_args.args[0]
        assert 'SEM_MATCH' in called_query
        assert 'MATCH (episode:Episodic)-[r:MENTIONS]->(n:Entity' not in called_query


@pytest.mark.integration
@pytest.mark.asyncio
async def test_oracle_driver_real_connection_smoke():
    if oracle_driver_module.oracledb is None:
        pytest.skip('oracledb is not installed')

    uri = os.getenv('ORACLE_URI')
    user = os.getenv('ORACLE_USER')
    password = os.getenv('ORACLE_PASSWORD')
    if not (uri and user and password):
        pytest.skip('set ORACLE_URI, ORACLE_USER, and ORACLE_PASSWORD to run this test')

    driver = OracleDriver(uri=uri, user=user, password=password)
    try:
        records, _, _ = await driver.execute_query('SELECT 1 AS value FROM dual')
        assert records
        assert any(value == 1 for value in records[0].values())
    finally:
        await driver.close()
