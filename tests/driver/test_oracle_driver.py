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

import os
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


class _FakeOracleDb:
    def __init__(self, connection: _FakeConnection):
        self._connection = connection
        self.connect_async_calls: list[dict] = []

    async def connect_async(self, **kwargs):
        self.connect_async_calls.append(kwargs)
        return self._connection


class _FailingOracleDb:
    async def connect_async(self, **kwargs):
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
    assert fake_oracledb.connect_async_calls == []

    records, keys, summary = await driver.execute_query(
        'SELECT $uuid AS uuid FROM dual',
        uuid='abc',
    )

    assert fake_oracledb.connect_async_calls == [
        {'user': 'scott', 'password': 'tiger', 'dsn': 'dbhost:1521/service_name'}
    ]
    assert fake_cursor.executed == [('SELECT :uuid AS uuid FROM dual', {'uuid': 'abc'})]
    assert records == [{'uuid': 'abc'}]
    assert keys == ['uuid']
    assert summary is None

    await driver.close()
    assert fake_connection.closed


@pytest.mark.asyncio
async def test_execute_query_reuses_single_native_connection(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_connection = _FakeConnection(fake_cursor)
    fake_oracledb = _FakeOracleDb(fake_connection)
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OracleDriver(uri='dbhost:1521/service_name', user='scott', password='tiger')

    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='abc')
    await driver.execute_query('SELECT $uuid AS uuid FROM dual', uuid='def')

    assert len(fake_oracledb.connect_async_calls) == 1
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

    assert len(fake_oracledb.connect_async_calls) == 2


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

    assert fake_oracledb.connect_async_calls == [
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

    assert fake_oracledb.connect_async_calls == [{'user': 'scott', 'password': 'tiger', 'dsn': dsn}]


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

    assert fake_oracledb.connect_async_calls == [
        {'user': 'env_user', 'password': 'env_pass', 'dsn': 'envhost:1522/envservice'}
    ]


@pytest.mark.asyncio
async def test_execute_query_passes_connect_kwargs_to_connect_async(monkeypatch):
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

    assert fake_oracledb.connect_async_calls == [
        {'user': 'scott', 'password': 'tiger', 'dsn': 'dbhost:1521/service_name', 'events': True}
    ]


@pytest.mark.asyncio
async def test_execute_query_surfaces_connect_async_errors(monkeypatch):
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
async def test_build_indices_runs_queries_when_enabled():
    query_runner = AsyncMock(return_value=[])
    driver = OracleDriver(query_runner=query_runner, supports_index_management=True)

    await driver.build_indices_and_constraints()

    assert query_runner.await_count > 0


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
