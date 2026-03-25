"""
Tests for OraclePGDriver table-backed behavior.
"""

from __future__ import annotations

from typing import Any

import pytest

import graphiti_core.driver.oracle_driver as oracle_driver_module
from graphiti_core.driver.driver import GraphProvider
from graphiti_core.driver.oracle_pg_driver import OraclePGDriver


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


@pytest.mark.asyncio
async def test_oracle_pg_driver_bootstraps_prefixed_tables(monkeypatch: pytest.MonkeyPatch):
    query_calls: list[tuple[str, dict[str, Any] | None]] = []
    fake_oracledb = _OracleDb(_Connection(query_calls))
    monkeypatch.setattr(oracle_driver_module, 'oracledb', fake_oracledb)

    driver = OraclePGDriver(
        uri='dbhost:1521/service_name',
        user='scott',
        password='tiger',
        graph_id='my graph-01',
    )

    assert driver.provider == GraphProvider.ORACLE_PG
    assert driver.graph_id == 'MY_GRAPH_01'
    assert driver.table_name('entity_nodes') == 'MY_GRAPH_01_ENTITY_NODES'

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
