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

from typing import Any

import pytest

from graphiti_core.driver.oracle.rdf_utils import (
    execute_sparql_update,
    get_rdf_namespace_prefix_for_executor,
    get_rdf_table_name,
    rdf_mode_for_executor,
    sanitize_oracle_table_base,
    sanitize_rdf_graph_name,
)
from graphiti_core.driver.query_executor import QueryExecutor


class _ExecutorStub(QueryExecutor):
    def __init__(
        self,
        rdf_enabled: bool | None = None,
        rdf_network_owner: str | None = None,
        rdf_network_name: str | None = None,
        rdf_graph_name: str | None = None,
        rdf_namespace_prefix: str | None = None,
    ):
        self.rdf_enabled = rdf_enabled
        self.rdf_network_owner = rdf_network_owner
        self.rdf_network_name = rdf_network_name
        self.rdf_graph_name = rdf_graph_name
        self.rdf_namespace_prefix = rdf_namespace_prefix
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute_query(self, cypher_query_: str, **kwargs: Any):
        self.calls.append((cypher_query_, kwargs))
        return [], None, None


@pytest.mark.asyncio
async def test_execute_sparql_update_uses_plsql_wrapper(monkeypatch):
    monkeypatch.setenv('ORACLE_USER', 'rdfuser')
    monkeypatch.setenv('ORACLE_RDF_NETWORK_NAME', 'NET1')
    monkeypatch.setenv('ORACLE_RDF_GRAPH_NAME', 'graphiti')
    executor = _ExecutorStub()

    await execute_sparql_update(executor, 'INSERT DATA { <s> <p> "o" . }')

    assert len(executor.calls) == 1
    query, params = executor.calls[0]
    assert 'BEGIN' in query
    assert 'sem_apis.update_rdf_graph' in query
    assert params['graph_name'] == 'graphiti'
    assert params['network_owner'] == 'RDFUSER'
    assert params['network_name'] == 'NET1'


@pytest.mark.asyncio
async def test_execute_sparql_update_prefers_executor_rdf_identifiers(monkeypatch):
    monkeypatch.delenv('ORACLE_RDF_NETWORK_OWNER', raising=False)
    monkeypatch.delenv('ORACLE_USER', raising=False)
    monkeypatch.setenv('ORACLE_RDF_NETWORK_NAME', 'NET1')
    monkeypatch.setenv('ORACLE_RDF_GRAPH_NAME', 'graphiti')
    executor = _ExecutorStub(
        rdf_network_owner='passed_owner',
        rdf_network_name='custom_net',
        rdf_graph_name='custom_graph',
    )

    await execute_sparql_update(executor, 'INSERT DATA { <s> <p> "o" . }')

    assert len(executor.calls) == 1
    _, params = executor.calls[0]
    assert params['network_owner'] == 'PASSED_OWNER'
    assert params['network_name'] == 'CUSTOM_NET'
    assert params['graph_name'] == 'custom_graph'


def test_get_rdf_table_name_uses_network_convention(monkeypatch):
    monkeypatch.setenv('ORACLE_USER', 'rdfuser')
    monkeypatch.setenv('ORACLE_RDF_NETWORK_NAME', 'NET1')
    monkeypatch.setenv('ORACLE_RDF_GRAPH_NAME', 'graphiti')

    table_name = get_rdf_table_name()

    assert table_name == 'RDFUSER.NET1#RDFT_GRAPHITI'


def test_rdf_mode_for_executor_prefers_driver_flag(monkeypatch):
    monkeypatch.setenv('ORACLE_USE_RDF', 'false')

    assert rdf_mode_for_executor(_ExecutorStub(rdf_enabled=True)) is True
    assert rdf_mode_for_executor(_ExecutorStub(rdf_enabled=False)) is False


@pytest.mark.asyncio
async def test_execute_sparql_update_rewrites_graphiti_prefix(monkeypatch):
    monkeypatch.setenv('ORACLE_USER', 'rdfuser')
    monkeypatch.setenv('ORACLE_RDF_NETWORK_NAME', 'NET1')
    executor = _ExecutorStub(rdf_graph_name='skill graph')

    await execute_sparql_update(executor, 'INSERT DATA { <s> <urn:graphiti:pred:type> "Entity" . }')

    assert len(executor.calls) == 1
    _, params = executor.calls[0]
    assert 'gti:SKILL_GRAPH:pred:type' in params['update_query']


def test_namespace_prefix_defaults_to_sanitized_table_base():
    executor = _ExecutorStub(rdf_graph_name='my-skill graph')
    assert get_rdf_namespace_prefix_for_executor(executor) == 'gti:MY_SKILL_GRAPH:'


def test_sanitize_oracle_table_base_replaces_spaces_and_hyphens():
    assert sanitize_oracle_table_base('my graph-name') == 'MY_GRAPH_NAME'


def test_sanitize_rdf_graph_name_replaces_invalid_characters():
    assert sanitize_rdf_graph_name('my graph-name@2026') == 'my_graph_name_2026'


@pytest.mark.asyncio
async def test_execute_sparql_update_sanitizes_rdf_graph_name(monkeypatch):
    monkeypatch.setenv('ORACLE_USER', 'rdfuser')
    monkeypatch.setenv('ORACLE_RDF_NETWORK_NAME', 'NET1')
    executor = _ExecutorStub(rdf_graph_name='skill graph@beta')

    await execute_sparql_update(executor, 'INSERT DATA { <s> <p> "o" . }')

    assert len(executor.calls) == 1
    _, params = executor.calls[0]
    assert params['graph_name'] == 'skill_graph_beta'
