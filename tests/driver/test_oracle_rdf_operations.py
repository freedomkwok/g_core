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

from datetime import datetime, timezone
from typing import Any

import pytest

from graphiti_core.driver.oracle.operations.entity_edge_ops import OracleEntityEdgeOperations
from graphiti_core.driver.oracle.operations.entity_node_ops import OracleEntityNodeOperations
from graphiti_core.driver.oracle.operations.episodic_edge_ops import OracleEpisodicEdgeOperations
from graphiti_core.driver.oracle.operations.has_episode_edge_ops import (
    OracleHasEpisodeEdgeOperations,
)
from graphiti_core.driver.oracle.operations.next_episode_edge_ops import (
    OracleNextEpisodeEdgeOperations,
)
from graphiti_core.edges import EntityEdge, EpisodicEdge, HasEpisodeEdge
from graphiti_core.nodes import EntityNode


def _set_rdf_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('ORACLE_USE_RDF', 'true')
    monkeypatch.setenv('ORACLE_USER', 'rdfuser')
    monkeypatch.setenv('ORACLE_RDF_NETWORK_NAME', 'NET1')
    monkeypatch.setenv('ORACLE_RDF_GRAPH_NAME', 'graphiti')


class _CaptureExecutor:
    def __init__(self):
        self.rdf_enabled = True
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute_query(self, cypher_query_: str, **kwargs: Any):
        self.calls.append((cypher_query_, kwargs))
        return [], None, None


class _CaptureTx:
    def __init__(self):
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def run(self, query: str, **kwargs: Any):
        self.calls.append((query, kwargs))
        return [], None, None


@pytest.mark.asyncio
async def test_entity_node_save_rdf_emits_expected_sparql(monkeypatch):
    _set_rdf_env(monkeypatch)
    executor = _CaptureExecutor()
    ops = OracleEntityNodeOperations()
    node = EntityNode(
        uuid='entity-1',
        name='Alice',
        group_id='group-a',
        labels=['Person'],
        created_at=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
        summary='Person summary',
        name_embedding=[0.1, 0.2],
        attributes={'role': 'engineer'},
    )

    await ops.save(executor, node)

    assert len(executor.calls) == 1
    query, params = executor.calls[0]
    assert 'sem_apis.update_rdf_graph' in query
    assert params['network_owner'] == 'RDFUSER'
    assert params['network_name'] == 'NET1'
    assert params['graph_name'] == 'graphiti'
    update = params['update_query']
    assert '<urn:graphiti:node:entity:entity-1>' in update
    assert '<urn:graphiti:pred:name> "Alice"' in update
    assert '<urn:graphiti:pred:group_id> "group-a"' in update
    assert '<urn:graphiti:pred:labels>' in update
    assert 'Person' in update
    assert 'Entity' in update
    assert 'role' in update
    assert 'engineer' in update


@pytest.mark.asyncio
async def test_entity_node_save_rdf_uses_tx_when_provided(monkeypatch):
    _set_rdf_env(monkeypatch)
    executor = _CaptureExecutor()
    tx = _CaptureTx()
    ops = OracleEntityNodeOperations()
    node = EntityNode(
        uuid='entity-2',
        name='Bob',
        group_id='group-b',
        labels=['Person'],
        created_at=datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc),
    )

    await ops.save(executor, node, tx=tx)

    assert len(executor.calls) == 0
    assert len(tx.calls) == 1
    query, params = tx.calls[0]
    assert 'sem_apis.update_rdf_graph' in query
    assert '<urn:graphiti:node:entity:entity-2>' in params['update_query']


@pytest.mark.asyncio
async def test_entity_node_save_bulk_rdf_emits_all_subjects(monkeypatch):
    _set_rdf_env(monkeypatch)
    executor = _CaptureExecutor()
    ops = OracleEntityNodeOperations()
    nodes = [
        EntityNode(
            uuid='entity-bulk-1',
            name='Alice',
            group_id='group-a',
            labels=['Person'],
            created_at=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
        ),
        EntityNode(
            uuid='entity-bulk-2',
            name='Bob',
            group_id='group-a',
            labels=['Person'],
            created_at=datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc),
        ),
    ]

    await ops.save_bulk(executor, nodes)

    assert len(executor.calls) == 1
    _, params = executor.calls[0]
    update = params['update_query']
    assert '<urn:graphiti:node:entity:entity-bulk-1>' in update
    assert '<urn:graphiti:node:entity:entity-bulk-2>' in update


@pytest.mark.asyncio
async def test_entity_node_delete_by_group_id_rdf_emits_delete_where(monkeypatch):
    _set_rdf_env(monkeypatch)
    executor = _CaptureExecutor()
    ops = OracleEntityNodeOperations()

    await ops.delete_by_group_id(executor, 'group-delete')

    assert len(executor.calls) == 1
    _, params = executor.calls[0]
    update = params['update_query']
    assert 'DELETE WHERE' in update
    assert '?s <urn:graphiti:pred:group_id> "group-delete"' in update
    assert '?s ?p ?o' in update


@pytest.mark.asyncio
async def test_entity_edge_save_rdf_emits_temporal_and_link_fields(monkeypatch):
    _set_rdf_env(monkeypatch)
    executor = _CaptureExecutor()
    ops = OracleEntityEdgeOperations()
    edge = EntityEdge(
        uuid='edge-1',
        group_id='group-a',
        source_node_uuid='entity-1',
        target_node_uuid='entity-2',
        created_at=datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc),
        name='works_with',
        fact='Alice works with Bob',
        episodes=['ep-1'],
        valid_at=datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc),
        invalid_at=datetime(2026, 1, 3, 10, 0, tzinfo=timezone.utc),
        expired_at=datetime(2026, 1, 4, 10, 0, tzinfo=timezone.utc),
        attributes={'confidence': 0.92},
    )

    await ops.save(executor, edge)

    assert len(executor.calls) == 1
    _, params = executor.calls[0]
    update = params['update_query']
    assert '<urn:graphiti:edge:relates_to:edge-1>' in update
    assert '<urn:graphiti:pred:source_node_uuid> "entity-1"' in update
    assert '<urn:graphiti:pred:target_node_uuid> "entity-2"' in update
    assert '<urn:graphiti:pred:valid_at>' in update
    assert '<urn:graphiti:pred:invalid_at>' in update
    assert '<urn:graphiti:pred:expired_at>' in update
    assert '<urn:graphiti:pred:attributes>' in update


@pytest.mark.asyncio
async def test_entity_edge_save_bulk_rdf_emits_all_subjects(monkeypatch):
    _set_rdf_env(monkeypatch)
    executor = _CaptureExecutor()
    ops = OracleEntityEdgeOperations()
    edges = [
        EntityEdge(
            uuid='edge-bulk-1',
            group_id='group-a',
            source_node_uuid='entity-1',
            target_node_uuid='entity-2',
            created_at=datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc),
            name='works_with',
            fact='Alice works with Bob',
            fact_embedding=[0.1, 0.2],
            episodes=['ep-1'],
        ),
        EntityEdge(
            uuid='edge-bulk-2',
            group_id='group-a',
            source_node_uuid='entity-2',
            target_node_uuid='entity-3',
            created_at=datetime(2026, 1, 2, 11, 0, tzinfo=timezone.utc),
            name='knows',
            fact='Bob knows Carol',
            fact_embedding=[0.2, 0.3],
            episodes=['ep-2'],
        ),
    ]

    await ops.save_bulk(executor, edges)

    assert len(executor.calls) == 1
    _, params = executor.calls[0]
    update = params['update_query']
    assert '<urn:graphiti:edge:relates_to:edge-bulk-1>' in update
    assert '<urn:graphiti:edge:relates_to:edge-bulk-2>' in update


@pytest.mark.asyncio
async def test_episodic_edge_save_bulk_rdf_emits_all_subjects(monkeypatch):
    _set_rdf_env(monkeypatch)
    executor = _CaptureExecutor()
    ops = OracleEpisodicEdgeOperations()
    edges = [
        EpisodicEdge(
            uuid='mentions-bulk-1',
            group_id='group-a',
            source_node_uuid='episode-1',
            target_node_uuid='entity-1',
            created_at=datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc),
        ),
        EpisodicEdge(
            uuid='mentions-bulk-2',
            group_id='group-a',
            source_node_uuid='episode-2',
            target_node_uuid='entity-2',
            created_at=datetime(2026, 1, 2, 13, 0, tzinfo=timezone.utc),
        ),
    ]

    await ops.save_bulk(executor, edges)

    assert len(executor.calls) == 1
    _, params = executor.calls[0]
    update = params['update_query']
    assert '<urn:graphiti:edge:mentions:mentions-bulk-1>' in update
    assert '<urn:graphiti:edge:mentions:mentions-bulk-2>' in update


@pytest.mark.asyncio
async def test_has_episode_edge_save_rdf_emits_expected_subject(monkeypatch):
    _set_rdf_env(monkeypatch)
    executor = _CaptureExecutor()
    ops = OracleHasEpisodeEdgeOperations()
    edge = HasEpisodeEdge(
        uuid='has-1',
        group_id='group-a',
        source_node_uuid='saga-1',
        target_node_uuid='episode-1',
        created_at=datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc),
    )

    await ops.save(executor, edge)

    assert len(executor.calls) == 1
    _, params = executor.calls[0]
    update = params['update_query']
    assert '<urn:graphiti:edge:has_episode:has-1>' in update
    assert '<urn:graphiti:pred:type> "HAS_EPISODE"' in update
    assert '<urn:graphiti:pred:source_node_uuid> "saga-1"' in update
    assert '<urn:graphiti:pred:target_node_uuid> "episode-1"' in update


@pytest.mark.asyncio
async def test_next_episode_edge_delete_by_uuids_rdf_emits_each_subject(monkeypatch):
    _set_rdf_env(monkeypatch)
    executor = _CaptureExecutor()
    ops = OracleNextEpisodeEdgeOperations()

    await ops.delete_by_uuids(executor, ['next-1', 'next-2'])

    assert len(executor.calls) == 1
    _, params = executor.calls[0]
    update = params['update_query']
    assert '<urn:graphiti:edge:next_episode:next-1>' in update
    assert '<urn:graphiti:edge:next_episode:next-2>' in update
    assert update.count('DELETE WHERE') == 2
