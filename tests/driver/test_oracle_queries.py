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

from typing import Any, cast

import pytest

from graphiti_core.driver.driver import GraphDriver, GraphProvider
from graphiti_core.graph_queries import get_fulltext_indices, get_range_indices
from graphiti_core.models.edges.edge_db_queries import (
    get_community_edge_save_query,
    get_entity_edge_save_bulk_query,
    get_entity_edge_save_query,
)
from graphiti_core.models.nodes.node_db_queries import (
    get_community_node_save_query,
    get_entity_node_save_bulk_query,
    get_entity_node_save_query,
)
from graphiti_core.nodes import EntityNode


def test_oracle_entity_node_query_avoids_neo4j_vector_procedure():
    query = get_entity_node_save_query(GraphProvider.ORACLE, 'Entity:Person')

    assert 'db.create.setNodeVectorProperty' not in query
    assert 'SET n = $entity_data' in query


def test_oracle_entity_node_bulk_query_avoids_neo4j_vector_procedure():
    query = get_entity_node_save_bulk_query(
        GraphProvider.ORACLE,
        [
            {
                'uuid': 'entity-1',
                'name': 'Alice',
                'group_id': 'group-1',
                'labels': ['Entity', 'Person'],
                'attributes': {'team': 'alpha'},
                'created_at': '2026-01-01T00:00:00+00:00',
                'summary': 'ignored',
                'name_embedding': [0.1, 0.2],
            }
        ],
    )

    assert isinstance(query, str)
    assert 'db.create.setNodeVectorProperty' not in query
    assert 'DELETE WHERE' in query
    assert 'INSERT DATA' in query
    assert '<gti:pred:name>' in query
    assert '<gti:pred:group_id>' in query
    assert '<gti:pred:labels>' in query
    assert '<gti:pred:attributes>' in query
    assert '<gti:pred:created_at>' in query
    assert '<gti:pred:summary>' not in query
    assert '<gti:pred:name_embedding>' not in query


def test_oracle_pg_entity_node_query_requires_oracle_pg_driver_ops():
    oracle_pg_provider = getattr(GraphProvider, 'ORACLE_PG', None)
    if oracle_pg_provider is None:
        pytest.skip('GraphProvider.ORACLE_PG is unavailable in this runtime')

    with pytest.raises(NotImplementedError, match='OraclePGDriver entity_node_ops.save'):
        get_entity_node_save_query(oracle_pg_provider, 'Entity:Person')

    with pytest.raises(NotImplementedError, match='OraclePGDriver entity_node_ops.save_bulk'):
        get_entity_node_save_bulk_query(oracle_pg_provider, [])


def test_oracle_community_node_query_avoids_neo4j_vector_procedure():
    query = get_community_node_save_query(GraphProvider.ORACLE)

    assert 'db.create.setNodeVectorProperty' not in query
    assert 'name_embedding: $name_embedding' in query


def test_oracle_entity_edge_query_avoids_neo4j_vector_procedure():
    query = get_entity_edge_save_query(GraphProvider.ORACLE)

    assert 'db.create.setRelationshipVectorProperty' not in query
    assert 'SET e = $edge_data' in query


def test_oracle_entity_edge_bulk_query_avoids_neo4j_vector_procedure():
    query = get_entity_edge_save_bulk_query(GraphProvider.ORACLE)

    assert 'db.create.setRelationshipVectorProperty' not in query
    assert 'UNWIND $entity_edges AS edge' in query


def test_oracle_pg_entity_edge_query_requires_oracle_pg_driver_ops():
    oracle_pg_provider = getattr(GraphProvider, 'ORACLE_PG', None)
    if oracle_pg_provider is None:
        pytest.skip('GraphProvider.ORACLE_PG is unavailable in this runtime')

    with pytest.raises(NotImplementedError, match='OraclePGDriver entity_edge_ops.save'):
        get_entity_edge_save_query(oracle_pg_provider)

    with pytest.raises(NotImplementedError, match='OraclePGDriver entity_edge_ops.save_bulk'):
        get_entity_edge_save_bulk_query(oracle_pg_provider)


def test_oracle_community_edge_query_uses_portable_label_filter():
    query = get_community_edge_save_query(GraphProvider.ORACLE)

    assert 'WHERE node:Entity OR node:Community' in query
    assert 'MERGE (community)-[r:HAS_MEMBER {uuid: $uuid}]->(node)' in query


class _OracleDeleteDriverStub:
    provider = GraphProvider.ORACLE
    graph_operations_interface = None

    def __init__(self):
        self.queries: list[str] = []

    async def execute_query(self, query: str, **kwargs: Any):
        self.queries.append(query)
        return [], None, None


@pytest.mark.asyncio
async def test_oracle_node_delete_by_uuids_avoids_in_transactions():
    driver = _OracleDeleteDriverStub()

    await EntityNode.delete_by_uuids(cast(GraphDriver, driver), ['abc'])

    assert len(driver.queries) == 3
    assert all('IN TRANSACTIONS' not in query for query in driver.queries)


def test_oracle_graph_queries_do_not_emit_generic_cypher_index_ddl():
    assert get_range_indices(GraphProvider.ORACLE) == []
    assert get_fulltext_indices(GraphProvider.ORACLE) == []
