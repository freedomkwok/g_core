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
from typing import Any

from graphiti_core.driver.driver import GraphProvider
from graphiti_core.driver.operations.graph_ops import GraphMaintenanceOperations
from graphiti_core.driver.operations.graph_utils import Neighbor, label_propagation
from graphiti_core.driver.oracle.rdf_utils import (
    build_delete_by_property_update,
    execute_sem_match_select,
    parse_json_dict_literal,
    parse_json_list_literal,
    execute_sparql_update,
    rdf_mode_for_executor,
    sparql_string_literal,
)
from graphiti_core.driver.query_executor import QueryExecutor
from graphiti_core.driver.record_parsers import community_node_from_record, entity_node_from_record
from graphiti_core.graph_queries import get_fulltext_indices, get_range_indices
from graphiti_core.helpers import semaphore_gather
from graphiti_core.nodes import CommunityNode, EntityNode, EpisodicNode

logger = logging.getLogger(__name__)
STRICT_RDF_ONLY_ERROR = (
    'Oracle strict RDF mode requires ORACLE_USE_RDF=true and RDF/SPARQL operations only.'
)


def _normalize_entity_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['labels'] = [str(value) for value in parse_json_list_literal(record.get('labels'))]
    normalized['attributes'] = parse_json_dict_literal(record.get('attributes'))
    normalized.setdefault('summary', '')
    return normalized


def _normalize_community_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized.setdefault('summary', '')
    normalized.setdefault('name_embedding', None)
    return normalized


class OracleGraphMaintenanceOperations(GraphMaintenanceOperations):
    async def clear_data(
        self,
        executor: QueryExecutor,
        group_ids: list[str] | None = None,
    ) -> None:
        if rdf_mode_for_executor(executor):
            if group_ids is None:
                await execute_sparql_update(executor, 'CLEAR DEFAULT')
            else:
                updates = [build_delete_by_property_update('group_id', group_id) for group_id in group_ids]
                if updates:
                    await execute_sparql_update(executor, '; '.join(updates))
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def build_indices_and_constraints(
        self,
        executor: QueryExecutor,
        delete_existing: bool = False,
    ) -> None:
        if rdf_mode_for_executor(executor):
            # RDF datatype indexes are managed by OracleDriver in RDF mode.
            return

        if delete_existing:
            await self.delete_all_indexes(executor)

        range_indices = get_range_indices(GraphProvider.ORACLE)
        fulltext_indices = get_fulltext_indices(GraphProvider.ORACLE)
        index_queries = range_indices + fulltext_indices

        await semaphore_gather(*[executor.execute_query(q) for q in index_queries])

    async def delete_all_indexes(
        self,
        executor: QueryExecutor,
    ) -> None:
        # Oracle index lifecycle is adapter-specific; safe no-op by default.
        return

    async def get_community_clusters(
        self,
        executor: QueryExecutor,
        group_ids: list[str] | None = None,
    ) -> list[Any]:
        community_clusters: list[list[EntityNode]] = []
        if rdf_mode_for_executor(executor):
            if group_ids is None:
                group_records = await execute_sem_match_select(
                    executor,
                    """
                    SELECT DISTINCT ?group_id
                    WHERE {
                        ?entity <gti:pred:type> "Entity" .
                        ?entity <gti:pred:group_id> ?group_id .
                    }
                    """,
                    ['group_id'],
                )
                group_ids = [record['group_id'] for record in group_records]

            resolved_group_ids = group_ids or []
            for group_id in resolved_group_ids:
                projection = {}
                node_records = await execute_sem_match_select(
                    executor,
                    f"""
                    SELECT ?uuid ?name ?group_id ?created_at ?summary ?labels ?attributes
                    WHERE {{
                        ?entity <gti:pred:type> "Entity" .
                        ?entity <gti:pred:uuid> ?uuid .
                        ?entity <gti:pred:name> ?name .
                        ?entity <gti:pred:group_id> ?group_id .
                        ?entity <gti:pred:created_at> ?created_at .
                        OPTIONAL {{ ?entity <gti:pred:summary> ?summary . }}
                        OPTIONAL {{ ?entity <gti:pred:labels> ?labels . }}
                        OPTIONAL {{ ?entity <gti:pred:attributes> ?attributes . }}
                        FILTER (?group_id = {sparql_string_literal(group_id)})
                    }}
                    """,
                    ['uuid', 'name', 'group_id', 'created_at', 'summary', 'labels', 'attributes'],
                )
                nodes = [entity_node_from_record(_normalize_entity_record(record)) for record in node_records]

                for node in nodes:
                    neighbor_records = await execute_sem_match_select(
                        executor,
                        f"""
                        SELECT ?uuid (COUNT(?edge) AS ?count)
                        WHERE {{
                            ?edge <gti:pred:type> "RELATES_TO" .
                            ?edge <gti:pred:group_id> {sparql_string_literal(group_id)} .
                            ?edge <gti:pred:source_node_uuid> ?source .
                            ?edge <gti:pred:target_node_uuid> ?target .
                            FILTER (
                                ?source = {sparql_string_literal(node.uuid)}
                                || ?target = {sparql_string_literal(node.uuid)}
                            )
                            BIND(
                                IF(
                                    ?source = {sparql_string_literal(node.uuid)},
                                    ?target,
                                    ?source
                                ) AS ?uuid
                            )
                        }}
                        GROUP BY ?uuid
                        """,
                        ['uuid', 'count'],
                    )
                    projection[node.uuid] = [
                        Neighbor(node_uuid=record['uuid'], edge_count=int(float(record['count'])))
                        for record in neighbor_records
                    ]

                cluster_uuids = label_propagation(projection)
                for cluster in cluster_uuids:
                    if len(cluster) == 0:
                        continue
                    cluster_values = ', '.join(sparql_string_literal(uuid) for uuid in cluster)
                    cluster_records = await execute_sem_match_select(
                        executor,
                        f"""
                        SELECT ?uuid ?name ?group_id ?created_at ?summary ?labels ?attributes
                        WHERE {{
                            ?entity <gti:pred:type> "Entity" .
                            ?entity <gti:pred:uuid> ?uuid .
                            ?entity <gti:pred:name> ?name .
                            ?entity <gti:pred:group_id> ?group_id .
                            ?entity <gti:pred:created_at> ?created_at .
                            OPTIONAL {{ ?entity <gti:pred:summary> ?summary . }}
                            OPTIONAL {{ ?entity <gti:pred:labels> ?labels . }}
                            OPTIONAL {{ ?entity <gti:pred:attributes> ?attributes . }}
                            FILTER (?uuid IN ({cluster_values}))
                        }}
                        """,
                        ['uuid', 'name', 'group_id', 'created_at', 'summary', 'labels', 'attributes'],
                    )
                    community_clusters.append(
                        [entity_node_from_record(_normalize_entity_record(record)) for record in cluster_records]
                    )
            return community_clusters
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def remove_communities(
        self,
        executor: QueryExecutor,
    ) -> None:
        if rdf_mode_for_executor(executor):
            await execute_sparql_update(
                executor,
                build_delete_by_property_update('type', 'Community'),
            )
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def determine_entity_community(
        self,
        executor: QueryExecutor,
        entity: EntityNode,
    ) -> None:
        if rdf_mode_for_executor(executor):
            records = await execute_sem_match_select(
                executor,
                f"""
                SELECT ?uuid ?name ?group_id ?created_at ?summary ?name_embedding
                WHERE {{
                    ?edge <gti:pred:type> "HAS_MEMBER" .
                    ?edge <gti:pred:target_node_uuid> {sparql_string_literal(entity.uuid)} .
                    ?edge <gti:pred:source_node_uuid> ?community_uuid .
                    ?community <gti:pred:type> "Community" .
                    ?community <gti:pred:uuid> ?uuid .
                    ?community <gti:pred:name> ?name .
                    ?community <gti:pred:group_id> ?group_id .
                    ?community <gti:pred:created_at> ?created_at .
                    OPTIONAL {{ ?community <gti:pred:summary> ?summary . }}
                    OPTIONAL {{ ?community <gti:pred:name_embedding> ?name_embedding . }}
                    FILTER (?uuid = ?community_uuid)
                }}
                LIMIT 1
                """,
                ['uuid', 'name', 'group_id', 'created_at', 'summary', 'name_embedding'],
            )
            if len(records) > 0:
                return

            await execute_sem_match_select(
                executor,
                f"""
                SELECT ?uuid ?name ?group_id ?created_at ?summary ?name_embedding
                WHERE {{
                    ?member_edge <gti:pred:type> "HAS_MEMBER" .
                    ?member_edge <gti:pred:source_node_uuid> ?community_uuid .
                    ?member_edge <gti:pred:target_node_uuid> ?neighbor_uuid .
                    ?rel_edge <gti:pred:type> "RELATES_TO" .
                    ?rel_edge <gti:pred:source_node_uuid> ?src .
                    ?rel_edge <gti:pred:target_node_uuid> ?dst .
                    FILTER (
                        (?src = ?neighbor_uuid && ?dst = {sparql_string_literal(entity.uuid)})
                        || (?dst = ?neighbor_uuid && ?src = {sparql_string_literal(entity.uuid)})
                    )
                    ?community <gti:pred:type> "Community" .
                    ?community <gti:pred:uuid> ?uuid .
                    ?community <gti:pred:name> ?name .
                    ?community <gti:pred:group_id> ?group_id .
                    ?community <gti:pred:created_at> ?created_at .
                    OPTIONAL {{ ?community <gti:pred:summary> ?summary . }}
                    OPTIONAL {{ ?community <gti:pred:name_embedding> ?name_embedding . }}
                    FILTER (?uuid = ?community_uuid)
                }}
                LIMIT 1
                """,
                ['uuid', 'name', 'group_id', 'created_at', 'summary', 'name_embedding'],
            )
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_mentioned_nodes(
        self,
        executor: QueryExecutor,
        episodes: list[EpisodicNode],
    ) -> list[EntityNode]:
        episode_uuids = [episode.uuid for episode in episodes]
        if rdf_mode_for_executor(executor):
            if len(episode_uuids) == 0:
                return []
            episode_values = ', '.join(sparql_string_literal(uuid) for uuid in episode_uuids)
            records = await execute_sem_match_select(
                executor,
                f"""
                SELECT ?uuid ?name ?group_id ?created_at ?summary ?labels ?attributes
                WHERE {{
                    ?mention <gti:pred:type> "MENTIONS" .
                    ?mention <gti:pred:source_node_uuid> ?episode_uuid .
                    ?mention <gti:pred:target_node_uuid> ?uuid .
                    FILTER (?episode_uuid IN ({episode_values}))
                    ?entity <gti:pred:type> "Entity" .
                    ?entity <gti:pred:uuid> ?uuid .
                    ?entity <gti:pred:name> ?name .
                    ?entity <gti:pred:group_id> ?group_id .
                    ?entity <gti:pred:created_at> ?created_at .
                    OPTIONAL {{ ?entity <gti:pred:summary> ?summary . }}
                    OPTIONAL {{ ?entity <gti:pred:labels> ?labels . }}
                    OPTIONAL {{ ?entity <gti:pred:attributes> ?attributes . }}
                }}
                """,
                ['uuid', 'name', 'group_id', 'created_at', 'summary', 'labels', 'attributes'],
            )
            deduped = {
                record['uuid']: entity_node_from_record(_normalize_entity_record(record))
                for record in records
            }
            return list(deduped.values())
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_communities_by_nodes(
        self,
        executor: QueryExecutor,
        nodes: list[EntityNode],
    ) -> list[CommunityNode]:
        node_uuids = [node.uuid for node in nodes]
        if rdf_mode_for_executor(executor):
            if len(node_uuids) == 0:
                return []
            node_values = ', '.join(sparql_string_literal(uuid) for uuid in node_uuids)
            records = await execute_sem_match_select(
                executor,
                f"""
                SELECT ?uuid ?name ?group_id ?created_at ?summary ?name_embedding
                WHERE {{
                    ?edge <gti:pred:type> "HAS_MEMBER" .
                    ?edge <gti:pred:target_node_uuid> ?node_uuid .
                    ?edge <gti:pred:source_node_uuid> ?uuid .
                    FILTER (?node_uuid IN ({node_values}))
                    ?community <gti:pred:type> "Community" .
                    ?community <gti:pred:uuid> ?uuid .
                    ?community <gti:pred:name> ?name .
                    ?community <gti:pred:group_id> ?group_id .
                    ?community <gti:pred:created_at> ?created_at .
                    OPTIONAL {{ ?community <gti:pred:summary> ?summary . }}
                    OPTIONAL {{ ?community <gti:pred:name_embedding> ?name_embedding . }}
                }}
                """,
                ['uuid', 'name', 'group_id', 'created_at', 'summary', 'name_embedding'],
            )
            deduped = {
                record['uuid']: community_node_from_record(_normalize_community_record(record))
                for record in records
            }
            return list(deduped.values())
        raise ValueError(STRICT_RDF_ONLY_ERROR)
