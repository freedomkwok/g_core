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

from graphiti_core.driver.operations.graph_ops import GraphMaintenanceOperations
from graphiti_core.driver.operations.graph_utils import Neighbor, label_propagation
from graphiti_core.driver.oracle.sql_utils import build_in_clause, loads_json
from graphiti_core.driver.query_executor import QueryExecutor
from graphiti_core.driver.record_parsers import community_node_from_record, entity_node_from_record
from graphiti_core.nodes import CommunityNode, EntityNode, EpisodicNode

logger = logging.getLogger(__name__)


class OracleGraphMaintenanceOperations(GraphMaintenanceOperations):
    async def clear_data(
        self,
        executor: QueryExecutor,
        group_ids: list[str] | None = None,
    ) -> None:
        if group_ids is None:
            for table in [
                'GRAPHITI_RELATES_TO_EDGES',
                'GRAPHITI_MENTIONS_EDGES',
                'GRAPHITI_HAS_MEMBER_EDGES',
                'GRAPHITI_HAS_EPISODE_EDGES',
                'GRAPHITI_NEXT_EPISODE_EDGES',
                'GRAPHITI_ENTITY_NODES',
                'GRAPHITI_EPISODIC_NODES',
                'GRAPHITI_COMMUNITY_NODES',
                'GRAPHITI_SAGA_NODES',
            ]:
                await executor.execute_query(f'DELETE FROM {table}')
            return

        where_clause, where_params = build_in_clause('GROUP_ID', 'group_id', group_ids)
        for table in [
            'GRAPHITI_RELATES_TO_EDGES',
            'GRAPHITI_MENTIONS_EDGES',
            'GRAPHITI_HAS_MEMBER_EDGES',
            'GRAPHITI_HAS_EPISODE_EDGES',
            'GRAPHITI_NEXT_EPISODE_EDGES',
            'GRAPHITI_ENTITY_NODES',
            'GRAPHITI_EPISODIC_NODES',
            'GRAPHITI_COMMUNITY_NODES',
            'GRAPHITI_SAGA_NODES',
        ]:
            await executor.execute_query(f'DELETE FROM {table} WHERE {where_clause}', **where_params)

    async def build_indices_and_constraints(
        self,
        executor: QueryExecutor,
        delete_existing: bool = False,
    ) -> None:
        # Native Oracle schema and indexes are auto-created by OracleDriver.
        if delete_existing:
            await self.delete_all_indexes(executor)
        return

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

        if group_ids is None:
            group_id_values, _, _ = await executor.execute_query(
                """
                SELECT DISTINCT GROUP_ID AS group_id
                FROM GRAPHITI_ENTITY_NODES
                WHERE GROUP_ID IS NOT NULL
                """
            )
            group_ids = [r['group_id'] for r in group_id_values]

        resolved_group_ids: list[str] = group_ids or []
        for group_id in resolved_group_ids:
            projection: dict[str, list[Neighbor]] = {}

            node_records, _, _ = await executor.execute_query(
                """
                SELECT
                    UUID AS uuid,
                    NAME AS name,
                    GROUP_ID AS group_id,
                    CREATED_AT AS created_at,
                    SUMMARY AS summary,
                    LABELS_JSON AS labels,
                    ATTRIBUTES_JSON AS attributes,
                    NAME_EMBEDDING_JSON AS name_embedding
                FROM GRAPHITI_ENTITY_NODES
                WHERE GROUP_ID = $group_id
                """,
                group_id=group_id,
            )
            for record in node_records:
                record['labels'] = loads_json(record.get('labels'), [])
                record['attributes'] = loads_json(record.get('attributes'), {})
                record['name_embedding'] = loads_json(record.get('name_embedding'), None)
            nodes = [entity_node_from_record(r) for r in node_records]

            for node in nodes:
                records, _, _ = await executor.execute_query(
                    """
                    SELECT
                        CASE
                            WHEN SOURCE_NODE_UUID = $uuid THEN TARGET_NODE_UUID
                            ELSE SOURCE_NODE_UUID
                        END AS uuid,
                        COUNT(*) AS count
                    FROM GRAPHITI_RELATES_TO_EDGES
                    WHERE GROUP_ID = $group_id
                      AND (SOURCE_NODE_UUID = $uuid OR TARGET_NODE_UUID = $uuid)
                    GROUP BY
                        CASE
                            WHEN SOURCE_NODE_UUID = $uuid THEN TARGET_NODE_UUID
                            ELSE SOURCE_NODE_UUID
                        END
                    """,
                    uuid=node.uuid,
                    group_id=group_id,
                )

                projection[node.uuid] = [
                    Neighbor(node_uuid=record['uuid'], edge_count=record['count'])
                    for record in records
                ]

            cluster_uuids = label_propagation(projection)

            for cluster in cluster_uuids:
                if not cluster:
                    continue
                where_clause, where_params = build_in_clause('UUID', 'uuid', cluster)
                cluster_records, _, _ = await executor.execute_query(
                    f"""
                    SELECT
                        UUID AS uuid,
                        NAME AS name,
                        GROUP_ID AS group_id,
                        CREATED_AT AS created_at,
                        SUMMARY AS summary,
                        LABELS_JSON AS labels,
                        ATTRIBUTES_JSON AS attributes,
                        NAME_EMBEDDING_JSON AS name_embedding
                    FROM GRAPHITI_ENTITY_NODES
                    WHERE {where_clause}
                    """,
                    **where_params,
                )
                for record in cluster_records:
                    record['labels'] = loads_json(record.get('labels'), [])
                    record['attributes'] = loads_json(record.get('attributes'), {})
                    record['name_embedding'] = loads_json(record.get('name_embedding'), None)
                community_clusters.append([entity_node_from_record(r) for r in cluster_records])

        return community_clusters

    async def remove_communities(
        self,
        executor: QueryExecutor,
    ) -> None:
        await executor.execute_query('DELETE FROM GRAPHITI_HAS_MEMBER_EDGES')
        await executor.execute_query('DELETE FROM GRAPHITI_COMMUNITY_NODES')

    async def determine_entity_community(
        self,
        executor: QueryExecutor,
        entity: EntityNode,
    ) -> None:
        records, _, _ = await executor.execute_query(
            """
            SELECT
                c.UUID AS uuid,
                c.NAME AS name,
                c.GROUP_ID AS group_id,
                c.NAME_EMBEDDING_JSON AS name_embedding,
                c.CREATED_AT AS created_at,
                c.SUMMARY AS summary
            FROM GRAPHITI_COMMUNITY_NODES c
            JOIN GRAPHITI_HAS_MEMBER_EDGES e ON e.SOURCE_NODE_UUID = c.UUID
            WHERE e.TARGET_NODE_UUID = $entity_uuid
            """,
            entity_uuid=entity.uuid,
        )
        for record in records:
            record['name_embedding'] = loads_json(record.get('name_embedding'), [])

        if len(records) > 0:
            return

        await executor.execute_query(
            """
            SELECT DISTINCT
                c.UUID AS uuid,
                c.NAME AS name,
                c.GROUP_ID AS group_id,
                c.NAME_EMBEDDING_JSON AS name_embedding,
                c.CREATED_AT AS created_at,
                c.SUMMARY AS summary
            FROM GRAPHITI_COMMUNITY_NODES c
            JOIN GRAPHITI_HAS_MEMBER_EDGES hm ON hm.SOURCE_NODE_UUID = c.UUID
            JOIN GRAPHITI_RELATES_TO_EDGES r
              ON (r.SOURCE_NODE_UUID = hm.TARGET_NODE_UUID OR r.TARGET_NODE_UUID = hm.TARGET_NODE_UUID)
            WHERE r.SOURCE_NODE_UUID = $entity_uuid OR r.TARGET_NODE_UUID = $entity_uuid
            """,
            entity_uuid=entity.uuid,
        )

    async def get_mentioned_nodes(
        self,
        executor: QueryExecutor,
        episodes: list[EpisodicNode],
    ) -> list[EntityNode]:
        episode_uuids = [episode.uuid for episode in episodes]
        if len(episode_uuids) == 0:
            return []
        where_clause, where_params = build_in_clause('m.SOURCE_NODE_UUID', 'episode_uuid', episode_uuids)

        records, _, _ = await executor.execute_query(
            f"""
            SELECT DISTINCT
                n.UUID AS uuid,
                n.NAME AS name,
                n.GROUP_ID AS group_id,
                n.CREATED_AT AS created_at,
                n.SUMMARY AS summary,
                n.LABELS_JSON AS labels,
                n.ATTRIBUTES_JSON AS attributes,
                n.NAME_EMBEDDING_JSON AS name_embedding
            FROM GRAPHITI_ENTITY_NODES n
            JOIN GRAPHITI_MENTIONS_EDGES m ON m.TARGET_NODE_UUID = n.UUID
            WHERE {where_clause}
            """,
            **where_params,
        )
        for record in records:
            record['labels'] = loads_json(record.get('labels'), [])
            record['attributes'] = loads_json(record.get('attributes'), {})
            record['name_embedding'] = loads_json(record.get('name_embedding'), None)

        return [entity_node_from_record(r) for r in records]

    async def get_communities_by_nodes(
        self,
        executor: QueryExecutor,
        nodes: list[EntityNode],
    ) -> list[CommunityNode]:
        node_uuids = [node.uuid for node in nodes]
        if len(node_uuids) == 0:
            return []
        where_clause, where_params = build_in_clause('m.TARGET_NODE_UUID', 'node_uuid', node_uuids)

        records, _, _ = await executor.execute_query(
            f"""
            SELECT DISTINCT
                c.UUID AS uuid,
                c.NAME AS name,
                c.GROUP_ID AS group_id,
                c.NAME_EMBEDDING_JSON AS name_embedding,
                c.CREATED_AT AS created_at,
                c.SUMMARY AS summary
            FROM GRAPHITI_COMMUNITY_NODES c
            JOIN GRAPHITI_HAS_MEMBER_EDGES m ON m.SOURCE_NODE_UUID = c.UUID
            WHERE {where_clause}
            """,
            **where_params,
        )
        for record in records:
            record['name_embedding'] = loads_json(record.get('name_embedding'), [])

        return [community_node_from_record(r) for r in records]
