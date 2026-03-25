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

from graphiti_core.driver.neo4j.operations.next_episode_edge_ops import (
    Neo4jNextEpisodeEdgeOperations,
)
from graphiti_core.driver.oracle.rdf_utils import (
    build_delete_subjects_update,
    build_edge_subject,
    build_subject_upsert_update,
    execute_sparql_update,
    rdf_mode_for_executor,
)

logger = logging.getLogger(__name__)


class OracleNextEpisodeEdgeOperations(Neo4jNextEpisodeEdgeOperations):
    """Oracle NEXT_EPISODE edge operations."""

    async def save(self, executor, edge, tx=None) -> None:
        if rdf_mode_for_executor(executor):
            subject = build_edge_subject('next_episode', edge.uuid)
            update_query = build_subject_upsert_update(
                subject,
                {
                    'type': 'NEXT_EPISODE',
                    'uuid': edge.uuid,
                    'group_id': edge.group_id,
                    'source_node_uuid': edge.source_node_uuid,
                    'target_node_uuid': edge.target_node_uuid,
                    'created_at': edge.created_at,
                },
            )
            await execute_sparql_update(executor, update_query, tx=tx)
            logger.debug(f'Saved NEXT_EPISODE edge to RDF Graph: {edge.uuid}')
            return
        await super().save(executor, edge, tx=tx)

    async def save_bulk(self, executor, edges, tx=None, batch_size: int = 100) -> None:
        if rdf_mode_for_executor(executor):
            updates = [
                build_subject_upsert_update(
                    build_edge_subject('next_episode', edge.uuid),
                    {
                        'type': 'NEXT_EPISODE',
                        'uuid': edge.uuid,
                        'group_id': edge.group_id,
                        'source_node_uuid': edge.source_node_uuid,
                        'target_node_uuid': edge.target_node_uuid,
                        'created_at': edge.created_at,
                    },
                )
                for edge in edges
            ]
            if updates:
                await execute_sparql_update(executor, '; '.join(updates), tx=tx)
            return
        await super().save_bulk(executor, edges, tx=tx, batch_size=batch_size)

    async def delete(self, executor, edge, tx=None) -> None:
        if rdf_mode_for_executor(executor):
            await execute_sparql_update(
                executor,
                build_delete_subjects_update([build_edge_subject('next_episode', edge.uuid)]),
                tx=tx,
            )
            logger.debug(f'Deleted NEXT_EPISODE edge from RDF Graph: {edge.uuid}')
            return
        await super().delete(executor, edge, tx=tx)

    async def delete_by_uuids(self, executor, uuids, tx=None) -> None:
        if rdf_mode_for_executor(executor):
            subjects = [build_edge_subject('next_episode', edge_uuid) for edge_uuid in uuids]
            if subjects:
                await execute_sparql_update(executor, build_delete_subjects_update(subjects), tx=tx)
            return
        await super().delete_by_uuids(executor, uuids, tx=tx)
