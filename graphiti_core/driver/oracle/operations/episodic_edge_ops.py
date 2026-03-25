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

from graphiti_core.driver.operations.episodic_edge_ops import EpisodicEdgeOperations
from graphiti_core.driver.oracle.rdf_utils import (
    build_delete_subjects_update,
    build_edge_subject,
    build_subject_upsert_update,
    execute_sem_match_select,
    execute_sparql_update,
    rdf_mode_for_executor,
    sparql_string_literal,
)
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.edges import EpisodicEdge
from graphiti_core.errors import EdgeNotFoundError
from graphiti_core.helpers import parse_db_date

logger = logging.getLogger(__name__)
STRICT_RDF_ONLY_ERROR = (
    'Oracle strict RDF mode requires ORACLE_USE_RDF=true and RDF/SPARQL operations only.'
)


def _episodic_edge_from_record(record: Any) -> EpisodicEdge:
    return EpisodicEdge(
        uuid=record['uuid'],
        group_id=record['group_id'],
        source_node_uuid=record['source_node_uuid'],
        target_node_uuid=record['target_node_uuid'],
        created_at=parse_db_date(record['created_at']),  # type: ignore[arg-type]
    )


class OracleEpisodicEdgeOperations(EpisodicEdgeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        edge: EpisodicEdge,
        tx: Transaction | None = None,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        subject = build_edge_subject('mentions', edge.uuid)
        update_query = build_subject_upsert_update(
            subject,
            {
                'type': 'MENTIONS',
                'uuid': edge.uuid,
                'group_id': edge.group_id,
                'source_node_uuid': edge.source_node_uuid,
                'target_node_uuid': edge.target_node_uuid,
                'created_at': edge.created_at,
            },
        )
        await execute_sparql_update(executor, update_query, tx=tx)
        logger.debug(f'Saved Edge to RDF Graph: {edge.uuid}')

    async def save_bulk(
        self,
        executor: QueryExecutor,
        edges: list[EpisodicEdge],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        updates = [
            build_subject_upsert_update(
                build_edge_subject('mentions', edge.uuid),
                {
                    'type': 'MENTIONS',
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

    async def delete(
        self,
        executor: QueryExecutor,
        edge: EpisodicEdge,
        tx: Transaction | None = None,
    ) -> None:
        if rdf_mode_for_executor(executor):
            await execute_sparql_update(
                executor,
                build_delete_subjects_update([build_edge_subject('mentions', edge.uuid)]),
                tx=tx,
            )
            logger.debug(f'Deleted Edge from RDF Graph: {edge.uuid}')
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
    ) -> None:
        if rdf_mode_for_executor(executor):
            subjects = [build_edge_subject('mentions', edge_uuid) for edge_uuid in uuids]
            if subjects:
                await execute_sparql_update(executor, build_delete_subjects_update(subjects), tx=tx)
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EpisodicEdge:
        if rdf_mode_for_executor(executor):
            sparql_query = f"""
            SELECT ?uuid ?group_id ?source_node_uuid ?target_node_uuid ?created_at
            WHERE {{
                ?edge <gti:pred:type> "MENTIONS" .
                ?edge <gti:pred:uuid> ?uuid .
                ?edge <gti:pred:group_id> ?group_id .
                ?edge <gti:pred:source_node_uuid> ?source_node_uuid .
                ?edge <gti:pred:target_node_uuid> ?target_node_uuid .
                ?edge <gti:pred:created_at> ?created_at .
                FILTER (?uuid = {sparql_string_literal(uuid)})
            }}
            LIMIT 1
            """
            records = await execute_sem_match_select(
                executor,
                sparql_query,
                ['uuid', 'group_id', 'source_node_uuid', 'target_node_uuid', 'created_at'],
            )
            edges = [_episodic_edge_from_record(r) for r in records]
            if len(edges) == 0:
                raise EdgeNotFoundError(uuid)
            return edges[0]
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EpisodicEdge]:
        if rdf_mode_for_executor(executor):
            if not uuids:
                return []
            uuid_values = ', '.join(sparql_string_literal(uuid) for uuid in uuids)
            sparql_query = f"""
            SELECT ?uuid ?group_id ?source_node_uuid ?target_node_uuid ?created_at
            WHERE {{
                ?edge <gti:pred:type> "MENTIONS" .
                ?edge <gti:pred:uuid> ?uuid .
                ?edge <gti:pred:group_id> ?group_id .
                ?edge <gti:pred:source_node_uuid> ?source_node_uuid .
                ?edge <gti:pred:target_node_uuid> ?target_node_uuid .
                ?edge <gti:pred:created_at> ?created_at .
                FILTER (?uuid IN ({uuid_values}))
            }}
            """
            records = await execute_sem_match_select(
                executor,
                sparql_query,
                ['uuid', 'group_id', 'source_node_uuid', 'target_node_uuid', 'created_at'],
            )
            return [_episodic_edge_from_record(r) for r in records]
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EpisodicEdge]:
        if rdf_mode_for_executor(executor):
            if not group_ids:
                return []
            filters = [f'?group_id IN ({", ".join(sparql_string_literal(v) for v in group_ids)})']
            if uuid_cursor:
                filters.append(f'?uuid < {sparql_string_literal(uuid_cursor)}')
            limit_clause = f'LIMIT {int(limit)}' if limit is not None else ''
            sparql_query = f"""
            SELECT ?uuid ?group_id ?source_node_uuid ?target_node_uuid ?created_at
            WHERE {{
                ?edge <gti:pred:type> "MENTIONS" .
                ?edge <gti:pred:uuid> ?uuid .
                ?edge <gti:pred:group_id> ?group_id .
                ?edge <gti:pred:source_node_uuid> ?source_node_uuid .
                ?edge <gti:pred:target_node_uuid> ?target_node_uuid .
                ?edge <gti:pred:created_at> ?created_at .
                FILTER ({' && '.join(filters)})
            }}
            ORDER BY DESC(?uuid)
            {limit_clause}
            """
            records = await execute_sem_match_select(
                executor,
                sparql_query,
                ['uuid', 'group_id', 'source_node_uuid', 'target_node_uuid', 'created_at'],
                order_by_sem_rownum=True,
            )
            return [_episodic_edge_from_record(r) for r in records]
        raise ValueError(STRICT_RDF_ONLY_ERROR)
