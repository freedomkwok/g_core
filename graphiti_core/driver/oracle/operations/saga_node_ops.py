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

from graphiti_core.driver.operations.saga_node_ops import SagaNodeOperations
from graphiti_core.driver.oracle.rdf_utils import (
    build_delete_by_property_update,
    build_delete_subjects_update,
    build_node_subject,
    build_subject_upsert_update,
    execute_sem_match_select,
    execute_sparql_update,
    rdf_mode_for_executor,
    sparql_string_literal,
)
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.helpers import parse_db_date
from graphiti_core.nodes import SagaNode

logger = logging.getLogger(__name__)
STRICT_RDF_ONLY_ERROR = (
    'Oracle strict RDF mode requires ORACLE_USE_RDF=true and RDF/SPARQL operations only.'
)


def _saga_node_from_record(record: Any) -> SagaNode:
    return SagaNode(
        uuid=record['uuid'],
        name=record['name'],
        group_id=record['group_id'],
        created_at=parse_db_date(record['created_at']),  # type: ignore[arg-type]
    )


class OracleSagaNodeOperations(SagaNodeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        node: SagaNode,
        tx: Transaction | None = None,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        subject = build_node_subject('saga', node.uuid)
        update_query = build_subject_upsert_update(
            subject,
            {
                'type': 'Saga',
                'uuid': node.uuid,
                'name': node.name,
                'group_id': node.group_id,
                'created_at': node.created_at,
            },
        )
        await execute_sparql_update(executor, update_query, tx=tx)
        logger.debug(f'Saved Saga Node to RDF Graph: {node.uuid}')

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[SagaNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if rdf_mode_for_executor(executor):
            updates = [
                build_subject_upsert_update(
                    build_node_subject('saga', node.uuid),
                    {
                        'type': 'Saga',
                        'uuid': node.uuid,
                        'name': node.name,
                        'group_id': node.group_id,
                        'created_at': node.created_at,
                    },
                )
                for node in nodes
            ]
            if updates:
                await execute_sparql_update(executor, '; '.join(updates), tx=tx)
            return

        for node in nodes:
            await self.save(executor, node, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        node: SagaNode,
        tx: Transaction | None = None,
    ) -> None:
        if rdf_mode_for_executor(executor):
            await execute_sparql_update(
                executor,
                build_delete_subjects_update([build_node_subject('saga', node.uuid)]),
                tx=tx,
            )
            logger.debug(f'Deleted Saga Node from RDF Graph: {node.uuid}')
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def delete_by_group_id(
        self,
        executor: QueryExecutor,
        group_id: str,
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if rdf_mode_for_executor(executor):
            await execute_sparql_update(
                executor,
                build_delete_by_property_update('group_id', group_id),
                tx=tx,
            )
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if rdf_mode_for_executor(executor):
            subjects = [build_node_subject('saga', node_uuid) for node_uuid in uuids]
            if subjects:
                await execute_sparql_update(executor, build_delete_subjects_update(subjects), tx=tx)
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> SagaNode:
        if rdf_mode_for_executor(executor):
            sparql_query = f"""
            SELECT ?uuid ?name ?group_id ?created_at
            WHERE {{
                ?saga <gti:pred:type> "Saga" .
                ?saga <gti:pred:uuid> ?uuid .
                ?saga <gti:pred:name> ?name .
                ?saga <gti:pred:group_id> ?group_id .
                ?saga <gti:pred:created_at> ?created_at .
                FILTER (?uuid = {sparql_string_literal(uuid)})
            }}
            LIMIT 1
            """
            records = await execute_sem_match_select(
                executor, sparql_query, ['uuid', 'name', 'group_id', 'created_at']
            )
            nodes = [_saga_node_from_record(r) for r in records]
            if len(nodes) == 0:
                raise NodeNotFoundError(uuid)
            return nodes[0]
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[SagaNode]:
        if rdf_mode_for_executor(executor):
            if not uuids:
                return []
            uuid_values = ', '.join(sparql_string_literal(uuid) for uuid in uuids)
            sparql_query = f"""
            SELECT ?uuid ?name ?group_id ?created_at
            WHERE {{
                ?saga <gti:pred:type> "Saga" .
                ?saga <gti:pred:uuid> ?uuid .
                ?saga <gti:pred:name> ?name .
                ?saga <gti:pred:group_id> ?group_id .
                ?saga <gti:pred:created_at> ?created_at .
                FILTER (?uuid IN ({uuid_values}))
            }}
            """
            records = await execute_sem_match_select(
                executor, sparql_query, ['uuid', 'name', 'group_id', 'created_at']
            )
            return [_saga_node_from_record(r) for r in records]
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[SagaNode]:
        if rdf_mode_for_executor(executor):
            if not group_ids:
                return []
            filters = [f'?group_id IN ({", ".join(sparql_string_literal(v) for v in group_ids)})']
            if uuid_cursor:
                filters.append(f'?uuid < {sparql_string_literal(uuid_cursor)}')
            limit_clause = f'LIMIT {int(limit)}' if limit is not None else ''
            sparql_query = f"""
            SELECT ?uuid ?name ?group_id ?created_at
            WHERE {{
                ?saga <gti:pred:type> "Saga" .
                ?saga <gti:pred:uuid> ?uuid .
                ?saga <gti:pred:name> ?name .
                ?saga <gti:pred:group_id> ?group_id .
                ?saga <gti:pred:created_at> ?created_at .
                FILTER ({' && '.join(filters)})
            }}
            ORDER BY DESC(?uuid)
            {limit_clause}
            """
            records = await execute_sem_match_select(
                executor,
                sparql_query,
                ['uuid', 'name', 'group_id', 'created_at'],
                order_by_sem_rownum=True,
            )
            return [_saga_node_from_record(r) for r in records]
        raise ValueError(STRICT_RDF_ONLY_ERROR)
