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

from graphiti_core.driver.operations.entity_node_ops import EntityNodeOperations
from graphiti_core.driver.oracle.rdf_utils import (
    delete_embedding,
    delete_embeddings_bulk,
    build_delete_by_property_update,
    build_delete_subjects_update,
    build_node_subject,
    build_subject_upsert_update,
    execute_sem_match_join_select,
    execute_sem_match_select,
    execute_sparql_update,
    fetch_entity_node_embedding,
    fetch_entity_node_embeddings_bulk,
    get_embedding_table_name,
    parse_float_list_literal,
    parse_json_dict_literal,
    parse_json_list_literal,
    rdf_mode_for_executor,
    sparql_string_literal,
    upsert_entity_node_embedding,
    upsert_entity_node_embeddings_bulk,
)
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.driver.record_parsers import entity_node_from_record
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.nodes import EntityNode

logger = logging.getLogger(__name__)
STRICT_RDF_ONLY_ERROR = (
    'Oracle strict RDF mode requires ORACLE_USE_RDF=true and RDF/SPARQL operations only.'
)


def _normalize_entity_record(record: Any) -> dict[str, Any]:
    normalized = dict(record)
    normalized['labels'] = [str(value) for value in parse_json_list_literal(normalized.get('labels'))]
    normalized['attributes'] = parse_json_dict_literal(normalized.get('attributes'))
    normalized['summary'] = normalized.get('summary') or ''
    normalized['name_embedding'] = parse_float_list_literal(normalized.get('name_embedding'))
    return normalized


ENTITY_NODE_JOIN_SELECT_COLUMNS = [
    'm.uuid AS uuid',
    'm.name AS name',
    'm.group_id AS group_id',
    'm.created_at AS created_at',
    'm.labels AS labels',
    'm.attributes AS attributes',
    'e.summary AS summary',
    'e.name_embedding AS name_embedding',
]

ENTITY_NODE_SPARQL_CORE_PATTERNS = """
            ?entity <gti:pred:type> "Entity" .
            ?entity <gti:pred:uuid> ?uuid .
            ?entity <gti:pred:name> ?name .
            ?entity <gti:pred:group_id> ?group_id .
            ?entity <gti:pred:created_at> ?created_at .
            OPTIONAL { ?entity <gti:pred:labels> ?labels . }
            OPTIONAL { ?entity <gti:pred:attributes> ?attributes . }
"""


class OracleEntityNodeOperations(EntityNodeOperations):
    async def _query_nodes_with_embeddings(
        self,
        executor: QueryExecutor,
        sparql_query: str,
        *,
        limit: int | None = None,
        order_by_sem_rownum: bool = False,
    ) -> list[EntityNode]:
        embedding_table = get_embedding_table_name(executor, 'ENTITY_NODES')
        records = await execute_sem_match_join_select(
            executor,
            sparql_query,
            ENTITY_NODE_JOIN_SELECT_COLUMNS,
            join_table=embedding_table,
            table_alias='e',
            sem_alias='m',
            left_join=True,
            order_by_sem_rownum=order_by_sem_rownum,
            limit=limit,
        )
        return [entity_node_from_record(_normalize_entity_record(record)) for record in records]

    async def save(
        self,
        executor: QueryExecutor,
        node: EntityNode,
        tx: Transaction | None = None,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        subject = build_node_subject('entity', node.uuid)
        update_query = build_subject_upsert_update(
            subject,
            {
                'type': 'Entity',
                'uuid': node.uuid,
                'name': node.name,
                'group_id': node.group_id,
                'created_at': node.created_at,
                'labels': list(set(node.labels + ['Entity'])),
                'attributes': node.attributes or {},
            },
        )
        await execute_sparql_update(executor, update_query, tx=tx)
        await upsert_entity_node_embedding(
            executor,
            node.uuid,
            node.name_embedding,
            node.summary,
            tx=tx,
        )
        logger.debug(f'Saved Node to RDF Graph: {node.uuid}')

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[EntityNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        updates = [
            build_subject_upsert_update(
                build_node_subject('entity', node.uuid),
                {
                    'type': 'Entity',
                    'uuid': node.uuid,
                    'name': node.name,
                    'group_id': node.group_id,
                    'created_at': node.created_at,
                    'labels': list(set(node.labels + ['Entity'])),
                    'attributes': node.attributes or {},
                },
            )
            for node in nodes
        ]
        if updates:
            await execute_sparql_update(executor, '; '.join(updates), tx=tx)

        await upsert_entity_node_embeddings_bulk(
            executor,
            [(node.uuid, node.name_embedding, node.summary) for node in nodes],
            tx=tx,
        )

    async def delete(
        self,
        executor: QueryExecutor,
        node: EntityNode,
        tx: Transaction | None = None,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        embedding_table = get_embedding_table_name(executor, 'ENTITY_NODES')
        subjects = [
            build_node_subject(kind, node.uuid)
            for kind in ['entity', 'episodic', 'community', 'saga']
        ]
        await execute_sparql_update(executor, build_delete_subjects_update(subjects), tx=tx)
        await delete_embedding(executor, embedding_table, node.uuid, tx=tx)
        logger.debug(f'Deleted Node from RDF Graph: {node.uuid}')

    async def delete_by_group_id(
        self,
        executor: QueryExecutor,
        group_id: str,
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        embedding_table = get_embedding_table_name(executor, 'ENTITY_NODES')
        sparql_query = f"""
        SELECT ?uuid
        WHERE {{
            ?entity <gti:pred:type> "Entity" .
            ?entity <gti:pred:uuid> ?uuid .
            ?entity <gti:pred:group_id> ?group_id .
            FILTER (?group_id = {sparql_string_literal(group_id)})
        }}
        """
        records = await execute_sem_match_select(executor, sparql_query, ['uuid'])
        uuids = [str(record['uuid']) for record in records if record.get('uuid') is not None]

        await execute_sparql_update(
            executor,
            build_delete_by_property_update('group_id', group_id),
            tx=tx,
        )
        if uuids:
            await delete_embeddings_bulk(executor, embedding_table, uuids, tx=tx)

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        embedding_table = get_embedding_table_name(executor, 'ENTITY_NODES')
        subjects: list[str] = []
        for node_uuid in uuids:
            subjects.extend(
                [build_node_subject(kind, node_uuid) for kind in ['entity', 'episodic', 'community']]
            )
        if subjects:
            await execute_sparql_update(executor, build_delete_subjects_update(subjects), tx=tx)
        if uuids:
            await delete_embeddings_bulk(executor, embedding_table, uuids, tx=tx)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EntityNode:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        sparql_query = f"""
        SELECT ?uuid ?name ?group_id ?created_at ?labels ?attributes
        WHERE {{
            {ENTITY_NODE_SPARQL_CORE_PATTERNS}
            FILTER (?uuid = {sparql_string_literal(uuid)})
        }}
        LIMIT 1
        """
        nodes = await self._query_nodes_with_embeddings(
            executor,
            sparql_query,
            limit=1,
        )
        if len(nodes) == 0:
            raise NodeNotFoundError(uuid)
        return nodes[0]

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EntityNode]:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        if not uuids:
            return []
        uuid_values = ', '.join(sparql_string_literal(uuid) for uuid in uuids)
        sparql_query = f"""
        SELECT ?uuid ?name ?group_id ?created_at ?labels ?attributes
        WHERE {{
            {ENTITY_NODE_SPARQL_CORE_PATTERNS}
            FILTER (?uuid IN ({uuid_values}))
        }}
        """
        return await self._query_nodes_with_embeddings(
            executor,
            sparql_query,
        )

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EntityNode]:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        if not group_ids:
            return []
        filters = [f'?group_id IN ({", ".join(sparql_string_literal(v) for v in group_ids)})']
        if uuid_cursor:
            filters.append(f'?uuid < {sparql_string_literal(uuid_cursor)}')
        limit_clause = f'LIMIT {int(limit)}' if limit is not None else ''
        sparql_query = f"""
        SELECT ?uuid ?name ?group_id ?created_at ?labels ?attributes
        WHERE {{
            {ENTITY_NODE_SPARQL_CORE_PATTERNS}
            FILTER ({' && '.join(filters)})
        }}
        ORDER BY DESC(?uuid)
        {limit_clause}
        """
        return await self._query_nodes_with_embeddings(
            executor,
            sparql_query,
            order_by_sem_rownum=True,
        )

    async def load_embeddings(
        self,
        executor: QueryExecutor,
        node: EntityNode,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        sparql_query = f"""
        SELECT ?uuid
        WHERE {{
            ?entity <gti:pred:type> "Entity" .
            ?entity <gti:pred:uuid> ?uuid .
            FILTER (?uuid = {sparql_string_literal(node.uuid)})
        }}
        LIMIT 1
        """
        records = await execute_sem_match_select(executor, sparql_query, ['uuid'])
        if len(records) == 0:
            raise NodeNotFoundError(node.uuid)
        node.name_embedding = await fetch_entity_node_embedding(executor, node.uuid)

    async def load_embeddings_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[EntityNode],
        batch_size: int = 100,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        uuids = [n.uuid for n in nodes]
        if not uuids:
            return
        embedding_map = await fetch_entity_node_embeddings_bulk(executor, uuids)
        for node in nodes:
            if node.uuid in embedding_map:
                node.name_embedding = embedding_map[node.uuid]
