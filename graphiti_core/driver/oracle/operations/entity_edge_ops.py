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

from graphiti_core.driver.operations.entity_edge_ops import EntityEdgeOperations
from graphiti_core.driver.oracle.rdf_utils import (
    delete_embedding,
    delete_embeddings_bulk,
    build_delete_subjects_update,
    build_edge_subject,
    build_subject_upsert_update,
    execute_sem_match_join_select,
    execute_sem_match_select,
    execute_sparql_update,
    fetch_entity_edge_fact_embedding,
    fetch_entity_edge_fact_embeddings_bulk,
    get_embedding_table_name,
    parse_float_list_literal,
    parse_json_dict_literal,
    parse_json_list_literal,
    rdf_mode_for_executor,
    sparql_string_literal,
    upsert_entity_edge_embedding,
    upsert_entity_edge_embeddings_bulk,
)
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.driver.record_parsers import entity_edge_from_record
from graphiti_core.edges import EntityEdge
from graphiti_core.errors import EdgeNotFoundError

logger = logging.getLogger(__name__)
STRICT_RDF_ONLY_ERROR = (
    'Oracle strict RDF mode requires ORACLE_USE_RDF=true and RDF/SPARQL operations only.'
)


def _normalize_entity_edge_record(record: Any) -> dict[str, Any]:
    normalized = dict(record)
    normalized['episodes'] = [str(value) for value in parse_json_list_literal(normalized.get('episodes'))]
    normalized['attributes'] = parse_json_dict_literal(normalized.get('attributes'))
    normalized['name'] = normalized.get('name') or ''
    normalized['fact'] = normalized.get('fact') or ''
    normalized['fact_embedding'] = parse_float_list_literal(normalized.get('fact_embedding'))
    return normalized


ENTITY_EDGE_JOIN_SELECT_COLUMNS = [
    'm.uuid AS uuid',
    'm.source_node_uuid AS source_node_uuid',
    'm.target_node_uuid AS target_node_uuid',
    'm.group_id AS group_id',
    'm.created_at AS created_at',
    'm.name AS name',
    'm.expired_at AS expired_at',
    'm.valid_at AS valid_at',
    'm.invalid_at AS invalid_at',
    'm.attributes AS attributes',
    'e.fact AS fact',
    'e.episodes AS episodes',
    'e.fact_embedding AS fact_embedding',
]

ENTITY_EDGE_SPARQL_CORE_PATTERNS = """
                ?edge <gti:pred:type> "RELATES_TO" .
                ?edge <gti:pred:uuid> ?uuid .
                ?edge <gti:pred:source_node_uuid> ?source_node_uuid .
                ?edge <gti:pred:target_node_uuid> ?target_node_uuid .
                ?edge <gti:pred:group_id> ?group_id .
                ?edge <gti:pred:created_at> ?created_at .
                OPTIONAL { ?edge <gti:pred:name> ?name . }
                OPTIONAL { ?edge <gti:pred:fact> ?fact . }
                OPTIONAL { ?edge <gti:pred:episodes> ?episodes . }
                OPTIONAL { ?edge <gti:pred:expired_at> ?expired_at . }
                OPTIONAL { ?edge <gti:pred:valid_at> ?valid_at . }
                OPTIONAL { ?edge <gti:pred:invalid_at> ?invalid_at . }
                OPTIONAL { ?edge <gti:pred:attributes> ?attributes . }
"""


class OracleEntityEdgeOperations(EntityEdgeOperations):
    async def _query_edges_with_embeddings(
        self,
        executor: QueryExecutor,
        sparql_query: str,
        *,
        limit: int | None = None,
        order_by_sem_rownum: bool = False,
    ) -> list[EntityEdge]:
        embedding_table = get_embedding_table_name(executor, 'ENTITY_EDGES')
        records = await execute_sem_match_join_select(
            executor,
            sparql_query,
            ENTITY_EDGE_JOIN_SELECT_COLUMNS,
            join_table=embedding_table,
            table_alias='e',
            sem_alias='m',
            left_join=True,
            order_by_sem_rownum=order_by_sem_rownum,
            limit=limit,
        )
        return [entity_edge_from_record(_normalize_entity_edge_record(r)) for r in records]

    async def save(
        self,
        executor: QueryExecutor,
        edge: EntityEdge,
        tx: Transaction | None = None,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        subject = build_edge_subject('relates_to', edge.uuid)
        update_query = build_subject_upsert_update(
            subject,
            {
                'type': 'RELATES_TO',
                'uuid': edge.uuid,
                'group_id': edge.group_id,
                'source_node_uuid': edge.source_node_uuid,
                'target_node_uuid': edge.target_node_uuid,
                'name': edge.name,
                'created_at': edge.created_at,
                'expired_at': edge.expired_at,
                'valid_at': edge.valid_at,
                'invalid_at': edge.invalid_at,
                'attributes': edge.attributes or {},
            },
        )
        await execute_sparql_update(executor, update_query, tx=tx)
        await upsert_entity_edge_embedding(
            executor,
            edge.uuid,
            edge.fact_embedding,
            tx=tx,
        )
        logger.debug(f'Saved Edge to RDF Graph: {edge.uuid}')

    async def save_bulk(
        self,
        executor: QueryExecutor,
        edges: list[EntityEdge],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        updates = [
            build_subject_upsert_update(
                build_edge_subject('relates_to', edge.uuid),
                {
                    'type': 'RELATES_TO',
                    'uuid': edge.uuid,
                    'group_id': edge.group_id,
                    'source_node_uuid': edge.source_node_uuid,
                    'target_node_uuid': edge.target_node_uuid,
                    'name': edge.name,
                    'created_at': edge.created_at,
                    'expired_at': edge.expired_at,
                    'valid_at': edge.valid_at,
                    'invalid_at': edge.invalid_at,
                    'attributes': edge.attributes or {},
                },
            )
            for edge in edges
        ]
        if updates:
            await execute_sparql_update(executor, '; '.join(updates), tx=tx)

        await upsert_entity_edge_embeddings_bulk(
            executor,
            [(edge.uuid, edge.fact_embedding) for edge in edges],
            tx=tx,
        )

    async def delete(
        self,
        executor: QueryExecutor,
        edge: EntityEdge,
        tx: Transaction | None = None,
    ) -> None:
        if rdf_mode_for_executor(executor):
            embedding_table = get_embedding_table_name(executor, 'ENTITY_EDGES')
            await execute_sparql_update(
                executor,
                build_delete_subjects_update([build_edge_subject('relates_to', edge.uuid)]),
                tx=tx,
            )
            await delete_embedding(
                executor,
                embedding_table,
                edge.uuid,
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
            embedding_table = get_embedding_table_name(executor, 'ENTITY_EDGES')
            subjects = [build_edge_subject('relates_to', edge_uuid) for edge_uuid in uuids]
            if subjects:
                await execute_sparql_update(executor, build_delete_subjects_update(subjects), tx=tx)
            if uuids:
                await delete_embeddings_bulk(
                    executor,
                    embedding_table,
                    uuids,
                    tx=tx,
                )
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EntityEdge:
        if rdf_mode_for_executor(executor):
            sparql_query = f"""
            SELECT
                ?uuid ?source_node_uuid ?target_node_uuid ?group_id ?created_at
                ?name ?fact ?episodes ?expired_at ?valid_at ?invalid_at ?attributes
            WHERE {{
                {ENTITY_EDGE_SPARQL_CORE_PATTERNS}
                FILTER (?uuid = {sparql_string_literal(uuid)})
            }}
            LIMIT 1
            """
            edges = await self._query_edges_with_embeddings(
                executor,
                sparql_query,
                limit=1,
            )
            if len(edges) == 0:
                raise EdgeNotFoundError(uuid)
            return edges[0]
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EntityEdge]:
        if rdf_mode_for_executor(executor):
            if not uuids:
                return []
            uuid_values = ', '.join(sparql_string_literal(uuid) for uuid in uuids)
            sparql_query = f"""
            SELECT
                ?uuid ?source_node_uuid ?target_node_uuid ?group_id ?created_at
                ?name ?fact ?episodes ?expired_at ?valid_at ?invalid_at ?attributes
            WHERE {{
                {ENTITY_EDGE_SPARQL_CORE_PATTERNS}
                FILTER (?uuid IN ({uuid_values}))
            }}
            """
            return await self._query_edges_with_embeddings(executor, sparql_query)
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EntityEdge]:
        if rdf_mode_for_executor(executor):
            if not group_ids:
                return []
            filters = [f'?group_id IN ({", ".join(sparql_string_literal(v) for v in group_ids)})']
            if uuid_cursor:
                filters.append(f'?uuid < {sparql_string_literal(uuid_cursor)}')
            limit_clause = f'LIMIT {int(limit)}' if limit is not None else ''
            sparql_query = f"""
            SELECT
                ?uuid ?source_node_uuid ?target_node_uuid ?group_id ?created_at
                ?name ?fact ?episodes ?expired_at ?valid_at ?invalid_at ?attributes
            WHERE {{
                {ENTITY_EDGE_SPARQL_CORE_PATTERNS}
                FILTER ({' && '.join(filters)})
            }}
            ORDER BY DESC(?uuid)
            {limit_clause}
            """
            return await self._query_edges_with_embeddings(
                executor,
                sparql_query,
                order_by_sem_rownum=True,
            )
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_between_nodes(
        self,
        executor: QueryExecutor,
        source_node_uuid: str,
        target_node_uuid: str,
    ) -> list[EntityEdge]:
        if rdf_mode_for_executor(executor):
            sparql_query = f"""
            SELECT
                ?uuid ?source_node_uuid ?target_node_uuid ?group_id ?created_at
                ?name ?fact ?episodes ?expired_at ?valid_at ?invalid_at ?attributes
            WHERE {{
                {ENTITY_EDGE_SPARQL_CORE_PATTERNS}
                FILTER (
                    ?source_node_uuid = {sparql_string_literal(source_node_uuid)}
                    && ?target_node_uuid = {sparql_string_literal(target_node_uuid)}
                )
            }}
            """
            return await self._query_edges_with_embeddings(executor, sparql_query)
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_node_uuid(
        self,
        executor: QueryExecutor,
        node_uuid: str,
    ) -> list[EntityEdge]:
        if rdf_mode_for_executor(executor):
            sparql_query = f"""
            SELECT
                ?uuid ?source_node_uuid ?target_node_uuid ?group_id ?created_at
                ?name ?fact ?episodes ?expired_at ?valid_at ?invalid_at ?attributes
            WHERE {{
                {ENTITY_EDGE_SPARQL_CORE_PATTERNS}
                FILTER (
                    ?source_node_uuid = {sparql_string_literal(node_uuid)}
                    || ?target_node_uuid = {sparql_string_literal(node_uuid)}
                )
            }}
            """
            return await self._query_edges_with_embeddings(executor, sparql_query)
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def load_embeddings(
        self,
        executor: QueryExecutor,
        edge: EntityEdge,
    ) -> None:
        if rdf_mode_for_executor(executor):
            embedding_table = get_embedding_table_name(executor, 'ENTITY_EDGES')
            sparql_query = f"""
            SELECT ?uuid
            WHERE {{
                ?edge <gti:pred:type> "RELATES_TO" .
                ?edge <gti:pred:uuid> ?uuid .
                FILTER (?uuid = {sparql_string_literal(edge.uuid)})
            }}
            LIMIT 1
            """
            records = await execute_sem_match_select(executor, sparql_query, ['uuid'])
            if len(records) == 0:
                raise EdgeNotFoundError(edge.uuid)
            edge.fact_embedding = await fetch_entity_edge_fact_embedding(executor, edge.uuid)
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def load_embeddings_bulk(
        self,
        executor: QueryExecutor,
        edges: list[EntityEdge],
        batch_size: int = 100,
    ) -> None:
        if rdf_mode_for_executor(executor):
            uuids = [e.uuid for e in edges]
            if not uuids:
                return
            embedding_map = await fetch_entity_edge_fact_embeddings_bulk(executor, uuids)
            for edge in edges:
                if edge.uuid in embedding_map:
                    edge.fact_embedding = embedding_map[edge.uuid]
            return
        raise ValueError(STRICT_RDF_ONLY_ERROR)
