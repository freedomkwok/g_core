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
from datetime import datetime
from typing import Any

from graphiti_core.driver.operations.episode_node_ops import EpisodeNodeOperations
from graphiti_core.driver.oracle.rdf_utils import (
    build_delete_by_property_update,
    build_delete_subjects_update,
    build_node_subject,
    build_subject_upsert_update,
    delete_embedding,
    delete_embeddings_bulk,
    execute_sem_match_join_select,
    execute_sem_match_select,
    execute_sparql_update,
    get_embedding_table_name,
    parse_float_list_literal,
    parse_json_list_literal,
    rdf_mode_for_executor,
    sparql_datetime_literal,
    sparql_string_literal,
    upsert_episodic_node_embedding,
    upsert_episodic_node_embeddings_bulk,
)
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.driver.record_parsers import episodic_node_from_record
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.nodes import EpisodicNode

logger = logging.getLogger(__name__)
STRICT_RDF_ONLY_ERROR = (
    'Oracle strict RDF mode requires ORACLE_USE_RDF=true and RDF/SPARQL operations only.'
)


def _normalize_episode_record(record: Any) -> dict[str, Any]:
    normalized_record = dict(record)
    source_value = normalized_record.get('source')
    if isinstance(source_value, str):
        stripped = source_value.strip()
        if len(stripped) >= 2 and stripped[0] == '"' and stripped[-1] == '"':
            normalized_record['source'] = stripped[1:-1]
    normalized_record['entity_edges'] = [
        str(value) for value in parse_json_list_literal(normalized_record.get('entity_edges'))
    ]
    normalized_record['content_embedding'] = parse_float_list_literal(
        normalized_record.get('content_embedding')
    )
    return normalized_record


EPISODE_NODE_JOIN_SELECT_COLUMNS = [
    'm.uuid AS uuid',
    'm.name AS name',
    'm.group_id AS group_id',
    'm.created_at AS created_at',
    'm.source AS source',
    'm.valid_at AS valid_at',
    'm.entity_edges AS entity_edges',
    'e.source_description AS source_description',
    'e.content AS content',
    'e.content_embedding AS content_embedding',
]

EPISODE_NODE_SPARQL_CORE_PATTERNS = """
                ?episode <gti:pred:type> "Episodic" .
                ?episode <gti:pred:uuid> ?uuid .
                ?episode <gti:pred:name> ?name .
                ?episode <gti:pred:group_id> ?group_id .
                ?episode <gti:pred:created_at> ?created_at .
                ?episode <gti:pred:source> ?source .
                ?episode <gti:pred:valid_at> ?valid_at .
                OPTIONAL { ?episode <gti:pred:entity_edges> ?entity_edges . }
"""


class OracleEpisodeNodeOperations(EpisodeNodeOperations):
    async def _query_episodes_with_embeddings(
        self,
        executor: QueryExecutor,
        sparql_query: str,
        *,
        limit: int | None = None,
        order_by_sem_rownum: bool = False,
    ) -> list[EpisodicNode]:
        embedding_table = get_embedding_table_name(executor, 'EPISODIC_NODES')
        records = await execute_sem_match_join_select(
            executor,
            sparql_query,
            EPISODE_NODE_JOIN_SELECT_COLUMNS,
            join_table=embedding_table,
            table_alias='e',
            sem_alias='m',
            left_join=True,
            order_by_sem_rownum=order_by_sem_rownum,
            limit=limit,
        )
        return [episodic_node_from_record(_normalize_episode_record(r)) for r in records]

    async def save(
        self,
        executor: QueryExecutor,
        node: EpisodicNode,
        tx: Transaction | None = None,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        subject = build_node_subject('episodic', node.uuid)
        update_query = build_subject_upsert_update(
            subject,
            {
                'type': 'Episodic',
                'uuid': node.uuid,
                'name': node.name,
                'group_id': node.group_id,
                'source': node.source.value,
                'entity_edges': node.entity_edges,
                'created_at': node.created_at,
                'valid_at': node.valid_at,
            },
        )
        await execute_sparql_update(executor, update_query, tx=tx)
        await upsert_episodic_node_embedding(
            executor,
            node.uuid,
            node.content_embedding,
            node.content,
            tx=tx,
        )
        logger.debug(f'Saved Episode to RDF Graph: {node.uuid}')

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[EpisodicNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        updates = [
            build_subject_upsert_update(
                build_node_subject('episodic', node.uuid),
                {
                    'type': 'Episodic',
                    'uuid': node.uuid,
                    'name': node.name,
                    'group_id': node.group_id,
                    'source': node.source.value,
                    'entity_edges': node.entity_edges,
                    'created_at': node.created_at,
                    'valid_at': node.valid_at,
                },
            )
            for node in nodes
        ]
        if updates:
            await execute_sparql_update(executor, '; '.join(updates), tx=tx)
        await upsert_episodic_node_embeddings_bulk(
            executor,
            [(node.uuid, node.content_embedding, node.content) for node in nodes],
            tx=tx,
        )

    async def delete(
        self,
        executor: QueryExecutor,
        node: EpisodicNode,
        tx: Transaction | None = None,
    ) -> None:
        if not rdf_mode_for_executor(executor):
            raise ValueError(STRICT_RDF_ONLY_ERROR)

        embedding_table = get_embedding_table_name(executor, 'EPISODIC_NODES')
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

        embedding_table = get_embedding_table_name(executor, 'EPISODIC_NODES')
        sparql_query = f"""
        SELECT ?uuid
        WHERE {{
            ?episode <gti:pred:type> "Episodic" .
            ?episode <gti:pred:uuid> ?uuid .
            ?episode <gti:pred:group_id> ?group_id .
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

        embedding_table = get_embedding_table_name(executor, 'EPISODIC_NODES')
        subjects = [build_node_subject('episodic', node_uuid) for node_uuid in uuids]
        if subjects:
            await execute_sparql_update(executor, build_delete_subjects_update(subjects), tx=tx)
        if uuids:
            await delete_embeddings_bulk(executor, embedding_table, uuids, tx=tx)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EpisodicNode:
        if rdf_mode_for_executor(executor):
            sparql_query = f"""
            SELECT ?uuid ?name ?group_id ?created_at ?source ?valid_at ?entity_edges
            WHERE {{
                {EPISODE_NODE_SPARQL_CORE_PATTERNS}
                FILTER (?uuid = {sparql_string_literal(uuid)})
            }}
            LIMIT 1
            """
            episodes = await self._query_episodes_with_embeddings(
                executor,
                sparql_query,
                limit=1,
            )
            if len(episodes) == 0:
                raise NodeNotFoundError(uuid)
            return episodes[0]

        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EpisodicNode]:
        if rdf_mode_for_executor(executor):
            if not uuids:
                return []
            uuid_values = ', '.join(sparql_string_literal(uuid) for uuid in uuids)
            sparql_query = f"""
            SELECT ?uuid ?name ?group_id ?created_at ?source ?valid_at ?entity_edges
            WHERE {{
                {EPISODE_NODE_SPARQL_CORE_PATTERNS}
                FILTER (?uuid IN ({uuid_values}))
            }}
            """
            return await self._query_episodes_with_embeddings(executor, sparql_query)

        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EpisodicNode]:
        if rdf_mode_for_executor(executor):
            if not group_ids:
                return []
            filters = [f'?group_id IN ({", ".join(sparql_string_literal(v) for v in group_ids)})']
            if uuid_cursor:
                filters.append(f'?uuid < {sparql_string_literal(uuid_cursor)}')
            limit_clause = f'LIMIT {int(limit)}' if limit is not None else ''
            sparql_query = f"""
            SELECT ?uuid ?name ?group_id ?created_at ?source ?valid_at ?entity_edges
            WHERE {{
                {EPISODE_NODE_SPARQL_CORE_PATTERNS}
                FILTER ({' && '.join(filters)})
            }}
            ORDER BY DESC(?uuid)
            {limit_clause}
            """
            return await self._query_episodes_with_embeddings(
                executor,
                sparql_query,
                order_by_sem_rownum=True,
            )

        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def get_by_entity_node_uuid(
        self,
        executor: QueryExecutor,
        entity_node_uuid: str,
    ) -> list[EpisodicNode]:
        if rdf_mode_for_executor(executor):
            sparql_query = f"""
            SELECT ?uuid ?name ?group_id ?created_at ?source ?valid_at ?entity_edges
            WHERE {{
                ?mentions <gti:pred:type> "MENTIONS" .
                ?mentions <gti:pred:target_node_uuid> {sparql_string_literal(entity_node_uuid)} .
                ?mentions <gti:pred:source_node_uuid> ?uuid .
                {EPISODE_NODE_SPARQL_CORE_PATTERNS}
            }}
            """
            return await self._query_episodes_with_embeddings(executor, sparql_query)

        raise ValueError(STRICT_RDF_ONLY_ERROR)

    async def retrieve_episodes(
        self,
        executor: QueryExecutor,
        reference_time: datetime,
        last_n: int = 3,
        group_ids: list[str] | None = None,
        source: str | None = None,
        saga: str | None = None,
    ) -> list[EpisodicNode]:
        if rdf_mode_for_executor(executor):
            if last_n <= 0:
                return []
            filters = [f'?valid_at <= {sparql_datetime_literal(reference_time)}']

            if group_ids:
                group_values = ', '.join(sparql_string_literal(group_id) for group_id in group_ids)
                filters.append(f'?group_id IN ({group_values})')

            if source:
                filters.append(f'?source = {sparql_string_literal(source)}')

            saga_patterns = ''
            if saga is not None and group_ids is not None and len(group_ids) > 0:
                saga_patterns = f"""
                ?saga <gti:pred:type> "Saga" .
                ?saga <gti:pred:name> {sparql_string_literal(saga)} .
                ?saga <gti:pred:group_id> {sparql_string_literal(group_ids[0])} .
                ?saga <gti:pred:uuid> ?saga_uuid .
                ?has_episode <gti:pred:type> "HAS_EPISODE" .
                ?has_episode <gti:pred:source_node_uuid> ?saga_uuid .
                ?has_episode <gti:pred:target_node_uuid> ?uuid .
                """

            sparql_query = f"""
            SELECT ?uuid ?name ?group_id ?created_at ?source ?valid_at ?entity_edges
            WHERE {{
                {EPISODE_NODE_SPARQL_CORE_PATTERNS}
                {saga_patterns}
                FILTER ({' && '.join(filters)})
            }}
            ORDER BY DESC(?valid_at)
            LIMIT {int(last_n)}
            """

            episodes = await self._query_episodes_with_embeddings(
                executor,
                sparql_query,
                order_by_sem_rownum=True,
            )
            return episodes

        raise ValueError(STRICT_RDF_ONLY_ERROR)
