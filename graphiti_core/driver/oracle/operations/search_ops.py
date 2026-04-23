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

import json
from typing import Any

from graphiti_core.driver.operations.search_ops import SearchOperations
from graphiti_core.driver.oracle.rdf_utils import (
    execute_sem_match_join_select,
    execute_sem_match_select,
    get_embedding_table_name,
    parse_float_list_literal,
    parse_json_dict_literal,
    parse_json_list_literal,
    rdf_mode_for_executor,
    sparql_string_literal,
)
from graphiti_core.driver.query_executor import QueryExecutor
from graphiti_core.driver.record_parsers import entity_node_from_record
from graphiti_core.edges import EntityEdge
from graphiti_core.nodes import CommunityNode, EntityNode, EpisodicNode
from graphiti_core.search.search_filters import SearchFilters


def _normalize_entity_record(record: Any) -> dict[str, Any]:
    normalized_record = dict(record)
    normalized_record['labels'] = [str(value) for value in parse_json_list_literal(record.get('labels'))]
    normalized_record['attributes'] = parse_json_dict_literal(record.get('attributes'))
    normalized_record['summary'] = normalized_record.get('summary') or ''
    normalized_record['name_embedding'] = parse_float_list_literal(normalized_record.get('name_embedding'))
    return normalized_record


def _score_to_float(raw_score: Any) -> float:
    if isinstance(raw_score, (int, float)):
        return float(raw_score)
    if isinstance(raw_score, str):
        stripped = raw_score.strip()
        if len(stripped) >= 2 and stripped[0] == '"' and stripped[-1] == '"':
            stripped = stripped[1:-1]
        try:
            return float(stripped)
        except ValueError:
            return 0.0
    return 0.0


class OracleSearchOperations(SearchOperations):
    """Oracle search operations with RDF-aware rerankers."""

    async def node_fulltext_search(
        self,
        executor: QueryExecutor,
        query: str,
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[EntityNode]:
        if not rdf_mode_for_executor(executor):
            return []

        search_text = query.strip()
        if search_text == '':
            return []

        filter_clauses = [
            f'CONTAINS(LCASE(STR(?name)), LCASE({sparql_string_literal(search_text)}))',
        ]

        if group_ids is not None:
            group_id_values = ', '.join(sparql_string_literal(group_id) for group_id in group_ids)
            filter_clauses.append(f'?group_id IN ({group_id_values})')

        if search_filter.node_labels:
            label_clauses: list[str] = []
            for label in search_filter.node_labels:
                label_literal = sparql_string_literal(f'"{label}"')
                label_clauses.append(f'CONTAINS(STR(?labels), {label_literal})')
            if label_clauses:
                filter_clauses.append('(' + ' && '.join(label_clauses) + ')')

        sparql_filter = f"FILTER ({' && '.join(filter_clauses)})" if filter_clauses else ''
        sparql_query = f"""
        SELECT ?uuid ?name ?group_id ?created_at ?labels ?attributes
        WHERE {{
            ?entity <gti:pred:type> "Entity" .
            ?entity <gti:pred:uuid> ?uuid .
            ?entity <gti:pred:name> ?name .
            ?entity <gti:pred:group_id> ?group_id .
            ?entity <gti:pred:created_at> ?created_at .
            OPTIONAL {{ ?entity <gti:pred:labels> ?labels . }}
            OPTIONAL {{ ?entity <gti:pred:attributes> ?attributes . }}
            {sparql_filter}
        }}
        """
        embedding_table = get_embedding_table_name(executor, 'ENTITY_NODES')
        records = await execute_sem_match_join_select(
            executor,
            sparql_query,
            [
                'm.uuid AS uuid',
                'm.name AS name',
                'm.group_id AS group_id',
                'm.created_at AS created_at',
                'm.labels AS labels',
                'm.attributes AS attributes',
                'e.summary AS summary',
                'e.name_embedding AS name_embedding',
                # Name-first ranking, then summary fallback.
                (
                    f"CASE WHEN LOWER(m.name) LIKE '%' || LOWER({sparql_string_literal(search_text)}) || '%'"
                    ' THEN 2 ELSE 0 END'
                )
                + ' + '
                + (
                    f"CASE WHEN LOWER(COALESCE(e.summary, '')) LIKE '%' || "
                    f"LOWER({sparql_string_literal(search_text)}) || '%' THEN 1 ELSE 0 END"
                )
                + ' AS score',
            ],
            join_table=embedding_table,
            table_alias='e',
            sem_alias='m',
            left_join=True,
            order_by_sem_rownum=True,
        )
        records.sort(key=lambda r: _score_to_float(r.get('score')), reverse=True)
        top_records = records[:limit]
        return [entity_node_from_record(_normalize_entity_record(record)) for record in top_records]

    async def node_similarity_search(
        self,
        executor: QueryExecutor,
        search_vector: list[float],
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
        min_score: float = 0.6,
    ) -> list[EntityNode]:

        if not search_vector:
            return []

        embedding_table = get_embedding_table_name(executor, 'ENTITY_NODES')
        candidate_limit = max(limit * 20, 200)
        candidate_query = f"""
        SELECT uuid, (1 - dist) AS score
        FROM (
            SELECT
                e.uuid AS uuid,
                COSINE_DISTANCE(e.name_embedding, TO_VECTOR($search_vector)) AS dist
            FROM {embedding_table} e
            WHERE e.name_embedding IS NOT NULL
        )
        WHERE dist <= (1 - $min_score)
        ORDER BY dist ASC
        FETCH FIRST {int(candidate_limit)} ROWS ONLY
        """
        candidate_records, _, _ = await executor.execute_query(
            candidate_query,
            search_vector=json.dumps(search_vector),
            min_score=min_score,
        )
        if not candidate_records:
            return []

        candidate_scores = {record['uuid']: _score_to_float(record.get('score')) for record in candidate_records}
        candidate_uuids = list(candidate_scores.keys())
        top_uuid_values = ', '.join(sparql_string_literal(uuid) for uuid in candidate_uuids)
        filter_clauses = [f'?uuid IN ({top_uuid_values})']
        if group_ids is not None:
            group_id_values = ', '.join(sparql_string_literal(group_id) for group_id in group_ids)
            filter_clauses.append(f'?group_id IN ({group_id_values})')

        if search_filter.node_labels:
            for idx, label in enumerate(search_filter.node_labels):
                label_var = f'?labels_{idx}'
                label_literal = sparql_string_literal(f'"{label}"')
                filter_clauses.append(
                    'EXISTS { '
                    f'?entity <gti:pred:labels> {label_var} . '
                    f'FILTER(CONTAINS(STR({label_var}), {label_literal})) '
                    '}'
                )

        filter_expr = ' && '.join(filter_clauses)
        filter_query = f'FILTER ({filter_expr})'
        node_query = f"""
        SELECT ?uuid ?name ?group_id ?created_at ?labels ?attributes
        WHERE {{
            ?entity <gti:pred:type> "Entity" .
            ?entity <gti:pred:uuid> ?uuid .
            ?entity <gti:pred:name> ?name .
            ?entity <gti:pred:group_id> ?group_id .
            ?entity <gti:pred:created_at> ?created_at .
            OPTIONAL {{ ?entity <gti:pred:labels> ?labels . }}
            OPTIONAL {{ ?entity <gti:pred:attributes> ?attributes . }}
            {filter_query}
        }}
        """
        records = await execute_sem_match_join_select(
            executor,
            node_query,
            [
                'm.uuid AS uuid',
                'm.name AS name',
                'm.group_id AS group_id',
                'm.created_at AS created_at',
                'm.labels AS labels',
                'm.attributes AS attributes',
                'e.summary AS summary',
                'e.name_embedding AS name_embedding',
            ],
            join_table=embedding_table,
            table_alias='e',
            sem_alias='m',
            left_join=True,
            order_by_sem_rownum=True,
        )
        normalized_records = {
            normalized['uuid']: normalized
            for normalized in (_normalize_entity_record(record) for record in records)
        }
        ranked_uuids = [uuid for uuid in candidate_uuids if uuid in normalized_records]
        final_records = [normalized_records[uuid] for uuid in ranked_uuids[:limit]]
        final_records.sort(
            key=lambda record: candidate_scores.get(record['uuid'], 0.0),
            reverse=True,
        )
        return [entity_node_from_record(record) for record in final_records]

    async def node_bfs_search(
        self,
        executor: QueryExecutor,
        origin_uuids: list[str],
        search_filter: SearchFilters,
        max_depth: int,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[EntityNode]:
        return []

    async def edge_fulltext_search(
        self,
        executor: QueryExecutor,
        query: str,
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[EntityEdge]:
        return []

    async def edge_similarity_search(
        self,
        executor: QueryExecutor,
        search_vector: list[float],
        source_node_uuid: str | None,
        target_node_uuid: str | None,
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
        min_score: float = 0.6,
    ) -> list[EntityEdge]:
        return []

    async def edge_bfs_search(
        self,
        executor: QueryExecutor,
        origin_uuids: list[str],
        max_depth: int,
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[EntityEdge]:
        return []

    async def episode_fulltext_search(
        self,
        executor: QueryExecutor,
        query: str,
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[EpisodicNode]:
        return []

    async def community_fulltext_search(
        self,
        executor: QueryExecutor,
        query: str,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[CommunityNode]:
        return []

    async def community_similarity_search(
        self,
        executor: QueryExecutor,
        search_vector: list[float],
        group_ids: list[str] | None = None,
        limit: int = 10,
        min_score: float = 0.6,
    ) -> list[CommunityNode]:
        return []

    async def node_distance_reranker(
        self,
        executor: QueryExecutor,
        node_uuids: list[str],
        center_node_uuid: str,
        min_score: float = 0,
    ) -> list[EntityNode]:
        return []

    async def episode_mentions_reranker(
        self,
        executor: QueryExecutor,
        node_uuids: list[str],
        min_score: float = 0,
    ) -> list[EntityNode]:
        if not rdf_mode_for_executor(executor):
            return []

        if not node_uuids:
            return []

        uuid_values = ', '.join(sparql_string_literal(uuid) for uuid in node_uuids)
        sparql_query = f"""
        SELECT ?uuid (COUNT(?edge) AS ?score)
        WHERE {{
            ?edge <gti:pred:type> "MENTIONS" .
            ?edge <gti:pred:target_node_uuid> ?uuid .
            FILTER (?uuid IN ({uuid_values}))
        }}
        GROUP BY ?uuid
        """
        score_records = await execute_sem_match_select(
            executor,
            sparql_query,
            ['uuid', 'score'],
            order_by_sem_rownum=True,
        )
        scores: dict[str, float] = {
            record['uuid']: _score_to_float(record.get('score')) for record in score_records
        }

        for uuid in node_uuids:
            if uuid not in scores:
                scores[uuid] = float('inf')

        sorted_uuids = list(node_uuids)
        sorted_uuids.sort(key=lambda cur_uuid: scores[cur_uuid])
        reranked_uuids = [uuid for uuid in sorted_uuids if scores[uuid] >= min_score]

        if not reranked_uuids:
            return []

        node_uuid_values = ', '.join(sparql_string_literal(uuid) for uuid in reranked_uuids)
        entity_query = f"""
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
            FILTER (?uuid IN ({node_uuid_values}))
        }}
        """
        entity_records = await execute_sem_match_select(
            executor,
            entity_query,
            ['uuid', 'name', 'group_id', 'created_at', 'summary', 'labels', 'attributes'],
            order_by_sem_rownum=True,
        )
        node_map = {
            record['uuid']: entity_node_from_record(_normalize_entity_record(record))
            for record in entity_records
        }
        return [node_map[uuid] for uuid in reranked_uuids if uuid in node_map]

    def build_node_search_filters(self, search_filters: SearchFilters) -> Any:
        return None

    def build_edge_search_filters(self, search_filters: SearchFilters) -> Any:
        return None

    def build_fulltext_query(
        self,
        query: str,
        group_ids: list[str] | None = None,
        max_query_length: int = 8000,
    ) -> str:
        return ''
