"""
Oracle PG search operations implemented with relational SQL.
"""

from __future__ import annotations

import json
from typing import Any

from graphiti_core.driver.operations.search_ops import SearchOperations
from graphiti_core.driver.oracle_pg.sql_utils import (
    get_graph_id_for_executor,
    get_property_graph_name,
    get_table_name,
    parse_json_dict,
    parse_json_list,
    run_query,
    sql_in_list,
    sql_string_literal,
)
from graphiti_core.driver.query_executor import QueryExecutor
from graphiti_core.driver.record_parsers import (
    community_node_from_record,
    entity_edge_from_record,
    entity_node_from_record,
    episodic_node_from_record,
)
from graphiti_core.edges import EntityEdge
from graphiti_core.helpers import validate_group_ids
from graphiti_core.utils.keyword_extractor import build_fulltext_terms_from_query
from graphiti_core.nodes import CommunityNode, EntityNode, EpisodicNode
from graphiti_core.search.search_filters import SearchFilters

MAX_QUERY_LENGTH = 128


def _normalize_entity_node(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['summary'] = normalized.get('summary') or ''
    normalized['labels'] = [str(value) for value in parse_json_list(normalized.get('labels'))]
    normalized['attributes'] = parse_json_dict(normalized.get('attributes'))
    return normalized


def _normalize_entity_edge(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['name'] = normalized.get('name') or ''
    normalized['fact'] = normalized.get('fact') or ''
    normalized['episodes'] = [str(value) for value in parse_json_list(normalized.get('episodes'))]
    normalized['attributes'] = parse_json_dict(normalized.get('attributes'))
    return normalized


def _normalize_episode(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['entity_edges'] = [str(value) for value in parse_json_list(normalized.get('entity_edges'))]
    return normalized


def _normalize_community(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['summary'] = normalized.get('summary') or ''
    normalized['name_embedding'] = None
    return normalized


def _node_label_sql(search_filter: SearchFilters, column: str = 'labels') -> str:
    labels = search_filter.node_labels or []
    if not labels:
        return ''
    clauses = [f"{column} LIKE '%\"{label}\"%'" for label in labels]
    return ' AND (' + ' OR '.join(clauses) + ')'


def _edge_type_sql(search_filter: SearchFilters, column: str = 'name') -> str:
    edge_types = search_filter.edge_types or []
    if not edge_types:
        return ''
    return f' AND {column} IN {sql_in_list(edge_types)}'


class OraclePGSearchOperations(SearchOperations):
    async def node_fulltext_search(
        self,
        executor: QueryExecutor,
        query: str,
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[EntityNode]:
        fulltext_query = self.build_fulltext_query(query, group_ids, MAX_QUERY_LENGTH)
        if fulltext_query == '':
            return []
        table = get_table_name(executor, 'entity_nodes')
        group_clause = f' AND group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        label_clause = _node_label_sql(search_filter)
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              name,
              group_id,
              created_at,
              summary,
              labels,
              attributes,
              SCORE(1) AS score
            FROM {table}
            WHERE CONTAINS(name, '{fulltext_query}', 1) > 0
            {group_clause}
            {label_clause}
            ORDER BY score DESC, uuid DESC
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
        )
        return [entity_node_from_record(_normalize_entity_node(record)) for record in records]

    async def node_similarity_search(
        self,
        executor: QueryExecutor,
        search_vector: list[float],
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
        min_score: float = 0.6,
    ) -> list[EntityNode]:
        table = get_table_name(executor, 'entity_nodes')
        group_clause = f' AND group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        label_clause = _node_label_sql(search_filter)
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              name,
              group_id,
              created_at,
              summary,
              labels,
              attributes,
              1 - COSINE_DISTANCE(name_embedding, TO_VECTOR($query_vec)) AS score
            FROM {table}
            WHERE name_embedding IS NOT NULL
            {group_clause}
            {label_clause}
            AND 1 - COSINE_DISTANCE(name_embedding, TO_VECTOR($query_vec)) >= $min_score
            ORDER BY score DESC
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
            query_vec=json.dumps(search_vector),
            min_score=min_score,
        )
        return [entity_node_from_record(_normalize_entity_node(record)) for record in records]

    async def node_bfs_search(
        self,
        executor: QueryExecutor,
        origin_uuids: list[str],
        search_filter: SearchFilters,
        max_depth: int,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[EntityNode]:
        if not origin_uuids or max_depth < 1:
            return []
        # edge_table = get_table_name(executor, 'entity_edges')
        # visited = set(origin_uuids)
        # frontier = set(origin_uuids)
        # for _ in range(max_depth):
        #     if not frontier:
        #         break
        #     rows = await run_query(
        #         executor,
        #         f"""
        #         SELECT src_uuid, dst_uuid
        #         FROM {edge_table}
        #         WHERE src_uuid IN {sql_in_list(list(frontier))}
        #            OR dst_uuid IN {sql_in_list(list(frontier))}
        #         """,
        #     )
        #     next_frontier: set[str] = set()
        #     for row in rows:
        #         src = row['src_uuid']
        #         dst = row['dst_uuid']
        #         if src not in visited:
        #             next_frontier.add(src)
        #         if dst not in visited:
        #             next_frontier.add(dst)
        #     visited.update(next_frontier)
        #     frontier = next_frontier
        #
        # table = get_table_name(executor, 'entity_nodes')
        # group_clause = f' AND group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        # label_clause = _node_label_sql(search_filter)
        # records = await run_query(
        #     executor,
        #     f"""
        #     SELECT uuid, name, group_id, created_at, summary, labels, attributes
        #     FROM {table}
        #     WHERE uuid IN {sql_in_list(list(visited))}
        #     {group_clause}
        #     {label_clause}
        #     FETCH FIRST {int(limit)} ROWS ONLY
        #     """,
        # )
        # return [entity_node_from_record(_normalize_entity_node(record)) for record in records]
        table = get_table_name(executor, 'entity_nodes')
        graph_name = get_property_graph_name(get_graph_id_for_executor(executor))
        where_parts = []
        if group_ids:
            where_parts.append(f'entity_node.group_id IN {sql_in_list(group_ids)}')
        origin_group_clause = f' AND origin.group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        label_clause = _node_label_sql(search_filter, 'entity_node.labels').removeprefix(' AND ')
        if label_clause:
            where_parts.append(label_clause)
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ''
        records = await run_query(
            executor,
            f"""
            SELECT DISTINCT
              entity_node.uuid,
              entity_node.name,
              entity_node.group_id,
              entity_node.created_at,
              entity_node.summary,
              entity_node.labels,
              entity_node.attributes
            FROM GRAPH_TABLE (
              {graph_name}
              MATCH (origin IS Entity) -[IS RELATES_TO|MENTIONS]->{{1, {int(max_depth)}}} (target IS Entity)
              WHERE origin.uuid IN {sql_in_list(origin_uuids)}
                AND target.group_id = origin.group_id
                {origin_group_clause}
              COLUMNS (target.uuid AS node_uuid)
            ) path_nodes
            JOIN {table} entity_node ON entity_node.uuid = path_nodes.node_uuid
            {where_clause}
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
        )
        return [entity_node_from_record(_normalize_entity_node(record)) for record in records]

    async def edge_fulltext_search(
        self,
        executor: QueryExecutor,
        query: str,
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[EntityEdge]:
        fulltext_query = self.build_fulltext_query(query, group_ids, MAX_QUERY_LENGTH)
        if fulltext_query == '':
            return []
        table = get_table_name(executor, 'entity_edges')
        group_clause = f' AND group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        edge_type_clause = _edge_type_sql(search_filter)
        uuid_clause = (
            f' AND uuid IN {sql_in_list(search_filter.edge_uuids)}' if search_filter.edge_uuids else ''
        )
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              src_uuid AS source_node_uuid,
              dst_uuid AS target_node_uuid,
              group_id,
              created_at,
              name,
              fact_text AS fact,
              episodes,
              valid_at,
              invalid_at,
              expired_at,
              attributes,
              SCORE(1) AS score
            FROM {table}
            WHERE CONTAINS(name, '{fulltext_query}', 1) > 0
            {group_clause}
            {edge_type_clause}
            {uuid_clause}
            ORDER BY score DESC, uuid DESC
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
        )
        return [entity_edge_from_record(_normalize_entity_edge(record)) for record in records]

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
        table = get_table_name(executor, 'entity_edges')
        where_parts = ['fact_embedding IS NOT NULL']
        if group_ids:
            where_parts.append(f'group_id IN {sql_in_list(group_ids)}')
        if source_node_uuid:
            where_parts.append(f'src_uuid = {sql_string_literal(source_node_uuid)}')
        if target_node_uuid:
            where_parts.append(f'dst_uuid = {sql_string_literal(target_node_uuid)}')
        if search_filter.edge_uuids:
            where_parts.append(f'uuid IN {sql_in_list(search_filter.edge_uuids)}')
        if search_filter.edge_types:
            where_parts.append(f'name IN {sql_in_list(search_filter.edge_types)}')
        where_parts.append('1 - COSINE_DISTANCE(fact_embedding, TO_VECTOR($query_vec)) >= $min_score')
        where_clause = ' AND '.join(where_parts)
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              src_uuid AS source_node_uuid,
              dst_uuid AS target_node_uuid,
              group_id,
              created_at,
              name,
              fact_text AS fact,
              episodes,
              valid_at,
              invalid_at,
              expired_at,
              attributes,
              1 - COSINE_DISTANCE(fact_embedding, TO_VECTOR($query_vec)) AS score
            FROM {table}
            WHERE {where_clause}
            ORDER BY score DESC
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
            query_vec=json.dumps(search_vector),
            min_score=min_score,
        )
        return [entity_edge_from_record(_normalize_entity_edge(record)) for record in records]

    async def edge_bfs_search(
        self,
        executor: QueryExecutor,
        origin_uuids: list[str],
        max_depth: int,
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[EntityEdge]:
        if not origin_uuids:
            return []
        table = get_table_name(executor, 'entity_edges')
        graph_name = get_property_graph_name(get_graph_id_for_executor(executor))
        depth = max(0, int(max_depth))
        if depth == 0:
            return []
        # visited_nodes = set(origin_uuids)
        # frontier = set(origin_uuids)
        # edge_ids: set[str] = set()
        # for _ in range(max_depth):
        #     if not frontier:
        #         break
        #     records = await run_query(
        #         executor,
        #         f"""
        #         SELECT uuid, src_uuid, dst_uuid, group_id
        #         FROM {table}
        #         WHERE src_uuid IN {sql_in_list(list(frontier))}
        #            OR dst_uuid IN {sql_in_list(list(frontier))}
        #         """,
        #     )
        #     next_frontier: set[str] = set()
        #     for record in records:
        #         if group_ids and record.get('group_id') not in group_ids:
        #             continue
        #         edge_ids.add(record['uuid'])
        #         src = record['src_uuid']
        #         dst = record['dst_uuid']
        #         if src not in visited_nodes:
        #             next_frontier.add(src)
        #         if dst not in visited_nodes:
        #             next_frontier.add(dst)
        #     visited_nodes.update(next_frontier)
        #     frontier = next_frontier
        # if not edge_ids:
        #     return []
        # edges = await self.get_edges_by_ids(executor, list(edge_ids), limit, search_filter)
        # return edges
        path_edge_group_clause = (
            f' WHERE path_edge.group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        )
        where_parts = ['1 = 1']
        if group_ids:
            where_parts.append(f'entity_edge.group_id IN {sql_in_list(group_ids)}')
        if search_filter.edge_uuids:
            where_parts.append(f'entity_edge.uuid IN {sql_in_list(search_filter.edge_uuids)}')
        if search_filter.edge_types:
            where_parts.append(f'entity_edge.name IN {sql_in_list(search_filter.edge_types)}')
        where_clause = ' AND '.join(where_parts)

        records = await run_query(
            executor,
            f"""
            SELECT DISTINCT
              entity_edge.uuid,
              entity_edge.src_uuid AS source_node_uuid,
              entity_edge.dst_uuid AS target_node_uuid,
              entity_edge.group_id,
              entity_edge.created_at,
              entity_edge.name,
              entity_edge.fact_text AS fact,
              entity_edge.episodes,
              entity_edge.valid_at,
              entity_edge.invalid_at,
              entity_edge.expired_at,
              entity_edge.attributes
            FROM GRAPH_TABLE (
              {graph_name}
              MATCH (origin) -[path_edge IS RELATES_TO|MENTIONS{path_edge_group_clause}]->{{1, {depth}}} (target IS Entity)
              WHERE origin.uuid IN {sql_in_list(origin_uuids)}
              ONE ROW PER STEP (step_src, step_edge, step_dst)
              COLUMNS (step_edge.uuid AS edge_uuid)
            ) path_edges
            JOIN {table} entity_edge ON entity_edge.uuid = path_edges.edge_uuid
            WHERE {where_clause}
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
        )
        return [entity_edge_from_record(_normalize_entity_edge(record)) for record in records]

    async def episode_fulltext_search(
        self,
        executor: QueryExecutor,
        query: str,
        search_filter: SearchFilters,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[EpisodicNode]:
        fulltext_query = self.build_fulltext_query(query, group_ids, MAX_QUERY_LENGTH)
        if fulltext_query == '':
            return []
        table = get_table_name(executor, 'episodic_nodes')
        group_clause = f' AND group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              source,
              source_description,
              content,
              entity_edges,
              created_at,
              valid_at,
              SCORE(1) AS score
            FROM {table}
            WHERE CONTAINS(content, '{fulltext_query}', 1) > 0
            {group_clause}
            ORDER BY score DESC, uuid DESC
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
        )
        return [episodic_node_from_record(_normalize_episode(record)) for record in records]

    async def community_fulltext_search(
        self,
        executor: QueryExecutor,
        query: str,
        group_ids: list[str] | None = None,
        limit: int = 10,
    ) -> list[CommunityNode]:
        fulltext_query = self.build_fulltext_query(query, group_ids, MAX_QUERY_LENGTH)
        if fulltext_query == '':
            return []
        table = get_table_name(executor, 'community_nodes')
        group_clause = f' AND group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              summary,
              created_at,
              SCORE(1) AS score
            FROM {table}
            WHERE CONTAINS(name, '{fulltext_query}', 1) > 0
            {group_clause}
            ORDER BY score DESC, uuid DESC
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
        )
        return [community_node_from_record(_normalize_community(record)) for record in records]

    async def community_similarity_search(
        self,
        executor: QueryExecutor,
        search_vector: list[float],
        group_ids: list[str] | None = None,
        limit: int = 10,
        min_score: float = 0.6,
    ) -> list[CommunityNode]:
        table = get_table_name(executor, 'community_nodes')
        group_clause = f' AND group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              group_id,
              name,
              summary,
              created_at,
              1 - COSINE_DISTANCE(name_embedding, TO_VECTOR($query_vec)) AS score
            FROM {table}
            WHERE name_embedding IS NOT NULL
            {group_clause}
            AND 1 - COSINE_DISTANCE(name_embedding, TO_VECTOR($query_vec)) >= $min_score
            ORDER BY score DESC
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
            query_vec=json.dumps(search_vector),
            min_score=min_score,
        )
        return [community_node_from_record(_normalize_community(record)) for record in records]

    async def node_distance_reranker(
        self,
        executor: QueryExecutor,
        node_uuids: list[str],
        center_node_uuid: str,
        min_score: float = 0,
    ) -> list[EntityNode]:
        if not node_uuids:
            return []
        edge_table = get_table_name(executor, 'entity_edges')
        remaining = set(node_uuids)
        visited = {center_node_uuid}
        frontier = {center_node_uuid}
        depth = 0
        score_by_uuid: dict[str, float] = {}
        while frontier and remaining:
            rows = await run_query(
                executor,
                f"""
                SELECT src_uuid, dst_uuid
                FROM {edge_table}
                WHERE src_uuid IN {sql_in_list(list(frontier))}
                   OR dst_uuid IN {sql_in_list(list(frontier))}
                """,
            )
            next_frontier: set[str] = set()
            for row in rows:
                src = row['src_uuid']
                dst = row['dst_uuid']
                neighbors = [src, dst]
                for neighbor in neighbors:
                    if neighbor in visited:
                        continue
                    visited.add(neighbor)
                    next_frontier.add(neighbor)
                    if neighbor in remaining:
                        score = 1.0 / float(depth + 2)
                        if score >= min_score:
                            score_by_uuid[neighbor] = score
                        remaining.remove(neighbor)
            frontier = next_frontier
            depth += 1

        ordered = sorted(score_by_uuid.items(), key=lambda item: item[1], reverse=True)
        if not ordered:
            return []
        ordered_ids = [uuid for uuid, _ in ordered]
        node_map = {
            node.uuid: node for node in await self._get_nodes_by_ids(executor, ordered_ids, limit=len(ordered_ids))
        }
        return [node_map[uuid] for uuid in ordered_ids if uuid in node_map]

    async def episode_mentions_reranker(
        self,
        executor: QueryExecutor,
        node_uuids: list[str],
        min_score: float = 0,
    ) -> list[EntityNode]:
        if not node_uuids:
            return []
        edge_table = get_table_name(executor, 'episodic_edges')
        rows = await run_query(
            executor,
            f"""
            SELECT target_node_uuid AS uuid, COUNT(*) AS mentions
            FROM {edge_table}
            WHERE target_node_uuid IN {sql_in_list(node_uuids)}
            GROUP BY target_node_uuid
            ORDER BY mentions DESC
            """,
        )
        ranked_ids = [
            row['uuid'] for row in rows if float(row.get('mentions') or 0) >= float(min_score or 0)
        ]
        if not ranked_ids:
            return []
        node_map = {
            node.uuid: node for node in await self._get_nodes_by_ids(executor, ranked_ids, limit=len(ranked_ids))
        }
        return [node_map[uuid] for uuid in ranked_ids if uuid in node_map]

    def build_node_search_filters(self, search_filters: SearchFilters) -> Any:
        clauses: list[str] = []
        if search_filters.node_labels:
            clauses.append('labels filter')
        return clauses

    def build_edge_search_filters(self, search_filters: SearchFilters) -> Any:
        clauses: list[str] = []
        if search_filters.edge_types:
            clauses.append('edge type filter')
        return clauses

    def build_fulltext_query(
        self,
        query: str,
        group_ids: list[str] | None = None,
        max_query_length: int = 128,
    ) -> str:
        """Build Oracle ``CONTAINS`` query text. Partitioning uses SQL ``group_id IN (...)``, not Text sections.

        ``max_query_length`` is kept for API compatibility with callers; term length is bounded in
        :func:`~graphiti_core.utils.keyword_extractor.build_fulltext_terms_from_query`.
        """
        validate_group_ids(group_ids)

        lucene_query = build_fulltext_terms_from_query(query)
        if not lucene_query.strip():
            return ''

        return '(' + lucene_query + ')'

    async def _get_nodes_by_ids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        limit: int,
    ) -> list[EntityNode]:
        if not uuids:
            return []
        table = get_table_name(executor, 'entity_nodes')
        records = await run_query(
            executor,
            f"""
            SELECT uuid, name, group_id, created_at, summary, labels, attributes
            FROM {table}
            WHERE uuid IN {sql_in_list(uuids)}
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
        )
        return [entity_node_from_record(_normalize_entity_node(record)) for record in records]

    async def get_edges_by_ids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        limit: int,
        search_filter: SearchFilters,
    ) -> list[EntityEdge]:
        if not uuids:
            return []
        table = get_table_name(executor, 'entity_edges')
        edge_type_clause = _edge_type_sql(search_filter)
        uuid_clause = f'uuid IN {sql_in_list(uuids)}'
        records = await run_query(
            executor,
            f"""
            SELECT
              uuid,
              src_uuid AS source_node_uuid,
              dst_uuid AS target_node_uuid,
              group_id,
              created_at,
              name,
              fact_text AS fact,
              episodes,
              valid_at,
              invalid_at,
              expired_at,
              attributes
            FROM {table}
            WHERE {uuid_clause}
            {edge_type_clause}
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
        )
        return [entity_edge_from_record(_normalize_entity_edge(record)) for record in records]
