"""
Oracle PG graph maintenance operations.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from graphiti_core.driver.operations.graph_ops import GraphMaintenanceOperations
from graphiti_core.driver.oracle_pg.sql_utils import (
    get_graph_id_for_executor,
    get_property_graph_create_block,
    get_property_graph_drop_block,
    get_table_ddl_blocks,
    get_table_drop_blocks,
    get_table_name,
    parse_json_dict,
    parse_json_list,
    run_query,
    sql_in_list,
)
from graphiti_core.driver.query_executor import QueryExecutor
from graphiti_core.driver.record_parsers import community_node_from_record, entity_node_from_record
from graphiti_core.nodes import CommunityNode, EntityNode, EpisodicNode


def _normalize_entity_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['summary'] = normalized.get('summary') or ''
    normalized['labels'] = [str(value) for value in parse_json_list(normalized.get('labels'))]
    normalized['attributes'] = parse_json_dict(normalized.get('attributes'))
    return normalized


def _normalize_community_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    normalized['summary'] = normalized.get('summary') or ''
    normalized['name_embedding'] = None
    return normalized


class OraclePGGraphMaintenanceOperations(GraphMaintenanceOperations):
    async def clear_data(
        self,
        executor: QueryExecutor,
        group_ids: list[str] | None = None,
    ) -> None:
        entity_nodes = get_table_name(executor, 'entity_nodes')
        episodic_nodes = get_table_name(executor, 'episodic_nodes')
        community_nodes = get_table_name(executor, 'community_nodes')
        saga_nodes = get_table_name(executor, 'saga_nodes')
        entity_edges = get_table_name(executor, 'entity_edges')
        episodic_edges = get_table_name(executor, 'episodic_edges')
        community_edges = get_table_name(executor, 'community_edges')
        has_episode_edges = get_table_name(executor, 'has_episode_edges')
        next_episode_edges = get_table_name(executor, 'next_episode_edges')
        order = [
            entity_edges,
            episodic_edges,
            community_edges,
            has_episode_edges,
            next_episode_edges,
            community_nodes,
            saga_nodes,
            episodic_nodes,
            entity_nodes,
        ]
        where_clause = f' WHERE group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        for table in order:
            await run_query(executor, f'DELETE FROM {table}{where_clause}')

    async def build_indices_and_constraints(
        self,
        executor: QueryExecutor,
        delete_existing: bool = False,
        drop_tables: bool = False,
    ) -> None:
        graph_id = get_graph_id_for_executor(executor)
        if delete_existing:
            await self.delete_all_indexes(executor)
        if delete_existing or drop_tables:
            await run_query(executor, get_property_graph_drop_block(graph_id))
        if drop_tables:
            for block in get_table_drop_blocks(graph_id):
                await run_query(executor, block)

        for block in get_table_ddl_blocks(graph_id):
            await run_query(executor, block)
        await run_query(executor, get_property_graph_create_block(graph_id))

        table_defs = {
            'entity_nodes': ['group_id', 'name'],
            'episodic_nodes': ['group_id', 'valid_at'],
            'community_nodes': ['group_id', 'name'],
            'saga_nodes': ['group_id', 'name'],
            'entity_edges': ['group_id', 'src_uuid', 'dst_uuid'],
            'episodic_edges': ['group_id', 'source_node_uuid', 'target_node_uuid'],
            'community_edges': ['group_id', 'source_node_uuid', 'target_node_uuid'],
            'has_episode_edges': ['group_id', 'source_node_uuid', 'target_node_uuid'],
            'next_episode_edges': ['group_id', 'source_node_uuid', 'target_node_uuid'],
        }
        for base, columns in table_defs.items():
            table_name = get_table_name(executor, base)
            for column in columns:
                index_name = f'{graph_id}_{base}_{column}_IDX'.upper()[:120]
                block = f"""
                BEGIN
                  EXECUTE IMMEDIATE 'CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column.upper()})';
                EXCEPTION
                  WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                      RAISE;
                    END IF;
                END;
                """
                await run_query(executor, block)

    async def delete_all_indexes(
        self,
        executor: QueryExecutor,
    ) -> None:
        graph_id = get_graph_id_for_executor(executor)
        records = await run_query(
            executor,
            """
            SELECT index_name
            FROM user_indexes
            WHERE index_name LIKE $index_like
            """,
            index_like=f'{graph_id}\\_%\\_IDX',
        )
        for record in records:
            await run_query(executor, f'DROP INDEX {record["index_name"]}')

    async def get_community_clusters(
        self,
        executor: QueryExecutor,
        group_ids: list[str] | None = None,
    ) -> list[Any]:
        table = get_table_name(executor, 'entity_nodes')
        where_clause = f'WHERE group_id IN {sql_in_list(group_ids)}' if group_ids else ''
        records = await run_query(
            executor,
            f"""
            SELECT uuid, name, group_id, created_at, summary, labels, attributes
            FROM {table}
            {where_clause}
            ORDER BY group_id, uuid
            """,
        )
        buckets: defaultdict[str, list[EntityNode]] = defaultdict(list)
        for record in records:
            node = entity_node_from_record(_normalize_entity_record(record))
            buckets[node.group_id].append(node)
        return list(buckets.values())

    async def remove_communities(
        self,
        executor: QueryExecutor,
    ) -> None:
        await run_query(executor, f'DELETE FROM {get_table_name(executor, "community_edges")}')
        await run_query(executor, f'DELETE FROM {get_table_name(executor, "community_nodes")}')

    async def determine_entity_community(
        self,
        executor: QueryExecutor,
        entity: EntityNode,
    ) -> tuple[CommunityNode | None, bool]:
        community_nodes = get_table_name(executor, 'community_nodes')
        community_edges = get_table_name(executor, 'community_edges')
        entity_edges = get_table_name(executor, 'entity_edges')
        existing = await run_query(
            executor,
            f"""
            SELECT
              c.uuid,
              c.group_id,
              c.name,
              c.summary,
              c.created_at
            FROM {community_nodes} c
            JOIN {community_edges} ce ON ce.source_node_uuid = c.uuid
            WHERE ce.target_node_uuid = $entity_uuid
            FETCH FIRST 1 ROWS ONLY
            """,
            entity_uuid=entity.uuid,
        )
        if existing:
            return community_node_from_record(_normalize_community_record(existing[0])), False

        candidate_records = await run_query(
            executor,
            f"""
            SELECT
              ce.source_node_uuid AS community_uuid,
              COUNT(*) AS mention_count
            FROM {entity_edges} ee
            JOIN {community_edges} ce
              ON ce.target_node_uuid = CASE
                WHEN ee.src_uuid = $entity_uuid THEN ee.dst_uuid
                ELSE ee.src_uuid
              END
            WHERE ee.src_uuid = $entity_uuid OR ee.dst_uuid = $entity_uuid
            GROUP BY ce.source_node_uuid
            ORDER BY mention_count DESC
            FETCH FIRST 1 ROWS ONLY
            """,
            entity_uuid=entity.uuid,
        )
        if not candidate_records:
            return None, False
        community_uuid = candidate_records[0]['community_uuid']
        selected = await run_query(
            executor,
            f"""
            SELECT uuid, group_id, name, summary, created_at
            FROM {community_nodes}
            WHERE uuid = $community_uuid
            """,
            community_uuid=community_uuid,
        )
        if not selected:
            return None, False
        return community_node_from_record(_normalize_community_record(selected[0])), True

    async def get_mentioned_nodes(
        self,
        executor: QueryExecutor,
        episodes: list[EpisodicNode],
    ) -> list[EntityNode]:
        if not episodes:
            return []
        episode_uuids = [episode.uuid for episode in episodes]
        entity_nodes = get_table_name(executor, 'entity_nodes')
        episodic_edges = get_table_name(executor, 'episodic_edges')
        records = await run_query(
            executor,
            f"""
            SELECT DISTINCT
              n.uuid,
              n.name,
              n.group_id,
              n.created_at,
              n.summary,
              n.labels,
              n.attributes
            FROM {entity_nodes} n
            JOIN {episodic_edges} e ON e.target_node_uuid = n.uuid
            WHERE e.source_node_uuid IN {sql_in_list(episode_uuids)}
            """,
        )
        return [entity_node_from_record(_normalize_entity_record(record)) for record in records]

    async def get_communities_by_nodes(
        self,
        executor: QueryExecutor,
        nodes: list[EntityNode],
    ) -> list[CommunityNode]:
        if not nodes:
            return []
        node_uuids = [node.uuid for node in nodes]
        community_nodes = get_table_name(executor, 'community_nodes')
        community_edges = get_table_name(executor, 'community_edges')
        records = await run_query(
            executor,
            f"""
            SELECT DISTINCT
              c.uuid,
              c.group_id,
              c.name,
              c.summary,
              c.created_at
            FROM {community_nodes} c
            JOIN {community_edges} ce ON ce.source_node_uuid = c.uuid
            WHERE ce.target_node_uuid IN {sql_in_list(node_uuids)}
            """,
        )
        return [community_node_from_record(_normalize_community_record(record)) for record in records]
