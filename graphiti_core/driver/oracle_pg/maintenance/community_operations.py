"""
Oracle PG implementations for community maintenance helpers.
"""

from __future__ import annotations

from collections import defaultdict

from graphiti_core.driver.driver import GraphDriver
from graphiti_core.driver.oracle_pg.sql_utils import get_table_name, parse_float_list, run_query
from graphiti_core.driver.record_parsers import community_node_from_record
from graphiti_core.nodes import CommunityNode, EntityNode
from graphiti_core.utils.maintenance.community_operations import Neighbor, label_propagation


def _normalize_community_record(record: dict) -> dict:
    normalized = dict(record)
    normalized['summary'] = normalized.get('summary') or ''
    normalized['name_embedding'] = parse_float_list(normalized.get('name_embedding'))
    return normalized


async def get_community_clusters(
    driver: GraphDriver, group_ids: list[str] | None
) -> list[list[EntityNode]]:
    entity_node_ops = driver.entity_node_ops
    if entity_node_ops is None:
        return []
    entity_edges_table = get_table_name(driver, 'entity_edges')
    entity_nodes_table = get_table_name(driver, 'entity_nodes')
    community_clusters: list[list[EntityNode]] = []

    if group_ids is None:
        records = await run_query(
            driver,
            f"""
            SELECT DISTINCT group_id
            FROM {entity_nodes_table}
            WHERE group_id IS NOT NULL
            """,
        )
        group_ids = [record['group_id'] for record in records if record.get('group_id') is not None]

    for group_id in group_ids or []:
        projection: dict[str, list[Neighbor]] = {}
        nodes = await entity_node_ops.get_by_group_ids(driver, [group_id])
        for node in nodes:
            neighbor_records = await run_query(
                driver,
                f"""
                SELECT
                  CASE
                    WHEN src_uuid = $uuid THEN dst_uuid
                    ELSE src_uuid
                  END AS uuid,
                  COUNT(*) AS count
                FROM {entity_edges_table}
                WHERE group_id = $group_id
                  AND (src_uuid = $uuid OR dst_uuid = $uuid)
                GROUP BY
                  CASE
                    WHEN src_uuid = $uuid THEN dst_uuid
                    ELSE src_uuid
                  END
                """,
                uuid=node.uuid,
                group_id=group_id,
            )
            projection[node.uuid] = [
                Neighbor(node_uuid=record['uuid'], edge_count=int(record['count']))
                for record in neighbor_records
                if record.get('uuid') is not None
            ]

        cluster_uuids = label_propagation(projection)
        for cluster in cluster_uuids:
            community_clusters.append(await entity_node_ops.get_by_uuids(driver, cluster))

    return community_clusters


async def remove_communities(driver: GraphDriver) -> None:
    graph_ops = driver.graph_ops
    if graph_ops is not None:
        await graph_ops.remove_communities(driver)
    return None


async def determine_entity_community(
    driver: GraphDriver, entity: EntityNode
) -> tuple[CommunityNode | None, bool]:
    community_nodes = get_table_name(driver, 'community_nodes')
    community_edges = get_table_name(driver, 'community_edges')
    entity_edges = get_table_name(driver, 'entity_edges')

    existing = await run_query(
        driver,
        f"""
        SELECT
          c.uuid,
          c.group_id,
          c.name,
          c.summary,
          c.created_at,
          c.name_embedding
        FROM {community_nodes} c
        JOIN {community_edges} ce ON ce.source_node_uuid = c.uuid
        WHERE ce.target_node_uuid = $entity_uuid
        FETCH FIRST 1 ROWS ONLY
        """,
        entity_uuid=entity.uuid,
    )
    if existing:
        return community_node_from_record(_normalize_community_record(existing[0])), False

    neighbor_communities = await run_query(
        driver,
        f"""
        SELECT
          c.uuid,
          c.group_id,
          c.name,
          c.summary,
          c.created_at,
          c.name_embedding
        FROM {community_nodes} c
        JOIN {community_edges} ce ON ce.source_node_uuid = c.uuid
        JOIN {entity_edges} ee
          ON (
            (ee.src_uuid = ce.target_node_uuid AND ee.dst_uuid = $entity_uuid)
            OR
            (ee.dst_uuid = ce.target_node_uuid AND ee.src_uuid = $entity_uuid)
          )
        """,
        entity_uuid=entity.uuid,
    )
    communities = [
        community_node_from_record(_normalize_community_record(record)) for record in neighbor_communities
    ]
    if not communities:
        return None, False

    community_counts: dict[str, int] = defaultdict(int)
    for community in communities:
        community_counts[community.uuid] += 1
    selected_uuid = max(community_counts, key=lambda uuid: community_counts[uuid])
    for community in communities:
        if community.uuid == selected_uuid:
            return community, True

    return None, False

