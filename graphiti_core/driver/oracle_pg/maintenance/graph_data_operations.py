"""
Oracle PG implementations for graph data maintenance helpers.
"""

from __future__ import annotations

from datetime import datetime

from graphiti_core.driver.oracle_pg.sql_utils import (
    get_table_name,
    parse_json_list,
    run_query,
    sql_in_list,
    sql_string_literal,
)
from graphiti_core.driver.record_parsers import episodic_node_from_record
from graphiti_core.driver.driver import GraphDriver
from graphiti_core.nodes import EpisodeType, EpisodicNode


def _normalize_episode_record(record: dict) -> dict:
    normalized = dict(record)
    normalized['entity_edges'] = [str(value) for value in parse_json_list(normalized.get('entity_edges'))]
    return normalized


async def clear_data(driver: GraphDriver, group_ids: list[str] | None = None) -> None:
    # Mirror utils/maintenance fallback semantics:
    # - no group filter => wipe all graph data
    # - group filter => delete Entity/Episodic/Community scoped data only
    if group_ids is None:
        graph_ops = driver.graph_ops
        if graph_ops is None:
            return None
        await graph_ops.clear_data(driver, None)
        return None

    entity_node_ops = driver.entity_node_ops
    episode_node_ops = driver.episode_node_ops
    community_node_ops = driver.community_node_ops
    if entity_node_ops is None or episode_node_ops is None or community_node_ops is None:
        return None

    for group_id in group_ids:
        await entity_node_ops.delete_by_group_id(driver, group_id)
        await episode_node_ops.delete_by_group_id(driver, group_id)
        await community_node_ops.delete_by_group_id(driver, group_id)
    return None


async def retrieve_episodes(
    driver: GraphDriver,
    reference_time: datetime,
    last_n: int = 3,
    group_ids: list[str] | None = None,
    source: EpisodeType | None = None,
    saga: str | None = None,
) -> list[EpisodicNode]:
    node_table = get_table_name(driver, 'episodic_nodes')

    if saga is not None:
        has_episode_table = get_table_name(driver, 'has_episode_edges')
        saga_table = get_table_name(driver, 'saga_nodes')
        group_id = group_ids[0] if group_ids else None
        source_filter = f'AND n.source = {sql_string_literal(source.name)}' if source else ''
        records = await run_query(
            driver,
            f"""
            SELECT
              n.uuid,
              n.group_id,
              n.name,
              n.source,
              n.source_description,
              n.content,
              n.entity_edges,
              n.created_at,
              n.valid_at
            FROM {saga_table} s
            JOIN {has_episode_table} h ON h.source_node_uuid = s.uuid
            JOIN {node_table} n ON n.uuid = h.target_node_uuid
            WHERE s.name = $saga_name
              AND s.group_id = $group_id
              AND n.valid_at <= $reference_time
              {source_filter}
            ORDER BY n.valid_at DESC
            FETCH FIRST {int(last_n)} ROWS ONLY
            """,
            saga_name=saga,
            group_id=group_id,
            reference_time=reference_time,
        )
        return [episodic_node_from_record(_normalize_episode_record(record)) for record in reversed(records)]

    conditions = ['n.valid_at <= $reference_time']
    if group_ids:
        conditions.append(f'n.group_id IN {sql_in_list(group_ids)}')
    if source:
        conditions.append(f'n.source = {sql_string_literal(source.name)}')
    where_clause = ' AND '.join(conditions)
    records = await run_query(
        driver,
        f"""
        SELECT
          n.uuid,
          n.group_id,
          n.name,
          n.source,
          n.source_description,
          n.content,
          n.entity_edges,
          n.created_at,
          n.valid_at
        FROM {node_table} n
        WHERE {where_clause}
        ORDER BY n.valid_at DESC
        FETCH FIRST {int(last_n)} ROWS ONLY
        """,
        reference_time=reference_time,
    )
    return [episodic_node_from_record(_normalize_episode_record(record)) for record in reversed(records)]

