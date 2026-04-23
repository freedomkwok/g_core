"""
Oracle PG implementations for edge maintenance helpers.
"""

from __future__ import annotations

from graphiti_core.driver.driver import GraphDriver
from graphiti_core.helpers import semaphore_gather
from graphiti_core.nodes import EntityNode


async def filter_existing_duplicate_of_edges(
    driver: GraphDriver, duplicates_node_tuples: list[tuple[EntityNode, EntityNode]]
) -> list[tuple[EntityNode, EntityNode]]:
    if not duplicates_node_tuples:
        return []

    if driver.entity_edge_ops is None:
        return duplicates_node_tuples

    duplicate_nodes_map = {
        (source.uuid, target.uuid): (source, target) for source, target in duplicates_node_tuples
    }
    duplicate_keys = list(duplicate_nodes_map.keys())
    edge_results = await semaphore_gather(
        *[
            driver.entity_edge_ops.get_between_nodes(driver, source_uuid, target_uuid)
            for source_uuid, target_uuid in duplicate_keys
        ]
    )

    records: list[dict[str, str]] = []
    for (source_uuid, target_uuid), edges in zip(duplicate_keys, edge_results, strict=True):
        if any(edge.name == 'IS_DUPLICATE_OF' for edge in edges):
            records.append({'source_uuid': source_uuid, 'target_uuid': target_uuid})

    # Follow utils/maintenance filtering pattern: process returned records
    # and remove already-existing IS_DUPLICATE_OF relationships.
    for record in records:
        duplicate_tuple = (record.get('source_uuid'), record.get('target_uuid'))
        if duplicate_nodes_map.get(duplicate_tuple):
            duplicate_nodes_map.pop(duplicate_tuple)

    return list(duplicate_nodes_map.values())

