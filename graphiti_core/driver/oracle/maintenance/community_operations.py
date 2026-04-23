"""
Oracle community maintenance stubs (temporary).
"""

from __future__ import annotations

from graphiti_core.driver.driver import GraphDriver
from graphiti_core.nodes import CommunityNode, EntityNode


async def get_community_clusters(
    driver: GraphDriver, group_ids: list[str] | None
) -> list[list[EntityNode]]:
    return []


async def remove_communities(driver: GraphDriver) -> None:
    return None


async def determine_entity_community(
    driver: GraphDriver, entity: EntityNode
) -> tuple[CommunityNode | None, bool]:
    return None, False

