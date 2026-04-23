"""
Oracle maintenance stubs (temporary).
"""

from __future__ import annotations

from datetime import datetime

from graphiti_core.driver.driver import GraphDriver
from graphiti_core.nodes import EpisodeType, EpisodicNode


async def clear_data(driver: GraphDriver, group_ids: list[str] | None = None) -> None:
    return None


async def retrieve_episodes(
    driver: GraphDriver,
    reference_time: datetime,
    last_n: int = 3,
    group_ids: list[str] | None = None,
    source: EpisodeType | None = None,
    saga: str | None = None,
) -> list[EpisodicNode]:
    episode_ops = driver.episode_node_ops
    if episode_ops is None:
        return []

    episodes = await episode_ops.retrieve_episodes(
        driver,
        reference_time,
        last_n,
        group_ids,
        source.name if source is not None else None,
        saga,
    )
    # Keep compatibility with maintenance utils behavior (chronological order).
    return list(reversed(episodes))

