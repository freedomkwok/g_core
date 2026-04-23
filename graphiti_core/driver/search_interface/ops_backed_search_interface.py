"""
SearchInterface adapter backed by driver.search_ops.
"""

from __future__ import annotations

from typing import Any

from graphiti_core.driver.search_interface.search_interface import SearchInterface


class OpsBackedSearchInterface(SearchInterface):
    @staticmethod
    def _search_ops(driver: Any) -> Any:
        search_ops = getattr(driver, 'search_ops', None)
        if search_ops is None:
            raise NotImplementedError('search_ops are not configured on this driver')
        return search_ops

    async def edge_fulltext_search(
        self,
        driver: Any,
        query: str,
        search_filter: Any,
        group_ids: list[str] | None = None,
        limit: int = 100,
    ) -> list[Any]:
        return await self._search_ops(driver).edge_fulltext_search(
            driver, query, search_filter, group_ids, limit
        )

    async def edge_similarity_search(
        self,
        driver: Any,
        search_vector: list[float],
        source_node_uuid: str | None,
        target_node_uuid: str | None,
        search_filter: Any,
        group_ids: list[str] | None = None,
        limit: int = 100,
        min_score: float = 0.7,
    ) -> list[Any]:
        return await self._search_ops(driver).edge_similarity_search(
            driver,
            search_vector,
            source_node_uuid,
            target_node_uuid,
            search_filter,
            group_ids,
            limit,
            min_score,
        )

    async def node_fulltext_search(
        self,
        driver: Any,
        query: str,
        search_filter: Any,
        group_ids: list[str] | None = None,
        limit: int = 100,
    ) -> list[Any]:
        return await self._search_ops(driver).node_fulltext_search(
            driver, query, search_filter, group_ids, limit
        )

    async def node_similarity_search(
        self,
        driver: Any,
        search_vector: list[float],
        search_filter: Any,
        group_ids: list[str] | None = None,
        limit: int = 100,
        min_score: float = 0.7,
    ) -> list[Any]:
        return await self._search_ops(driver).node_similarity_search(
            driver, search_vector, search_filter, group_ids, limit, min_score
        )

    async def episode_fulltext_search(
        self,
        driver: Any,
        query: str,
        search_filter: Any,
        group_ids: list[str] | None = None,
        limit: int = 100,
    ) -> list[Any]:
        return await self._search_ops(driver).episode_fulltext_search(
            driver,
            query,
            search_filter,
            group_ids,
            limit,
        )

    async def edge_bfs_search(
        self,
        driver: Any,
        bfs_origin_node_uuids: list[str] | None,
        bfs_max_depth: int,
        search_filter: Any,
        group_ids: list[str] | None = None,
        limit: int = 100,
    ) -> list[Any]:
        return await self._search_ops(driver).edge_bfs_search(
            driver, bfs_origin_node_uuids, bfs_max_depth, search_filter, group_ids, limit
        )

    async def node_bfs_search(
        self,
        driver: Any,
        bfs_origin_node_uuids: list[str] | None,
        search_filter: Any,
        bfs_max_depth: int,
        group_ids: list[str] | None = None,
        limit: int = 100,
    ) -> list[Any]:
        return await self._search_ops(driver).node_bfs_search(
            driver, bfs_origin_node_uuids, search_filter, bfs_max_depth, group_ids, limit
        )

    async def community_fulltext_search(
        self,
        driver: Any,
        query: str,
        group_ids: list[str] | None = None,
        limit: int = 100,
    ) -> list[Any]:
        return await self._search_ops(driver).community_fulltext_search(
            driver, query, group_ids, limit
        )

    async def community_similarity_search(
        self,
        driver: Any,
        search_vector: list[float],
        group_ids: list[str] | None = None,
        limit: int = 100,
        min_score: float = 0.6,
    ) -> list[Any]:
        return await self._search_ops(driver).community_similarity_search(
            driver, search_vector, group_ids, limit, min_score
        )

    async def get_embeddings_for_communities(
        self,
        driver: Any,
        communities: list[Any],
    ) -> dict[str, list[float]]:
        community_node_ops = getattr(driver, 'community_node_ops', None)
        if community_node_ops is None:
            raise NotImplementedError('community_node_ops are not configured on this driver')
        for community in communities:
            await community_node_ops.load_name_embedding(driver, community)
        return {
            community.uuid: community.name_embedding
            for community in communities
            if getattr(community, 'name_embedding', None) is not None
        }

    async def node_distance_reranker(
        self,
        driver: Any,
        node_uuids: list[str],
        center_node_uuid: str,
        min_score: float = 0,
    ) -> tuple[list[str], list[float]]:
        nodes = await self._search_ops(driver).node_distance_reranker(
            driver, node_uuids, center_node_uuid, min_score
        )
        return [node.uuid for node in nodes], [1.0] * len(nodes)

    async def episode_mentions_reranker(
        self,
        driver: Any,
        node_uuids: list[list[str]],
        min_score: float = 0,
    ) -> tuple[list[str], list[float]]:
        flattened = [uuid for node_uuid_list in node_uuids for uuid in node_uuid_list]
        nodes = await self._search_ops(driver).episode_mentions_reranker(
            driver, flattened, min_score
        )
        return [node.uuid for node in nodes], [1.0] * len(nodes)

    def build_node_search_filters(self, search_filters: Any) -> Any:
        return []

    def build_edge_search_filters(self, search_filters: Any) -> Any:
        return []
