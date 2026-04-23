"""
Oracle PG graph operations adapter.

This adapter fulfills the legacy `GraphOperationsInterface` by delegating
to the newer operation interfaces exposed by `OraclePGDriver`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from graphiti_core.driver.graph_operations.graph_operations import GraphOperationsInterface
from graphiti_core.driver.oracle_pg.maintenance.community_operations import (
    determine_entity_community as determine_entity_community_oracle_pg,
)
from graphiti_core.driver.oracle_pg.maintenance.community_operations import (
    get_community_clusters as get_community_clusters_oracle_pg,
)
from graphiti_core.driver.oracle_pg.maintenance.community_operations import (
    remove_communities as remove_communities_oracle_pg,
)
from graphiti_core.driver.oracle_pg.maintenance.graph_data_operations import (
    clear_data as clear_data_oracle_pg,
)
from graphiti_core.driver.oracle_pg.maintenance.graph_data_operations import (
    retrieve_episodes as retrieve_episodes_oracle_pg,
)
from graphiti_core.driver.oracle_pg.sql_utils import get_table_name, run_query

if TYPE_CHECKING:
    from graphiti_core.driver.oracle_pg_driver import OraclePGDriver
else:
    OraclePGDriver = Any


class OraclePGGraphOperationsAdapter(GraphOperationsInterface):
    # -----------------
    # Node: Save/Delete
    # -----------------

    async def node_save(self, node: Any, driver: OraclePGDriver) -> None:
        await driver.entity_node_ops.save(driver, node)

    async def node_delete(self, node: Any, driver: OraclePGDriver) -> None:
        await driver.entity_node_ops.delete(driver, node)

    async def node_save_bulk(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        transaction: Any,
        nodes: list[Any],
        batch_size: int = 100,
    ) -> None:
        await driver.entity_node_ops.save_bulk(driver, nodes, tx=transaction, batch_size=batch_size)

    async def node_delete_by_group_id(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_id: str,
        batch_size: int = 100,
    ) -> None:
        await driver.entity_node_ops.delete_by_group_id(driver, group_id, batch_size=batch_size)

    async def node_delete_by_uuids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        uuids: list[str],
        group_id: str | None = None,
        batch_size: int = 100,
    ) -> None:
        await driver.entity_node_ops.delete_by_uuids(driver, uuids, batch_size=batch_size)

    # -----------------
    # Node: Read
    # -----------------

    async def node_get_by_uuid(self, _cls: Any, driver: OraclePGDriver, uuid: str) -> Any:
        return await driver.entity_node_ops.get_by_uuid(driver, uuid)

    async def node_get_by_uuids(self, _cls: Any, driver: OraclePGDriver, uuids: list[str]) -> list[Any]:
        return await driver.entity_node_ops.get_by_uuids(driver, uuids)

    async def node_get_by_group_ids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[Any]:
        return await driver.entity_node_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    # --------------------------
    # Node: Embeddings (load)
    # --------------------------

    async def node_load_embeddings(self, node: Any, driver: OraclePGDriver) -> None:
        await driver.entity_node_ops.load_embeddings(driver, node)

    async def node_load_embeddings_bulk(
        self,
        driver: OraclePGDriver,
        nodes: list[Any],
        batch_size: int = 100,
    ) -> dict[str, list[float]]:
        await driver.entity_node_ops.load_embeddings_bulk(driver, nodes, batch_size=batch_size)
        return {
            node.uuid: node.name_embedding
            for node in nodes
            if getattr(node, 'name_embedding', None) is not None
        }

    # --------------------------
    # EpisodicNode: Save/Delete
    # --------------------------

    async def episodic_node_save(self, node: Any, driver: OraclePGDriver) -> None:
        await driver.episode_node_ops.save(driver, node)

    async def episodic_node_delete(self, node: Any, driver: OraclePGDriver) -> None:
        await driver.episode_node_ops.delete(driver, node)

    async def episodic_node_save_bulk(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        transaction: Any,
        nodes: list[Any],
        batch_size: int = 100,
    ) -> None:
        await driver.episode_node_ops.save_bulk(driver, nodes, tx=transaction, batch_size=batch_size)

    async def episodic_edge_save_bulk(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        transaction: Any,
        episodic_edges: list[Any],
        batch_size: int = 100,
    ) -> None:
        await driver.episodic_edge_ops.save_bulk(
            driver, episodic_edges, tx=transaction, batch_size=batch_size
        )

    async def episodic_node_delete_by_group_id(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_id: str,
        batch_size: int = 100,
    ) -> None:
        await driver.episode_node_ops.delete_by_group_id(driver, group_id, batch_size=batch_size)

    async def episodic_node_delete_by_uuids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        uuids: list[str],
        group_id: str | None = None,
        batch_size: int = 100,
    ) -> None:
        await driver.episode_node_ops.delete_by_uuids(driver, uuids, batch_size=batch_size)

    # -----------------------
    # EpisodicNode: Read
    # -----------------------

    async def episodic_node_get_by_uuid(self, _cls: Any, driver: OraclePGDriver, uuid: str) -> Any:
        return await driver.episode_node_ops.get_by_uuid(driver, uuid)

    async def episodic_node_get_by_uuids(self, _cls: Any, driver: OraclePGDriver, uuids: list[str]) -> list[Any]:
        return await driver.episode_node_ops.get_by_uuids(driver, uuids)

    async def episodic_node_get_by_group_ids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[Any]:
        return await driver.episode_node_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    async def retrieve_episodes(
        self,
        driver: OraclePGDriver,
        reference_time: Any,
        last_n: int = 3,
        group_ids: list[str] | None = None,
        source: Any | None = None,
        saga: str | None = None,
    ) -> list:
        return await retrieve_episodes_oracle_pg(driver, reference_time, last_n, group_ids, source, saga)

    # -----------------------
    # CommunityNode: Save/Delete
    # -----------------------

    async def community_node_save(self, node: Any, driver: OraclePGDriver) -> None:
        await driver.community_node_ops.save(driver, node)

    async def community_node_delete(self, node: Any, driver: OraclePGDriver) -> None:
        await driver.community_node_ops.delete(driver, node)

    async def community_node_save_bulk(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        transaction: Any,
        nodes: list[Any],
        batch_size: int = 100,
    ) -> None:
        await driver.community_node_ops.save_bulk(driver, nodes, tx=transaction, batch_size=batch_size)

    async def community_node_delete_by_group_id(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_id: str,
        batch_size: int = 100,
    ) -> None:
        await driver.community_node_ops.delete_by_group_id(driver, group_id, batch_size=batch_size)

    async def community_node_delete_by_uuids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        uuids: list[str],
        group_id: str | None = None,
        batch_size: int = 100,
    ) -> None:
        await driver.community_node_ops.delete_by_uuids(driver, uuids, batch_size=batch_size)

    # -----------------------
    # CommunityNode: Read
    # -----------------------

    async def community_node_get_by_uuid(self, _cls: Any, driver: OraclePGDriver, uuid: str) -> Any:
        return await driver.community_node_ops.get_by_uuid(driver, uuid)

    async def community_node_get_by_uuids(
        self, _cls: Any, driver: OraclePGDriver, uuids: list[str]
    ) -> list[Any]:
        return await driver.community_node_ops.get_by_uuids(driver, uuids)

    async def community_node_get_by_group_ids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[Any]:
        return await driver.community_node_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    # -----------------------
    # SagaNode: Save/Delete
    # -----------------------

    async def saga_node_save(self, node: Any, driver: OraclePGDriver) -> None:
        await driver.saga_node_ops.save(driver, node)

    async def saga_node_delete(self, node: Any, driver: OraclePGDriver) -> None:
        await driver.saga_node_ops.delete(driver, node)

    async def saga_node_save_bulk(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        transaction: Any,
        nodes: list[Any],
        batch_size: int = 100,
    ) -> None:
        await driver.saga_node_ops.save_bulk(driver, nodes, tx=transaction, batch_size=batch_size)

    async def saga_node_delete_by_group_id(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_id: str,
        batch_size: int = 100,
    ) -> None:
        await driver.saga_node_ops.delete_by_group_id(driver, group_id, batch_size=batch_size)

    async def saga_node_delete_by_uuids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        uuids: list[str],
        group_id: str | None = None,
        batch_size: int = 100,
    ) -> None:
        await driver.saga_node_ops.delete_by_uuids(driver, uuids, batch_size=batch_size)

    # -----------------------
    # SagaNode: Read
    # -----------------------

    async def saga_node_get_by_uuid(self, _cls: Any, driver: OraclePGDriver, uuid: str) -> Any:
        return await driver.saga_node_ops.get_by_uuid(driver, uuid)

    async def saga_node_get_by_uuids(self, _cls: Any, driver: OraclePGDriver, uuids: list[str]) -> list[Any]:
        return await driver.saga_node_ops.get_by_uuids(driver, uuids)

    async def saga_node_get_by_group_ids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[Any]:
        return await driver.saga_node_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    # -----------------------
    # Saga: Queries
    # -----------------------

    async def saga_get_previous_episode_uuid(
        self,
        driver: OraclePGDriver,
        saga_uuid: str,
        current_episode_uuid: str,
    ) -> str | None:
        episode_table = get_table_name(driver, 'episodic_nodes')
        has_episode_table = get_table_name(driver, 'has_episode_edges')
        records = await run_query(
            driver,
            f"""
            SELECT e.uuid
            FROM {episode_table} e
            JOIN {has_episode_table} h ON h.target_node_uuid = e.uuid
            WHERE h.source_node_uuid = $saga_uuid
              AND e.uuid <> $current_episode_uuid
            ORDER BY e.valid_at DESC, e.created_at DESC
            FETCH FIRST 1 ROWS ONLY
            """,
            saga_uuid=saga_uuid,
            current_episode_uuid=current_episode_uuid,
        )
        return records[0]['uuid'] if records else None

    async def saga_get_episode_contents(
        self,
        driver: OraclePGDriver,
        saga_uuid: str,
        since: Any | None = None,
        limit: int = 200,
    ) -> list[str]:
        episode_table = get_table_name(driver, 'episodic_nodes')
        has_episode_table = get_table_name(driver, 'has_episode_edges')
        since_clause = ' AND e.created_at > $since' if since is not None else ''
        params: dict[str, Any] = {'saga_uuid': saga_uuid}
        if since is not None:
            params['since'] = since
        records = await run_query(
            driver,
            f"""
            SELECT e.content
            FROM {episode_table} e
            JOIN {has_episode_table} h ON h.target_node_uuid = e.uuid
            WHERE h.source_node_uuid = $saga_uuid
            {since_clause}
            ORDER BY e.valid_at ASC, e.created_at ASC
            FETCH FIRST {int(limit)} ROWS ONLY
            """,
            **params,
        )
        return [record['content'] for record in records if record.get('content')]

    # -----------------
    # Edge: Save/Delete
    # -----------------

    async def edge_save(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.entity_edge_ops.save(driver, edge)

    async def edge_delete(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.entity_edge_ops.delete(driver, edge)

    async def edge_save_bulk(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        transaction: Any,
        edges: list[Any],
        batch_size: int = 100,
    ) -> None:
        await driver.entity_edge_ops.save_bulk(driver, edges, tx=transaction, batch_size=batch_size)

    async def edge_delete_by_uuids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        uuids: list[str],
        group_id: str | None = None,
    ) -> None:
        await driver.entity_edge_ops.delete_by_uuids(driver, uuids)

    # -----------------
    # Edge: Read
    # -----------------

    async def edge_get_by_uuid(self, _cls: Any, driver: OraclePGDriver, uuid: str) -> Any:
        return await driver.entity_edge_ops.get_by_uuid(driver, uuid)

    async def edge_get_by_uuids(self, _cls: Any, driver: OraclePGDriver, uuids: list[str]) -> list[Any]:
        return await driver.entity_edge_ops.get_by_uuids(driver, uuids)

    async def edge_get_by_group_ids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[Any]:
        return await driver.entity_edge_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    # -----------------
    # Edge: Embeddings (load)
    # -----------------

    async def edge_load_embeddings(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.entity_edge_ops.load_embeddings(driver, edge)

    async def edge_load_embeddings_bulk(
        self,
        driver: OraclePGDriver,
        edges: list[Any],
        batch_size: int = 100,
    ) -> dict[str, list[float]]:
        await driver.entity_edge_ops.load_embeddings_bulk(driver, edges, batch_size=batch_size)
        return {
            edge.uuid: edge.fact_embedding
            for edge in edges
            if getattr(edge, 'fact_embedding', None) is not None
        }

    # ---------------------------
    # EpisodicEdge: Save/Delete
    # ---------------------------

    async def episodic_edge_save(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.episodic_edge_ops.save(driver, edge)

    async def episodic_edge_delete(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.episodic_edge_ops.delete(driver, edge)

    async def episodic_edge_delete_by_uuids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        uuids: list[str],
        group_id: str | None = None,
    ) -> None:
        await driver.episodic_edge_ops.delete_by_uuids(driver, uuids)

    # ---------------------------
    # EpisodicEdge: Read
    # ---------------------------

    async def episodic_edge_get_by_uuid(self, _cls: Any, driver: OraclePGDriver, uuid: str) -> Any:
        return await driver.episodic_edge_ops.get_by_uuid(driver, uuid)

    async def episodic_edge_get_by_uuids(self, _cls: Any, driver: OraclePGDriver, uuids: list[str]) -> list[Any]:
        return await driver.episodic_edge_ops.get_by_uuids(driver, uuids)

    async def episodic_edge_get_by_group_ids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[Any]:
        return await driver.episodic_edge_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    # ---------------------------
    # CommunityEdge: Save/Delete
    # ---------------------------

    async def community_edge_save(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.community_edge_ops.save(driver, edge)

    async def community_edge_delete(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.community_edge_ops.delete(driver, edge)

    async def community_edge_delete_by_uuids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        uuids: list[str],
        group_id: str | None = None,
    ) -> None:
        await driver.community_edge_ops.delete_by_uuids(driver, uuids)

    # ---------------------------
    # CommunityEdge: Read
    # ---------------------------

    async def community_edge_get_by_uuid(self, _cls: Any, driver: OraclePGDriver, uuid: str) -> Any:
        return await driver.community_edge_ops.get_by_uuid(driver, uuid)

    async def community_edge_get_by_uuids(
        self, _cls: Any, driver: OraclePGDriver, uuids: list[str]
    ) -> list[Any]:
        return await driver.community_edge_ops.get_by_uuids(driver, uuids)

    async def community_edge_get_by_group_ids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[Any]:
        return await driver.community_edge_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    # ---------------------------
    # HasEpisodeEdge: Save/Delete
    # ---------------------------

    async def has_episode_edge_save(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.has_episode_edge_ops.save(driver, edge)

    async def has_episode_edge_delete(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.has_episode_edge_ops.delete(driver, edge)

    async def has_episode_edge_save_bulk(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        transaction: Any,
        edges: list[Any],
        batch_size: int = 100,
    ) -> None:
        await driver.has_episode_edge_ops.save_bulk(driver, edges, tx=transaction, batch_size=batch_size)

    async def has_episode_edge_delete_by_uuids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        uuids: list[str],
        group_id: str | None = None,
    ) -> None:
        await driver.has_episode_edge_ops.delete_by_uuids(driver, uuids)

    # ---------------------------
    # HasEpisodeEdge: Read
    # ---------------------------

    async def has_episode_edge_get_by_uuid(self, _cls: Any, driver: OraclePGDriver, uuid: str) -> Any:
        return await driver.has_episode_edge_ops.get_by_uuid(driver, uuid)

    async def has_episode_edge_get_by_uuids(
        self, _cls: Any, driver: OraclePGDriver, uuids: list[str]
    ) -> list[Any]:
        return await driver.has_episode_edge_ops.get_by_uuids(driver, uuids)

    async def has_episode_edge_get_by_group_ids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[Any]:
        return await driver.has_episode_edge_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    # ----------------------------
    # NextEpisodeEdge: Save/Delete
    # ----------------------------

    async def next_episode_edge_save(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.next_episode_edge_ops.save(driver, edge)

    async def next_episode_edge_delete(self, edge: Any, driver: OraclePGDriver) -> None:
        await driver.next_episode_edge_ops.delete(driver, edge)

    async def next_episode_edge_save_bulk(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        transaction: Any,
        edges: list[Any],
        batch_size: int = 100,
    ) -> None:
        await driver.next_episode_edge_ops.save_bulk(driver, edges, tx=transaction, batch_size=batch_size)

    async def next_episode_edge_delete_by_uuids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        uuids: list[str],
        group_id: str | None = None,
    ) -> None:
        await driver.next_episode_edge_ops.delete_by_uuids(driver, uuids)

    # ----------------------------
    # NextEpisodeEdge: Read
    # ----------------------------

    async def next_episode_edge_get_by_uuid(self, _cls: Any, driver: OraclePGDriver, uuid: str) -> Any:
        return await driver.next_episode_edge_ops.get_by_uuid(driver, uuid)

    async def next_episode_edge_get_by_uuids(
        self, _cls: Any, driver: OraclePGDriver, uuids: list[str]
    ) -> list[Any]:
        return await driver.next_episode_edge_ops.get_by_uuids(driver, uuids)

    async def next_episode_edge_get_by_group_ids(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[Any]:
        return await driver.next_episode_edge_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    # -----------------
    # Search
    # -----------------

    async def get_mentioned_nodes(self, driver: OraclePGDriver, episodes: list[Any]) -> list[Any]:
        return await driver.graph_ops.get_mentioned_nodes(driver, episodes)

    async def get_communities_by_nodes(self, driver: OraclePGDriver, nodes: list[Any]) -> list[Any]:
        return await driver.graph_ops.get_communities_by_nodes(driver, nodes)

    # -----------------
    # Maintenance
    # -----------------

    async def clear_data(self, driver: OraclePGDriver, group_ids: list[str] | None = None) -> None:
        await clear_data_oracle_pg(driver, group_ids)

    async def get_community_clusters(self, driver: OraclePGDriver, group_ids: list[str] | None) -> list[list]:
        return await get_community_clusters_oracle_pg(driver, group_ids)

    async def remove_communities(self, driver: OraclePGDriver) -> None:
        await remove_communities_oracle_pg(driver)

    async def determine_entity_community(
        self, driver: OraclePGDriver, entity: Any
    ) -> tuple[Any | None, bool] | None:
        return await determine_entity_community_oracle_pg(driver, entity)

    # -----------------
    # Additional Node Operations
    # -----------------

    async def episodic_node_get_by_entity_node_uuid(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        entity_node_uuid: str,
    ) -> list[Any]:
        return await driver.episode_node_ops.get_by_entity_node_uuid(driver, entity_node_uuid)

    async def community_node_load_name_embedding(
        self,
        node: Any,
        driver: OraclePGDriver,
    ) -> None:
        await driver.community_node_ops.load_name_embedding(driver, node)

    # -----------------
    # Additional Edge Operations
    # -----------------

    async def edge_get_between_nodes(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        source_node_uuid: str,
        target_node_uuid: str,
    ) -> list[Any]:
        return await driver.entity_edge_ops.get_between_nodes(
            driver, source_node_uuid, target_node_uuid
        )

    async def edge_get_by_node_uuid(
        self,
        _cls: Any,
        driver: OraclePGDriver,
        node_uuid: str,
    ) -> list[Any]:
        return await driver.entity_edge_ops.get_by_node_uuid(driver, node_uuid)
