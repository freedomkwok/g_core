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

import logging
from typing import Any

from graphiti_core.driver.operations.entity_edge_ops import EntityEdgeOperations
from graphiti_core.driver.oracle.sql_utils import build_in_clause, dumps_json, loads_json
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.driver.record_parsers import entity_edge_from_record
from graphiti_core.edges import EntityEdge
from graphiti_core.errors import EdgeNotFoundError

logger = logging.getLogger(__name__)


def _entity_edge_from_sql_record(record: dict[str, Any]) -> EntityEdge:
    prepared = dict(record)
    prepared['episodes'] = loads_json(prepared.get('episodes_json'), [])
    prepared['attributes'] = loads_json(prepared.get('attributes_json'), {})
    prepared['fact_embedding'] = loads_json(prepared.get('fact_embedding_json'), None)
    return entity_edge_from_record(prepared)


class OracleEntityEdgeOperations(EntityEdgeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        edge: EntityEdge,
        tx: Transaction | None = None,
    ) -> None:
        delete_query = 'DELETE FROM GRAPHITI_RELATES_TO_EDGES WHERE UUID = $uuid'
        insert_query = """
            INSERT INTO GRAPHITI_RELATES_TO_EDGES (
                UUID, GROUP_ID, SOURCE_NODE_UUID, TARGET_NODE_UUID, NAME, FACT, FACT_EMBEDDING_JSON,
                EPISODES_JSON, CREATED_AT, EXPIRED_AT, VALID_AT, INVALID_AT, ATTRIBUTES_JSON
            ) VALUES (
                $uuid, $group_id, $source_node_uuid, $target_node_uuid, $name, $fact, $fact_embedding_json,
                $episodes_json, $created_at, $expired_at, $valid_at, $invalid_at, $attributes_json
            )
        """
        params: dict[str, Any] = {
            'uuid': edge.uuid,
            'group_id': edge.group_id,
            'source_node_uuid': edge.source_node_uuid,
            'target_node_uuid': edge.target_node_uuid,
            'name': edge.name,
            'fact': edge.fact,
            'fact_embedding_json': dumps_json(edge.fact_embedding),
            'episodes_json': dumps_json(edge.episodes),
            'created_at': edge.created_at,
            'expired_at': edge.expired_at,
            'valid_at': edge.valid_at,
            'invalid_at': edge.invalid_at,
            'attributes_json': dumps_json(edge.attributes or {}),
        }
        if tx is not None:
            await tx.run(delete_query, uuid=edge.uuid)
            await tx.run(insert_query, **params)
        else:
            await executor.execute_query(delete_query, uuid=edge.uuid)
            await executor.execute_query(insert_query, **params)

        logger.debug(f'Saved Edge to Graph: {edge.uuid}')

    async def save_bulk(
        self,
        executor: QueryExecutor,
        edges: list[EntityEdge],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for edge in edges:
            await self.save(executor, edge, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        edge: EntityEdge,
        tx: Transaction | None = None,
    ) -> None:
        query = 'DELETE FROM GRAPHITI_RELATES_TO_EDGES WHERE UUID = $uuid'
        if tx is not None:
            await tx.run(query, uuid=edge.uuid)
        else:
            await executor.execute_query(query, uuid=edge.uuid)
        logger.debug(f'Deleted Edge: {edge.uuid}')

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
    ) -> None:
        clause, params = build_in_clause('UUID', 'uuid', uuids)
        query = f'DELETE FROM GRAPHITI_RELATES_TO_EDGES WHERE {clause}'
        if tx is not None:
            await tx.run(query, **params)
        else:
            await executor.execute_query(query, **params)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EntityEdge:
        query = """
            SELECT
                UUID AS uuid,
                SOURCE_NODE_UUID AS source_node_uuid,
                TARGET_NODE_UUID AS target_node_uuid,
                FACT AS fact,
                NAME AS name,
                GROUP_ID AS group_id,
                EPISODES_JSON AS episodes_json,
                CREATED_AT AS created_at,
                EXPIRED_AT AS expired_at,
                VALID_AT AS valid_at,
                INVALID_AT AS invalid_at,
                ATTRIBUTES_JSON AS attributes_json,
                FACT_EMBEDDING_JSON AS fact_embedding_json
            FROM GRAPHITI_RELATES_TO_EDGES
            WHERE UUID = $uuid
        """
        records, _, _ = await executor.execute_query(query, uuid=uuid)
        edges = [_entity_edge_from_sql_record(r) for r in records]
        if len(edges) == 0:
            raise EdgeNotFoundError(uuid)
        return edges[0]

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EntityEdge]:
        if not uuids:
            return []
        clause, params = build_in_clause('UUID', 'uuid', uuids)
        query = f"""
            SELECT
                UUID AS uuid,
                SOURCE_NODE_UUID AS source_node_uuid,
                TARGET_NODE_UUID AS target_node_uuid,
                FACT AS fact,
                NAME AS name,
                GROUP_ID AS group_id,
                EPISODES_JSON AS episodes_json,
                CREATED_AT AS created_at,
                EXPIRED_AT AS expired_at,
                VALID_AT AS valid_at,
                INVALID_AT AS invalid_at,
                ATTRIBUTES_JSON AS attributes_json,
                FACT_EMBEDDING_JSON AS fact_embedding_json
            FROM GRAPHITI_RELATES_TO_EDGES
            WHERE {clause}
        """
        records, _, _ = await executor.execute_query(query, **params)
        return [_entity_edge_from_sql_record(r) for r in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EntityEdge]:
        where_clause, where_params = build_in_clause('GROUP_ID', 'group_id', group_ids)
        query = f"""
            SELECT
                UUID AS uuid,
                SOURCE_NODE_UUID AS source_node_uuid,
                TARGET_NODE_UUID AS target_node_uuid,
                FACT AS fact,
                NAME AS name,
                GROUP_ID AS group_id,
                EPISODES_JSON AS episodes_json,
                CREATED_AT AS created_at,
                EXPIRED_AT AS expired_at,
                VALID_AT AS valid_at,
                INVALID_AT AS invalid_at,
                ATTRIBUTES_JSON AS attributes_json,
                FACT_EMBEDDING_JSON AS fact_embedding_json
            FROM GRAPHITI_RELATES_TO_EDGES
            WHERE {where_clause}
        """
        params = dict(where_params)
        if uuid_cursor is not None:
            query += ' AND UUID < $uuid'
            params['uuid'] = uuid_cursor
        query += ' ORDER BY UUID DESC'
        records, _, _ = await executor.execute_query(query, **params)
        if limit is not None:
            records = records[:limit]
        return [_entity_edge_from_sql_record(r) for r in records]

    async def get_between_nodes(
        self,
        executor: QueryExecutor,
        source_node_uuid: str,
        target_node_uuid: str,
    ) -> list[EntityEdge]:
        query = """
            SELECT
                UUID AS uuid,
                SOURCE_NODE_UUID AS source_node_uuid,
                TARGET_NODE_UUID AS target_node_uuid,
                FACT AS fact,
                NAME AS name,
                GROUP_ID AS group_id,
                EPISODES_JSON AS episodes_json,
                CREATED_AT AS created_at,
                EXPIRED_AT AS expired_at,
                VALID_AT AS valid_at,
                INVALID_AT AS invalid_at,
                ATTRIBUTES_JSON AS attributes_json,
                FACT_EMBEDDING_JSON AS fact_embedding_json
            FROM GRAPHITI_RELATES_TO_EDGES
            WHERE SOURCE_NODE_UUID = $source_node_uuid
              AND TARGET_NODE_UUID = $target_node_uuid
        """
        records, _, _ = await executor.execute_query(
            query,
            source_node_uuid=source_node_uuid,
            target_node_uuid=target_node_uuid,
        )
        return [_entity_edge_from_sql_record(r) for r in records]

    async def get_by_node_uuid(
        self,
        executor: QueryExecutor,
        node_uuid: str,
    ) -> list[EntityEdge]:
        query = """
            SELECT
                UUID AS uuid,
                SOURCE_NODE_UUID AS source_node_uuid,
                TARGET_NODE_UUID AS target_node_uuid,
                FACT AS fact,
                NAME AS name,
                GROUP_ID AS group_id,
                EPISODES_JSON AS episodes_json,
                CREATED_AT AS created_at,
                EXPIRED_AT AS expired_at,
                VALID_AT AS valid_at,
                INVALID_AT AS invalid_at,
                ATTRIBUTES_JSON AS attributes_json,
                FACT_EMBEDDING_JSON AS fact_embedding_json
            FROM GRAPHITI_RELATES_TO_EDGES
            WHERE SOURCE_NODE_UUID = $node_uuid OR TARGET_NODE_UUID = $node_uuid
        """
        records, _, _ = await executor.execute_query(query, node_uuid=node_uuid)
        return [_entity_edge_from_sql_record(r) for r in records]

    async def load_embeddings(
        self,
        executor: QueryExecutor,
        edge: EntityEdge,
    ) -> None:
        query = """
            SELECT FACT_EMBEDDING_JSON AS fact_embedding_json
            FROM GRAPHITI_RELATES_TO_EDGES
            WHERE UUID = $uuid
        """
        records, _, _ = await executor.execute_query(query, uuid=edge.uuid)
        if len(records) == 0:
            raise EdgeNotFoundError(edge.uuid)
        edge.fact_embedding = loads_json(records[0].get('fact_embedding_json'), None)

    async def load_embeddings_bulk(
        self,
        executor: QueryExecutor,
        edges: list[EntityEdge],
        batch_size: int = 100,
    ) -> None:
        uuids = [e.uuid for e in edges]
        clause, params = build_in_clause('UUID', 'uuid', uuids)
        query = f"""
            SELECT UUID AS uuid, FACT_EMBEDDING_JSON AS fact_embedding_json
            FROM GRAPHITI_RELATES_TO_EDGES
            WHERE {clause}
        """
        records, _, _ = await executor.execute_query(query, **params)
        embedding_map = {r['uuid']: loads_json(r.get('fact_embedding_json'), None) for r in records}
        for edge in edges:
            if edge.uuid in embedding_map:
                edge.fact_embedding = embedding_map[edge.uuid]
