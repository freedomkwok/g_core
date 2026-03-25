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

from graphiti_core.driver.operations.entity_node_ops import EntityNodeOperations
from graphiti_core.driver.oracle.sql_utils import build_in_clause, dumps_json, loads_json
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.driver.record_parsers import entity_node_from_record
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.nodes import EntityNode

logger = logging.getLogger(__name__)


def _entity_node_from_sql_record(record: dict[str, Any]) -> EntityNode:
    prepared = dict(record)
    prepared['labels'] = loads_json(prepared.get('labels_json'), [])
    prepared['attributes'] = loads_json(prepared.get('attributes_json'), {})
    prepared['name_embedding'] = loads_json(prepared.get('name_embedding_json'), None)
    return entity_node_from_record(prepared)


class OracleEntityNodeOperations(EntityNodeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        node: EntityNode,
        tx: Transaction | None = None,
    ) -> None:
        labels = list(set(node.labels + ['Entity']))
        delete_query = 'DELETE FROM GRAPHITI_ENTITY_NODES WHERE UUID = $uuid'
        insert_query = """
            INSERT INTO GRAPHITI_ENTITY_NODES (
                UUID, NAME, GROUP_ID, LABELS_JSON, CREATED_AT, SUMMARY, NAME_EMBEDDING_JSON, ATTRIBUTES_JSON
            ) VALUES (
                $uuid, $name, $group_id, $labels_json, $created_at, $summary, $name_embedding_json, $attributes_json
            )
        """
        params: dict[str, Any] = {
            'uuid': node.uuid,
            'name': node.name,
            'group_id': node.group_id,
            'labels_json': dumps_json(labels),
            'created_at': node.created_at,
            'summary': node.summary,
            'name_embedding_json': dumps_json(node.name_embedding),
            'attributes_json': dumps_json(node.attributes or {}),
        }
        if tx is not None:
            await tx.run(delete_query, uuid=node.uuid)
            await tx.run(insert_query, **params)
        else:
            await executor.execute_query(delete_query, uuid=node.uuid)
            await executor.execute_query(insert_query, **params)

        logger.debug(f'Saved Node to Graph: {node.uuid}')

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[EntityNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for node in nodes:
            await self.save(executor, node, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        node: EntityNode,
        tx: Transaction | None = None,
    ) -> None:
        query = 'DELETE FROM GRAPHITI_ENTITY_NODES WHERE UUID = $uuid'
        if tx is not None:
            await tx.run(query, uuid=node.uuid)
        else:
            await executor.execute_query(query, uuid=node.uuid)
        logger.debug(f'Deleted Node: {node.uuid}')

    async def delete_by_group_id(
        self,
        executor: QueryExecutor,
        group_id: str,
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        query = 'DELETE FROM GRAPHITI_ENTITY_NODES WHERE GROUP_ID = $group_id'
        if tx is not None:
            await tx.run(query, group_id=group_id)
        else:
            await executor.execute_query(query, group_id=group_id)

    async def delete_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        clause, params = build_in_clause('UUID', 'uuid', uuids)
        query = f'DELETE FROM GRAPHITI_ENTITY_NODES WHERE {clause}'
        if tx is not None:
            await tx.run(query, **params)
        else:
            await executor.execute_query(query, **params)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EntityNode:
        query = """
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                CREATED_AT AS created_at,
                SUMMARY AS summary,
                LABELS_JSON AS labels_json,
                ATTRIBUTES_JSON AS attributes_json,
                NAME_EMBEDDING_JSON AS name_embedding_json
            FROM GRAPHITI_ENTITY_NODES
            WHERE UUID = $uuid
        """
        records, _, _ = await executor.execute_query(query, uuid=uuid)
        nodes = [_entity_node_from_sql_record(r) for r in records]
        if len(nodes) == 0:
            raise NodeNotFoundError(uuid)
        return nodes[0]

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EntityNode]:
        clause, params = build_in_clause('UUID', 'uuid', uuids)
        query = f"""
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                CREATED_AT AS created_at,
                SUMMARY AS summary,
                LABELS_JSON AS labels_json,
                ATTRIBUTES_JSON AS attributes_json,
                NAME_EMBEDDING_JSON AS name_embedding_json
            FROM GRAPHITI_ENTITY_NODES
            WHERE {clause}
        """
        records, _, _ = await executor.execute_query(query, **params)
        return [_entity_node_from_sql_record(r) for r in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EntityNode]:
        where_clause, where_params = build_in_clause('GROUP_ID', 'group_id', group_ids)
        query = f"""
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                CREATED_AT AS created_at,
                SUMMARY AS summary,
                LABELS_JSON AS labels_json,
                ATTRIBUTES_JSON AS attributes_json,
                NAME_EMBEDDING_JSON AS name_embedding_json
            FROM GRAPHITI_ENTITY_NODES
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
        return [_entity_node_from_sql_record(r) for r in records]

    async def load_embeddings(
        self,
        executor: QueryExecutor,
        node: EntityNode,
    ) -> None:
        query = """
            SELECT NAME_EMBEDDING_JSON AS name_embedding_json
            FROM GRAPHITI_ENTITY_NODES
            WHERE UUID = $uuid
        """
        records, _, _ = await executor.execute_query(query, uuid=node.uuid)
        if len(records) == 0:
            raise NodeNotFoundError(node.uuid)
        node.name_embedding = loads_json(records[0].get('name_embedding_json'), None)

    async def load_embeddings_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[EntityNode],
        batch_size: int = 100,
    ) -> None:
        uuids = [n.uuid for n in nodes]
        clause, params = build_in_clause('UUID', 'uuid', uuids)
        query = f"""
            SELECT UUID AS uuid, NAME_EMBEDDING_JSON AS name_embedding_json
            FROM GRAPHITI_ENTITY_NODES
            WHERE {clause}
        """
        records, _, _ = await executor.execute_query(query, **params)
        embedding_map = {r['uuid']: loads_json(r.get('name_embedding_json'), None) for r in records}
        for node in nodes:
            if node.uuid in embedding_map:
                node.name_embedding = embedding_map[node.uuid]
