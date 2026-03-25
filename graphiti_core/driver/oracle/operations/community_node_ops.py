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

from graphiti_core.driver.operations.community_node_ops import CommunityNodeOperations
from graphiti_core.driver.oracle.sql_utils import build_in_clause, dumps_json, loads_json
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.driver.record_parsers import community_node_from_record
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.nodes import CommunityNode

logger = logging.getLogger(__name__)


class OracleCommunityNodeOperations(CommunityNodeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        node: CommunityNode,
        tx: Transaction | None = None,
    ) -> None:
        delete_query = 'DELETE FROM GRAPHITI_COMMUNITY_NODES WHERE UUID = $uuid'
        insert_query = """
            INSERT INTO GRAPHITI_COMMUNITY_NODES (
                UUID, NAME, GROUP_ID, SUMMARY, CREATED_AT, NAME_EMBEDDING_JSON
            ) VALUES (
                $uuid, $name, $group_id, $summary, $created_at, $name_embedding_json
            )
        """
        params: dict[str, Any] = {
            'uuid': node.uuid,
            'name': node.name,
            'group_id': node.group_id,
            'summary': node.summary,
            'created_at': node.created_at,
            'name_embedding_json': dumps_json(node.name_embedding),
        }
        if tx is not None:
            await tx.run(delete_query, uuid=node.uuid)
            await tx.run(insert_query, **params)
        else:
            await executor.execute_query(delete_query, uuid=node.uuid)
            await executor.execute_query(insert_query, **params)

        logger.debug(f'Saved Community Node to Graph: {node.uuid}')

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[CommunityNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for node in nodes:
            await self.save(executor, node, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        node: CommunityNode,
        tx: Transaction | None = None,
    ) -> None:
        query = 'DELETE FROM GRAPHITI_COMMUNITY_NODES WHERE UUID = $uuid'
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
        query = 'DELETE FROM GRAPHITI_COMMUNITY_NODES WHERE GROUP_ID = $group_id'
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
        query = f'DELETE FROM GRAPHITI_COMMUNITY_NODES WHERE {clause}'
        if tx is not None:
            await tx.run(query, **params)
        else:
            await executor.execute_query(query, **params)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> CommunityNode:
        query = """
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                NAME_EMBEDDING_JSON AS name_embedding,
                CREATED_AT AS created_at,
                SUMMARY AS summary
            FROM GRAPHITI_COMMUNITY_NODES
            WHERE UUID = $uuid
        """
        records, _, _ = await executor.execute_query(query, uuid=uuid)
        for record in records:
            record['name_embedding'] = loads_json(record.get('name_embedding'), [])
        nodes = [community_node_from_record(r) for r in records]
        if len(nodes) == 0:
            raise NodeNotFoundError(uuid)
        return nodes[0]

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[CommunityNode]:
        clause, params = build_in_clause('UUID', 'uuid', uuids)
        query = f"""
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                NAME_EMBEDDING_JSON AS name_embedding,
                CREATED_AT AS created_at,
                SUMMARY AS summary
            FROM GRAPHITI_COMMUNITY_NODES
            WHERE {clause}
        """
        records, _, _ = await executor.execute_query(query, **params)
        for record in records:
            record['name_embedding'] = loads_json(record.get('name_embedding'), [])
        return [community_node_from_record(r) for r in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[CommunityNode]:
        where_clause, where_params = build_in_clause('GROUP_ID', 'group_id', group_ids)
        query = f"""
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                NAME_EMBEDDING_JSON AS name_embedding,
                CREATED_AT AS created_at,
                SUMMARY AS summary
            FROM GRAPHITI_COMMUNITY_NODES
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
        for record in records:
            record['name_embedding'] = loads_json(record.get('name_embedding'), [])
        return [community_node_from_record(r) for r in records]

    async def load_name_embedding(
        self,
        executor: QueryExecutor,
        node: CommunityNode,
    ) -> None:
        query = """
            SELECT NAME_EMBEDDING_JSON AS name_embedding
            FROM GRAPHITI_COMMUNITY_NODES
            WHERE UUID = $uuid
        """
        records, _, _ = await executor.execute_query(query, uuid=node.uuid)
        if len(records) == 0:
            raise NodeNotFoundError(node.uuid)
        node.name_embedding = loads_json(records[0].get('name_embedding'), [])
