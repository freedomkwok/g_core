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

from graphiti_core.driver.operations.saga_node_ops import SagaNodeOperations
from graphiti_core.driver.oracle.sql_utils import build_in_clause
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.helpers import parse_db_date
from graphiti_core.nodes import SagaNode

logger = logging.getLogger(__name__)


def _saga_node_from_record(record: Any) -> SagaNode:
    return SagaNode(
        uuid=record['uuid'],
        name=record['name'],
        group_id=record['group_id'],
        created_at=parse_db_date(record['created_at']),  # type: ignore[arg-type]
    )


class OracleSagaNodeOperations(SagaNodeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        node: SagaNode,
        tx: Transaction | None = None,
    ) -> None:
        delete_query = 'DELETE FROM GRAPHITI_SAGA_NODES WHERE UUID = $uuid'
        insert_query = """
            INSERT INTO GRAPHITI_SAGA_NODES (
                UUID, NAME, GROUP_ID, CREATED_AT
            ) VALUES (
                $uuid, $name, $group_id, $created_at
            )
        """
        params: dict[str, Any] = {
            'uuid': node.uuid,
            'name': node.name,
            'group_id': node.group_id,
            'created_at': node.created_at,
        }
        if tx is not None:
            await tx.run(delete_query, uuid=node.uuid)
            await tx.run(insert_query, **params)
        else:
            await executor.execute_query(delete_query, uuid=node.uuid)
            await executor.execute_query(insert_query, **params)

        logger.debug(f'Saved Saga Node to Graph: {node.uuid}')

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[SagaNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for node in nodes:
            await self.save(executor, node, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        node: SagaNode,
        tx: Transaction | None = None,
    ) -> None:
        query = 'DELETE FROM GRAPHITI_SAGA_NODES WHERE UUID = $uuid'
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
        query = 'DELETE FROM GRAPHITI_SAGA_NODES WHERE GROUP_ID = $group_id'
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
        query = f'DELETE FROM GRAPHITI_SAGA_NODES WHERE {clause}'
        if tx is not None:
            await tx.run(query, **params)
        else:
            await executor.execute_query(query, **params)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> SagaNode:
        query = """
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                CREATED_AT AS created_at
            FROM GRAPHITI_SAGA_NODES
            WHERE UUID = $uuid
        """
        records, _, _ = await executor.execute_query(query, uuid=uuid)
        nodes = [_saga_node_from_record(r) for r in records]
        if len(nodes) == 0:
            raise NodeNotFoundError(uuid)
        return nodes[0]

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[SagaNode]:
        clause, params = build_in_clause('UUID', 'uuid', uuids)
        query = f"""
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                CREATED_AT AS created_at
            FROM GRAPHITI_SAGA_NODES
            WHERE {clause}
        """
        records, _, _ = await executor.execute_query(query, **params)
        return [_saga_node_from_record(r) for r in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[SagaNode]:
        where_clause, where_params = build_in_clause('GROUP_ID', 'group_id', group_ids)
        query = f"""
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                CREATED_AT AS created_at
            FROM GRAPHITI_SAGA_NODES
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
        return [_saga_node_from_record(r) for r in records]
