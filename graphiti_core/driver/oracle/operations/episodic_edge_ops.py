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

from graphiti_core.driver.operations.episodic_edge_ops import EpisodicEdgeOperations
from graphiti_core.driver.oracle.sql_utils import build_in_clause
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.edges import EpisodicEdge
from graphiti_core.errors import EdgeNotFoundError
from graphiti_core.helpers import parse_db_date

logger = logging.getLogger(__name__)


def _episodic_edge_from_record(record: Any) -> EpisodicEdge:
    return EpisodicEdge(
        uuid=record['uuid'],
        group_id=record['group_id'],
        source_node_uuid=record['source_node_uuid'],
        target_node_uuid=record['target_node_uuid'],
        created_at=parse_db_date(record['created_at']),  # type: ignore[arg-type]
    )


class OracleEpisodicEdgeOperations(EpisodicEdgeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        edge: EpisodicEdge,
        tx: Transaction | None = None,
    ) -> None:
        delete_query = 'DELETE FROM GRAPHITI_MENTIONS_EDGES WHERE UUID = $uuid'
        insert_query = """
            INSERT INTO GRAPHITI_MENTIONS_EDGES (
                UUID, GROUP_ID, SOURCE_NODE_UUID, TARGET_NODE_UUID, CREATED_AT
            ) VALUES (
                $uuid, $group_id, $source_node_uuid, $target_node_uuid, $created_at
            )
        """
        params: dict[str, Any] = {
            'uuid': edge.uuid,
            'group_id': edge.group_id,
            'source_node_uuid': edge.source_node_uuid,
            'target_node_uuid': edge.target_node_uuid,
            'created_at': edge.created_at,
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
        edges: list[EpisodicEdge],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for edge in edges:
            await self.save(executor, edge, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        edge: EpisodicEdge,
        tx: Transaction | None = None,
    ) -> None:
        query = 'DELETE FROM GRAPHITI_MENTIONS_EDGES WHERE UUID = $uuid'
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
        query = f'DELETE FROM GRAPHITI_MENTIONS_EDGES WHERE {clause}'
        if tx is not None:
            await tx.run(query, **params)
        else:
            await executor.execute_query(query, **params)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EpisodicEdge:
        query = """
            SELECT
                UUID AS uuid,
                GROUP_ID AS group_id,
                SOURCE_NODE_UUID AS source_node_uuid,
                TARGET_NODE_UUID AS target_node_uuid,
                CREATED_AT AS created_at
            FROM GRAPHITI_MENTIONS_EDGES
            WHERE UUID = $uuid
        """
        records, _, _ = await executor.execute_query(query, uuid=uuid)
        edges = [_episodic_edge_from_record(r) for r in records]
        if len(edges) == 0:
            raise EdgeNotFoundError(uuid)
        return edges[0]

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EpisodicEdge]:
        clause, params = build_in_clause('UUID', 'uuid', uuids)
        query = f"""
            SELECT
                UUID AS uuid,
                GROUP_ID AS group_id,
                SOURCE_NODE_UUID AS source_node_uuid,
                TARGET_NODE_UUID AS target_node_uuid,
                CREATED_AT AS created_at
            FROM GRAPHITI_MENTIONS_EDGES
            WHERE {clause}
        """
        records, _, _ = await executor.execute_query(query, **params)
        return [_episodic_edge_from_record(r) for r in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EpisodicEdge]:
        where_clause, where_params = build_in_clause('GROUP_ID', 'group_id', group_ids)
        query = f"""
            SELECT
                UUID AS uuid,
                GROUP_ID AS group_id,
                SOURCE_NODE_UUID AS source_node_uuid,
                TARGET_NODE_UUID AS target_node_uuid,
                CREATED_AT AS created_at
            FROM GRAPHITI_MENTIONS_EDGES
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
        return [_episodic_edge_from_record(r) for r in records]
