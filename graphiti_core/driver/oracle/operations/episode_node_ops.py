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
from datetime import datetime
from typing import Any

from graphiti_core.driver.operations.episode_node_ops import EpisodeNodeOperations
from graphiti_core.driver.oracle.sql_utils import build_in_clause, dumps_json, loads_json
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.driver.record_parsers import episodic_node_from_record
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.nodes import EpisodicNode

logger = logging.getLogger(__name__)


def _episodic_node_from_sql_record(record: dict[str, Any]) -> EpisodicNode:
    prepared = dict(record)
    prepared['entity_edges'] = loads_json(prepared.get('entity_edges_json'), [])
    return episodic_node_from_record(prepared)


class OracleEpisodeNodeOperations(EpisodeNodeOperations):
    async def save(
        self,
        executor: QueryExecutor,
        node: EpisodicNode,
        tx: Transaction | None = None,
    ) -> None:
        delete_query = 'DELETE FROM GRAPHITI_EPISODIC_NODES WHERE UUID = $uuid'
        insert_query = """
            INSERT INTO GRAPHITI_EPISODIC_NODES (
                UUID, NAME, GROUP_ID, SOURCE_DESCRIPTION, CONTENT, ENTITY_EDGES_JSON,
                CREATED_AT, VALID_AT, SOURCE
            ) VALUES (
                $uuid, $name, $group_id, $source_description, $content, $entity_edges_json,
                $created_at, $valid_at, $source
            )
        """
        params: dict[str, Any] = {
            'uuid': node.uuid,
            'name': node.name,
            'group_id': node.group_id,
            'source_description': node.source_description,
            'content': node.content,
            'entity_edges_json': dumps_json(node.entity_edges),
            'created_at': node.created_at,
            'valid_at': node.valid_at,
            'source': node.source.value,
        }
        if tx is not None:
            await tx.run(delete_query, uuid=node.uuid)
            await tx.run(insert_query, **params)
        else:
            await executor.execute_query(delete_query, uuid=node.uuid)
            await executor.execute_query(insert_query, **params)

        logger.debug(f'Saved Episode to Graph: {node.uuid}')

    async def save_bulk(
        self,
        executor: QueryExecutor,
        nodes: list[EpisodicNode],
        tx: Transaction | None = None,
        batch_size: int = 100,
    ) -> None:
        for node in nodes:
            await self.save(executor, node, tx=tx)

    async def delete(
        self,
        executor: QueryExecutor,
        node: EpisodicNode,
        tx: Transaction | None = None,
    ) -> None:
        query = 'DELETE FROM GRAPHITI_EPISODIC_NODES WHERE UUID = $uuid'
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
        query = 'DELETE FROM GRAPHITI_EPISODIC_NODES WHERE GROUP_ID = $group_id'
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
        query = f'DELETE FROM GRAPHITI_EPISODIC_NODES WHERE {clause}'
        if tx is not None:
            await tx.run(query, **params)
        else:
            await executor.execute_query(query, **params)

    async def get_by_uuid(
        self,
        executor: QueryExecutor,
        uuid: str,
    ) -> EpisodicNode:
        query = """
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                SOURCE_DESCRIPTION AS source_description,
                CONTENT AS content,
                ENTITY_EDGES_JSON AS entity_edges_json,
                CREATED_AT AS created_at,
                VALID_AT AS valid_at,
                SOURCE AS source
            FROM GRAPHITI_EPISODIC_NODES
            WHERE UUID = $uuid
        """
        records, _, _ = await executor.execute_query(query, uuid=uuid)
        episodes = [_episodic_node_from_sql_record(r) for r in records]
        if len(episodes) == 0:
            raise NodeNotFoundError(uuid)
        return episodes[0]

    async def get_by_uuids(
        self,
        executor: QueryExecutor,
        uuids: list[str],
    ) -> list[EpisodicNode]:
        clause, params = build_in_clause('UUID', 'uuid', uuids)
        query = f"""
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                SOURCE_DESCRIPTION AS source_description,
                CONTENT AS content,
                ENTITY_EDGES_JSON AS entity_edges_json,
                CREATED_AT AS created_at,
                VALID_AT AS valid_at,
                SOURCE AS source
            FROM GRAPHITI_EPISODIC_NODES
            WHERE {clause}
        """
        records, _, _ = await executor.execute_query(query, **params)
        return [_episodic_node_from_sql_record(r) for r in records]

    async def get_by_group_ids(
        self,
        executor: QueryExecutor,
        group_ids: list[str],
        limit: int | None = None,
        uuid_cursor: str | None = None,
    ) -> list[EpisodicNode]:
        where_clause, where_params = build_in_clause('GROUP_ID', 'group_id', group_ids)
        query = f"""
            SELECT
                UUID AS uuid,
                NAME AS name,
                GROUP_ID AS group_id,
                SOURCE_DESCRIPTION AS source_description,
                CONTENT AS content,
                ENTITY_EDGES_JSON AS entity_edges_json,
                CREATED_AT AS created_at,
                VALID_AT AS valid_at,
                SOURCE AS source
            FROM GRAPHITI_EPISODIC_NODES
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
        return [_episodic_node_from_sql_record(r) for r in records]

    async def get_by_entity_node_uuid(
        self,
        executor: QueryExecutor,
        entity_node_uuid: str,
    ) -> list[EpisodicNode]:
        query = """
            SELECT
                e.UUID AS uuid,
                e.NAME AS name,
                e.GROUP_ID AS group_id,
                e.SOURCE_DESCRIPTION AS source_description,
                e.CONTENT AS content,
                e.ENTITY_EDGES_JSON AS entity_edges_json,
                e.CREATED_AT AS created_at,
                e.VALID_AT AS valid_at,
                e.SOURCE AS source
            FROM GRAPHITI_EPISODIC_NODES e
            JOIN GRAPHITI_MENTIONS_EDGES m ON m.SOURCE_NODE_UUID = e.UUID
            WHERE m.TARGET_NODE_UUID = $entity_node_uuid
        """
        records, _, _ = await executor.execute_query(query, entity_node_uuid=entity_node_uuid)
        return [_episodic_node_from_sql_record(r) for r in records]

    async def retrieve_episodes(
        self,
        executor: QueryExecutor,
        reference_time: datetime,
        last_n: int = 3,
        group_ids: list[str] | None = None,
        source: str | None = None,
        saga: str | None = None,
    ) -> list[EpisodicNode]:
        records: list[dict[str, Any]] = []
        if saga is not None and group_ids is not None and len(group_ids) > 0:
            saga_records, _, _ = await executor.execute_query(
                """
                SELECT UUID AS uuid
                FROM GRAPHITI_SAGA_NODES
                WHERE NAME = $saga_name
                  AND GROUP_ID = $group_id
                """,
                saga_name=saga,
                group_id=group_ids[0],
            )
            if len(saga_records) == 0:
                return []
            saga_uuid = saga_records[0]['uuid']
            edge_records, _, _ = await executor.execute_query(
                """
                SELECT TARGET_NODE_UUID AS episode_uuid
                FROM GRAPHITI_HAS_EPISODE_EDGES
                WHERE SOURCE_NODE_UUID = $saga_uuid
                """,
                saga_uuid=saga_uuid,
            )
            episode_uuids = [r['episode_uuid'] for r in edge_records]
            if not episode_uuids:
                return []
            clause, params = build_in_clause('UUID', 'uuid', episode_uuids)
            query = f"""
                SELECT
                    UUID AS uuid,
                    NAME AS name,
                    GROUP_ID AS group_id,
                    SOURCE_DESCRIPTION AS source_description,
                    CONTENT AS content,
                    ENTITY_EDGES_JSON AS entity_edges_json,
                    CREATED_AT AS created_at,
                    VALID_AT AS valid_at,
                    SOURCE AS source
                FROM GRAPHITI_EPISODIC_NODES
                WHERE VALID_AT <= $reference_time
                  AND {clause}
            """
            params['reference_time'] = reference_time
            if source is not None:
                query += ' AND SOURCE = $source'
                params['source'] = source
            query += ' ORDER BY VALID_AT DESC'
            records, _, _ = await executor.execute_query(query, **params)
        else:
            query = """
                SELECT
                    UUID AS uuid,
                    NAME AS name,
                    GROUP_ID AS group_id,
                    SOURCE_DESCRIPTION AS source_description,
                    CONTENT AS content,
                    ENTITY_EDGES_JSON AS entity_edges_json,
                    CREATED_AT AS created_at,
                    VALID_AT AS valid_at,
                    SOURCE AS source
                FROM GRAPHITI_EPISODIC_NODES
                WHERE VALID_AT <= $reference_time
            """
            params: dict[str, Any] = {'reference_time': reference_time}
            if group_ids:
                clause, group_params = build_in_clause('GROUP_ID', 'group_id', group_ids)
                query += f' AND {clause}'
                params.update(group_params)
            if source is not None:
                query += ' AND SOURCE = $source'
                params['source'] = source
            query += ' ORDER BY VALID_AT DESC'
            records, _, _ = await executor.execute_query(query, **params)

        records = records[:last_n]
        return [_episodic_node_from_sql_record(r) for r in records]
