from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

import graphiti_core.driver.oracle_driver as oracle_driver_module
from graphiti_core.driver.oracle_driver import OracleDriver
from graphiti_core.edges import (
    CommunityEdge,
    EntityEdge,
    EpisodicEdge,
    HasEpisodeEdge,
    NextEpisodeEdge,
)
from graphiti_core.embedder.client import EMBEDDING_DIM
from graphiti_core.nodes import CommunityNode, EntityNode, EpisodeType, EpisodicNode, SagaNode

SUCCESS_FILE = Path(__file__).with_name('success.txt')


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {'1', 'true', 'yes', 'on'}


def _read_success() -> set[str]:
    if not SUCCESS_FILE.exists():
        return set()
    return {line.strip() for line in SUCCESS_FILE.read_text().splitlines() if line.strip()}


def _append_success(step_id: str) -> None:
    with SUCCESS_FILE.open('a', encoding='utf-8') as handle:
        handle.write(f'{step_id}\n')


def _step_id(prefix: str, step_name: str) -> str:
    return f'{prefix}:{step_name}'


async def _run_step(prefix: str, step_name: str, success_steps: set[str], step_coro) -> None:
    step_id = _step_id(prefix, step_name)
    if step_id in success_steps:
        return
    await step_coro()
    _append_success(step_id)
    success_steps.add(step_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_int_oracle_driver_operations_checkpointed():
    if oracle_driver_module.oracledb is None:
        pytest.skip('oracledb is not installed')

    uri = os.getenv('ORACLE_URI')
    user = os.getenv('ORACLE_USER')
    password = os.getenv('ORACLE_PASSWORD')
    if not (uri and user and password):
        pytest.skip('set ORACLE_URI, ORACLE_USER, and ORACLE_PASSWORD to run this integration test')

    use_rdf = _env_bool('ORACLE_USE_RDF', False)
    graph_name = os.getenv('ORACLE_RDF_GRAPH_NAME') or os.getenv('ORACLE_RDF_GRAPH') or 'GRAPHITI'
    run_key = f'{user}@{uri}|rdf={use_rdf}|graph={graph_name}'
    success_steps = _read_success()

    now = datetime.now(timezone.utc)
    group_id = f'int-oracle-{uuid4().hex[:8]}'
    base_vector = [0.01] * EMBEDDING_DIM

    entity_a = EntityNode(
        uuid=f'{group_id}-entity-a',
        name='Entity A',
        group_id=group_id,
        labels=['Person'],
        created_at=now,
    )
    entity_a.summary = 'entity a summary'
    entity_a.name_embedding = base_vector
    entity_a.attributes = {'team': 'alpha'}
    entity_b = EntityNode(
        uuid=f'{group_id}-entity-b',
        name='Entity B',
        group_id=group_id,
        labels=['Person'],
        created_at=now,
    )
    entity_b.summary = 'entity b summary'
    entity_b.name_embedding = base_vector
    entity_b.attributes = {'team': 'beta'}
    entity_c = EntityNode(
        uuid=f'{group_id}-entity-c',
        name='Entity C',
        group_id=group_id,
        labels=['Project'],
        created_at=now,
    )
    entity_c.summary = 'entity c summary'
    entity_c.name_embedding = base_vector
    entity_c.attributes = {'status': 'active'}
    community = CommunityNode(
        uuid=f'{group_id}-community',
        name='Community A',
        group_id=group_id,
        summary='community summary',
        created_at=now,
        name_embedding=base_vector,
    )
    saga = SagaNode(uuid=f'{group_id}-saga', name='Saga A', group_id=group_id, created_at=now)
    episode_1 = EpisodicNode(
        uuid=f'{group_id}-episode-1',
        name='Episode 1',
        group_id=group_id,
        source=EpisodeType.text,
        source_description='integration test',
        content='Entity A collaborated with Entity B on Project C.',
        valid_at=now,
        created_at=now,
        entity_edges=[],
    )
    episode_2 = EpisodicNode(
        uuid=f'{group_id}-episode-2',
        name='Episode 2',
        group_id=group_id,
        source=EpisodeType.text,
        source_description='integration test',
        content='Entity B updated Project C timeline.',
        valid_at=now,
        created_at=now,
        entity_edges=[],
    )
    relation_ab = EntityEdge(
        uuid=f'{group_id}-rel-ab',
        source_node_uuid=entity_a.uuid,
        target_node_uuid=entity_b.uuid,
        group_id=group_id,
        created_at=now,
        name='collaborates_with',
        fact='Entity A collaborates with Entity B',
        fact_embedding=base_vector,
        episodes=[episode_1.uuid],
    )
    relation_bc = EntityEdge(
        uuid=f'{group_id}-rel-bc',
        source_node_uuid=entity_b.uuid,
        target_node_uuid=entity_c.uuid,
        group_id=group_id,
        created_at=now,
        name='updates',
        fact='Entity B updates Project C',
        fact_embedding=base_vector,
        episodes=[episode_2.uuid],
    )
    mention_a = EpisodicEdge(
        uuid=f'{group_id}-mention-a',
        source_node_uuid=episode_1.uuid,
        target_node_uuid=entity_a.uuid,
        group_id=group_id,
        created_at=now,
    )
    member_a = CommunityEdge(
        uuid=f'{group_id}-member-a',
        source_node_uuid=community.uuid,
        target_node_uuid=entity_a.uuid,
        group_id=group_id,
        created_at=now,
    )
    has_episode = HasEpisodeEdge(
        uuid=f'{group_id}-has-episode',
        source_node_uuid=saga.uuid,
        target_node_uuid=episode_1.uuid,
        group_id=group_id,
        created_at=now,
    )
    next_episode = NextEpisodeEdge(
        uuid=f'{group_id}-next-episode',
        source_node_uuid=episode_1.uuid,
        target_node_uuid=episode_2.uuid,
        group_id=group_id,
        created_at=now,
    )

    driver = OracleDriver(uri=uri, user=user, password=password, use_rdf=use_rdf)
    try:
        await driver.graph_ops.clear_data(driver, [group_id])

        # Always reseed baseline data for this run.
        await driver.entity_node_ops.save(driver, entity_a)
        await driver.entity_node_ops.save_bulk(driver, [entity_b, entity_c])
        await driver.episode_node_ops.save(driver, episode_1)
        await driver.episode_node_ops.save_bulk(driver, [episode_2])
        await driver.community_node_ops.save(driver, community)
        await driver.saga_node_ops.save(driver, saga)
        await driver.entity_edge_ops.save(driver, relation_ab)
        await driver.entity_edge_ops.save_bulk(driver, [relation_bc])
        await driver.episodic_edge_ops.save(driver, mention_a)
        await driver.community_edge_ops.save(driver, member_a)
        await driver.has_episode_edge_ops.save(driver, has_episode)
        await driver.next_episode_edge_ops.save(driver, next_episode)

        await _run_step(
            run_key,
            'entity_node_reads_and_embeddings',
            success_steps,
            lambda: _probe_entity_node_ops(driver, group_id, entity_a, [entity_a, entity_b, entity_c]),
        )
        await _run_step(
            run_key,
            'episode_node_reads_and_retrieve',
            success_steps,
            lambda: _probe_episode_node_ops(driver, group_id, now, episode_1, entity_a),
        )
        await _run_step(
            run_key,
            'community_node_reads_and_embedding',
            success_steps,
            lambda: _probe_community_node_ops(driver, group_id, community),
        )
        await _run_step(
            run_key,
            'saga_node_reads',
            success_steps,
            lambda: _probe_saga_node_ops(driver, group_id, saga),
        )
        await _run_step(
            run_key,
            'entity_edge_reads_and_embeddings',
            success_steps,
            lambda: _probe_entity_edge_ops(driver, group_id, entity_a, entity_b, relation_ab, relation_bc),
        )
        await _run_step(
            run_key,
            'episodic_edge_reads',
            success_steps,
            lambda: _probe_episodic_edge_ops(driver, group_id, mention_a),
        )
        await _run_step(
            run_key,
            'community_edge_reads',
            success_steps,
            lambda: _probe_community_edge_ops(driver, group_id, member_a),
        )
        await _run_step(
            run_key,
            'graph_ops_and_search_reranker',
            success_steps,
            lambda: _probe_graph_and_search_ops(driver, entity_a, episode_1, use_rdf),
        )
        await _run_step(
            run_key,
            'edge_mutation_ops',
            success_steps,
            lambda: _probe_edge_mutations(
                driver, group_id, now, saga.uuid, episode_1.uuid, episode_2.uuid, community.uuid, entity_c.uuid
            ),
        )
        await _run_step(
            run_key,
            'node_delete_ops',
            success_steps,
            lambda: _probe_node_deletions(driver, group_id, now, base_vector),
        )
    finally:
        await driver.graph_ops.clear_data(driver, [group_id])
        await driver.close()


async def _probe_entity_node_ops(
    driver: OracleDriver,
    group_id: str,
    entity_for_embedding: EntityNode,
    entities: list[EntityNode],
) -> None:
    assert (await driver.entity_node_ops.get_by_uuid(driver, entities[0].uuid)).uuid == entities[0].uuid
    assert len(await driver.entity_node_ops.get_by_uuids(driver, [e.uuid for e in entities])) >= 2
    assert len(await driver.entity_node_ops.get_by_group_ids(driver, [group_id], limit=10)) >= 2

    await driver.entity_node_ops.load_embeddings(driver, entity_for_embedding)
    assert entity_for_embedding.name_embedding is not None
    await driver.entity_node_ops.load_embeddings_bulk(driver, entities)
    assert any(entity.name_embedding is not None for entity in entities)


async def _probe_episode_node_ops(
    driver: OracleDriver,
    group_id: str,
    now: datetime,
    episode: EpisodicNode,
    entity: EntityNode,
) -> None:
    assert (await driver.episode_node_ops.get_by_uuid(driver, episode.uuid)).uuid == episode.uuid
    assert len(await driver.episode_node_ops.get_by_uuids(driver, [episode.uuid])) == 1
    assert len(await driver.episode_node_ops.get_by_group_ids(driver, [group_id], limit=10)) >= 1
    assert len(await driver.episode_node_ops.get_by_entity_node_uuid(driver, entity.uuid)) >= 1
    assert len(await driver.episode_node_ops.retrieve_episodes(driver, now, last_n=2, group_ids=[group_id])) >= 1


async def _probe_community_node_ops(
    driver: OracleDriver,
    group_id: str,
    community: CommunityNode,
) -> None:
    assert (await driver.community_node_ops.get_by_uuid(driver, community.uuid)).uuid == community.uuid
    assert len(await driver.community_node_ops.get_by_uuids(driver, [community.uuid])) == 1
    assert len(await driver.community_node_ops.get_by_group_ids(driver, [group_id], limit=10)) >= 1
    await driver.community_node_ops.load_name_embedding(driver, community)
    assert community.name_embedding is not None


async def _probe_saga_node_ops(driver: OracleDriver, group_id: str, saga: SagaNode) -> None:
    assert (await driver.saga_node_ops.get_by_uuid(driver, saga.uuid)).uuid == saga.uuid
    assert len(await driver.saga_node_ops.get_by_uuids(driver, [saga.uuid])) == 1
    assert len(await driver.saga_node_ops.get_by_group_ids(driver, [group_id], limit=10)) >= 1


async def _probe_entity_edge_ops(
    driver: OracleDriver,
    group_id: str,
    entity_a: EntityNode,
    entity_b: EntityNode,
    edge_for_embedding: EntityEdge,
    edge_secondary: EntityEdge,
) -> None:
    assert (await driver.entity_edge_ops.get_by_uuid(driver, edge_for_embedding.uuid)).uuid == edge_for_embedding.uuid
    assert len(await driver.entity_edge_ops.get_by_uuids(driver, [edge_for_embedding.uuid, edge_secondary.uuid])) >= 2
    assert len(await driver.entity_edge_ops.get_by_group_ids(driver, [group_id], limit=10)) >= 2
    assert len(await driver.entity_edge_ops.get_between_nodes(driver, entity_a.uuid, entity_b.uuid)) >= 1
    assert len(await driver.entity_edge_ops.get_by_node_uuid(driver, entity_a.uuid)) >= 1

    await driver.entity_edge_ops.load_embeddings(driver, edge_for_embedding)
    assert edge_for_embedding.fact_embedding is not None
    await driver.entity_edge_ops.load_embeddings_bulk(driver, [edge_for_embedding, edge_secondary])
    assert edge_secondary.fact_embedding is not None


async def _probe_episodic_edge_ops(driver: OracleDriver, group_id: str, mention: EpisodicEdge) -> None:
    assert (await driver.episodic_edge_ops.get_by_uuid(driver, mention.uuid)).uuid == mention.uuid
    assert len(await driver.episodic_edge_ops.get_by_uuids(driver, [mention.uuid])) == 1
    assert len(await driver.episodic_edge_ops.get_by_group_ids(driver, [group_id], limit=10)) >= 1


async def _probe_community_edge_ops(driver: OracleDriver, group_id: str, member: CommunityEdge) -> None:
    assert (await driver.community_edge_ops.get_by_uuid(driver, member.uuid)).uuid == member.uuid
    assert len(await driver.community_edge_ops.get_by_uuids(driver, [member.uuid])) == 1
    assert len(await driver.community_edge_ops.get_by_group_ids(driver, [group_id], limit=10)) >= 1


async def _probe_graph_and_search_ops(
    driver: OracleDriver,
    entity: EntityNode,
    episode: EpisodicNode,
    use_rdf: bool,
) -> None:
    await driver.graph_ops.build_indices_and_constraints(driver, delete_existing=False)
    await driver.graph_ops.delete_all_indexes(driver)
    # Some graph maintenance read paths are still Cypher-only and not yet RDF-backed.
    if not use_rdf:
        _ = await driver.graph_ops.get_community_clusters(driver, [entity.group_id])
        await driver.graph_ops.determine_entity_community(driver, entity)
        assert len(await driver.graph_ops.get_mentioned_nodes(driver, [episode])) >= 1
        _ = await driver.graph_ops.get_communities_by_nodes(driver, [entity])
    _ = await driver.search_ops.episode_mentions_reranker(driver, [entity.uuid], min_score=0)


async def _probe_edge_mutations(
    driver: OracleDriver,
    group_id: str,
    now: datetime,
    saga_uuid: str,
    episode_1_uuid: str,
    episode_2_uuid: str,
    community_uuid: str,
    entity_uuid: str,
) -> None:
    temp_mention = EpisodicEdge(
        uuid=f'{group_id}-mention-temp',
        source_node_uuid=episode_1_uuid,
        target_node_uuid=entity_uuid,
        group_id=group_id,
        created_at=now,
    )
    await driver.episodic_edge_ops.save(driver, temp_mention)
    await driver.episodic_edge_ops.delete(driver, temp_mention)
    await driver.episodic_edge_ops.save_bulk(driver, [temp_mention])
    await driver.episodic_edge_ops.delete_by_uuids(driver, [temp_mention.uuid])

    temp_member = CommunityEdge(
        uuid=f'{group_id}-member-temp',
        source_node_uuid=community_uuid,
        target_node_uuid=entity_uuid,
        group_id=group_id,
        created_at=now,
    )
    await driver.community_edge_ops.save(driver, temp_member)
    await driver.community_edge_ops.delete(driver, temp_member)
    await driver.community_edge_ops.save(driver, temp_member)
    await driver.community_edge_ops.delete_by_uuids(driver, [temp_member.uuid])

    temp_has_episode = HasEpisodeEdge(
        uuid=f'{group_id}-has-temp',
        source_node_uuid=saga_uuid,
        target_node_uuid=episode_1_uuid,
        group_id=group_id,
        created_at=now,
    )
    await driver.has_episode_edge_ops.save_bulk(driver, [temp_has_episode])
    await driver.has_episode_edge_ops.delete(driver, temp_has_episode)
    await driver.has_episode_edge_ops.save(driver, temp_has_episode)
    await driver.has_episode_edge_ops.delete_by_uuids(driver, [temp_has_episode.uuid])

    temp_next = NextEpisodeEdge(
        uuid=f'{group_id}-next-temp',
        source_node_uuid=episode_1_uuid,
        target_node_uuid=episode_2_uuid,
        group_id=group_id,
        created_at=now,
    )
    await driver.next_episode_edge_ops.save_bulk(driver, [temp_next])
    await driver.next_episode_edge_ops.delete(driver, temp_next)
    await driver.next_episode_edge_ops.save(driver, temp_next)
    await driver.next_episode_edge_ops.delete_by_uuids(driver, [temp_next.uuid])


async def _probe_node_deletions(
    driver: OracleDriver,
    group_id: str,
    now: datetime,
    base_vector: list[float],
) -> None:
    delete_one = EntityNode(
        uuid=f'{group_id}-delete-entity-1',
        name='Delete Entity 1',
        group_id=group_id,
        labels=['Entity'],
        created_at=now,
    )
    delete_one.summary = 'delete test 1'
    delete_one.name_embedding = base_vector
    delete_two = EntityNode(
        uuid=f'{group_id}-delete-entity-2',
        name='Delete Entity 2',
        group_id=group_id,
        labels=['Entity'],
        created_at=now,
    )
    delete_two.summary = 'delete test 2'
    delete_two.name_embedding = base_vector
    await driver.entity_node_ops.save_bulk(driver, [delete_one, delete_two])
    await driver.entity_node_ops.delete(driver, delete_one)
    await driver.entity_node_ops.delete_by_uuids(driver, [delete_two.uuid])

    delete_episode = EpisodicNode(
        uuid=f'{group_id}-delete-episode',
        name='Delete Episode',
        group_id=group_id,
        source=EpisodeType.text,
        source_description='delete test',
        content='temporary episode',
        valid_at=now,
        created_at=now,
        entity_edges=[],
    )
    await driver.episode_node_ops.save(driver, delete_episode)
    await driver.episode_node_ops.delete(driver, delete_episode)

    delete_community = CommunityNode(
        uuid=f'{group_id}-delete-community',
        name='Delete Community',
        group_id=group_id,
        summary='delete community',
        created_at=now,
        name_embedding=base_vector,
    )
    await driver.community_node_ops.save(driver, delete_community)
    await driver.community_node_ops.delete_by_uuids(driver, [delete_community.uuid])

    delete_saga = SagaNode(
        uuid=f'{group_id}-delete-saga',
        name='Delete Saga',
        group_id=group_id,
        created_at=now,
    )
    await driver.saga_node_ops.save(driver, delete_saga)
    await driver.saga_node_ops.delete(driver, delete_saga)

    await driver.graph_ops.remove_communities(driver)
    await driver.episode_node_ops.delete_by_group_id(driver, group_id)
    await driver.entity_node_ops.delete_by_group_id(driver, group_id)
    await driver.community_node_ops.delete_by_group_id(driver, group_id)
    await driver.saga_node_ops.delete_by_group_id(driver, group_id)
