from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import BaseModel

import graphiti_client.graphiti_oracle_pg_client as graphiti_oracle_pg_client
from graphiti_client import BatchLimitedEmbedder, GraphitiOraclePGClient, GraphitiOraclePGConnection


def _run_async(coro: Any) -> Any:
    return asyncio.run(coro)


class _Person(BaseModel):
    name: str


class _Knows(BaseModel):
    since: int | None = None


class _EpisodeType:
    text = 'text'
    message = 'message'
    json = 'json'


class _RawEpisode:
    def __init__(
        self,
        name: str,
        content: str,
        source_description: str,
        source: Any,
        reference_time: datetime,
        uuid: str | None = None,
    ) -> None:
        self.name = name
        self.uuid = uuid
        self.content = content
        self.source_description = source_description
        self.source = source
        self.reference_time = reference_time


class _SearchConfig:
    def __init__(self) -> None:
        self.limit = 10

    def model_copy(self, deep: bool = False) -> _SearchConfig:
        del deep
        copied = _SearchConfig()
        copied.limit = self.limit
        return copied


class _NodeOps:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def get_by_group_ids(self, *args: Any, **kwargs: Any) -> list[str]:
        self.calls.append(('get_by_group_ids', args, kwargs))
        return ['node-1']

    async def get_by_uuid(self, *args: Any, **kwargs: Any) -> str:
        self.calls.append(('get_by_uuid', args, kwargs))
        return 'node-1'


class _EdgeOps:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def get_by_group_ids(self, *args: Any, **kwargs: Any) -> list[str]:
        self.calls.append(('get_by_group_ids', args, kwargs))
        return ['edge-1']

    async def get_by_uuid(self, *args: Any, **kwargs: Any) -> str:
        self.calls.append(('get_by_uuid', args, kwargs))
        return 'edge-1'

    async def get_by_node_uuid(self, *args: Any, **kwargs: Any) -> list[str]:
        self.calls.append(('get_by_node_uuid', args, kwargs))
        return ['edge-1']


class _EpisodeOps:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def get_by_uuid(self, *args: Any, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(('get_by_uuid', args, kwargs))
        return _episode_node('episode-1')

    async def get_by_group_ids(self, *args: Any, **kwargs: Any) -> list[SimpleNamespace]:
        self.calls.append(('get_by_group_ids', args, kwargs))
        return [_episode_node('episode-1')]

    async def delete_by_uuids(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(('delete_by_uuids', args, kwargs))


class _GraphOps:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def clear_data(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(('clear_data', args, kwargs))


class _Graphiti:
    def __init__(self) -> None:
        self.node_ops = _NodeOps()
        self.edge_ops = _EdgeOps()
        self.episode_ops = _EpisodeOps()
        self.graph_ops = _GraphOps()
        self.driver = SimpleNamespace(
            entity_node_ops=self.node_ops,
            entity_edge_ops=self.edge_ops,
            episode_node_ops=self.episode_ops,
            graph_ops=self.graph_ops,
        )
        self.add_episode_calls: list[dict[str, Any]] = []
        self.add_episode_bulk_calls: list[dict[str, Any]] = []
        self.search_calls: list[dict[str, Any]] = []
        self.index_calls = 0

    async def build_indices_and_constraints(self) -> None:
        self.index_calls += 1

    async def add_episode(self, **kwargs: Any) -> SimpleNamespace:
        self.add_episode_calls.append(kwargs)
        return SimpleNamespace(episode=_episode_node('episode-1', content=kwargs['episode_body']))

    async def add_episode_bulk(self, **kwargs: Any) -> SimpleNamespace:
        self.add_episode_bulk_calls.append(kwargs)
        return SimpleNamespace(
            episodes=[
                _episode_node(raw_episode.uuid or f'episode-{index}', content=raw_episode.content)
                for index, raw_episode in enumerate(kwargs['bulk_episodes'])
            ]
        )

    async def search_(self, **kwargs: Any) -> SimpleNamespace:
        self.search_calls.append(kwargs)
        return SimpleNamespace(nodes=['node-result'], edges=['edge-result'])

    async def get_nodes_and_edges_by_episode(self, episode_uuids: list[str]) -> SimpleNamespace:
        return SimpleNamespace(episode_uuids=episode_uuids, nodes=['node-1'], edges=['edge-1'])


class _FactoryDriver:
    calls: list[dict[str, Any]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.entity_node_ops = _NodeOps()
        self.entity_edge_ops = _EdgeOps()
        self.episode_node_ops = _EpisodeOps()
        self.graph_ops = _GraphOps()
        self.__class__.calls.append(kwargs)


class _FactoryGraphiti(_Graphiti):
    calls: list[dict[str, Any]] = []

    def __init__(self, **kwargs: Any) -> None:
        super().__init__()
        self.driver = kwargs['graph_driver']
        self.kwargs = kwargs
        self.__class__.calls.append(kwargs)


@pytest.fixture(autouse=True)
def graphiti_runtime_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    _FactoryDriver.calls = []
    _FactoryGraphiti.calls = []
    monkeypatch.setattr(graphiti_oracle_pg_client, '_episode_type', lambda: _EpisodeType)
    monkeypatch.setattr(graphiti_oracle_pg_client, '_graphiti_class', lambda: _FactoryGraphiti)
    monkeypatch.setattr(graphiti_oracle_pg_client, '_oracle_pg_driver_class', lambda: _FactoryDriver)
    monkeypatch.setattr(graphiti_oracle_pg_client, '_raw_episode_class', lambda: _RawEpisode)
    monkeypatch.setattr(
        graphiti_oracle_pg_client,
        '_search_config_recipes',
        lambda: (_SearchConfig(), _SearchConfig(), _SearchConfig()),
    )


def _episode_node(uuid: str, content: str = 'content') -> SimpleNamespace:
    return SimpleNamespace(
        uuid=uuid,
        content=content,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        valid_at=datetime(2026, 1, 1, tzinfo=UTC),
        source=_EpisodeType.text,
        source_description='source',
    )


def _client() -> tuple[GraphitiOraclePGClient, _Graphiti]:
    graphiti = _Graphiti()
    return GraphitiOraclePGClient(cast(Any, graphiti), run_async=_run_async), graphiti


def test_from_connection_builds_oracle_driver_and_graphiti() -> None:
    llm_client = object()
    embedder = object()
    cross_encoder = object()
    tracer = object()

    client = GraphitiOraclePGClient.from_connection(
        dsn='database',
        user='user',
        password='password',
        graph_id='project',
        llm_client=llm_client,
        embedder=embedder,
        cross_encoder=cross_encoder,
        tracer=tracer,
        connect_kwargs={'min': 1, 'max': 5, 'increment': 1},
        max_coroutines=12,
        log_queries=True,
        store_raw_episode_content=False,
        run_async=_run_async,
    )

    assert isinstance(client.client, _FactoryGraphiti)
    assert _FactoryDriver.calls == [
        {
            'dsn': 'database',
            'user': 'user',
            'password': 'password',
            'graph_id': 'project',
            'log_queries': True,
            'max_coroutines': 12,
            'connect_kwargs': {'min': 1, 'max': 5, 'increment': 1},
        }
    ]
    assert _FactoryGraphiti.calls[0]['graph_driver'].kwargs['graph_id'] == 'project'
    assert _FactoryGraphiti.calls[0]['llm_client'] is llm_client
    assert _FactoryGraphiti.calls[0]['embedder'] is embedder
    assert _FactoryGraphiti.calls[0]['cross_encoder'] is cross_encoder
    assert _FactoryGraphiti.calls[0]['tracer'] is tracer
    assert _FactoryGraphiti.calls[0]['trace_span_prefix'] == 'graphiti.oracle'
    assert _FactoryGraphiti.calls[0]['store_raw_episode_content'] is False


def test_from_config_accepts_connection_dataclass() -> None:
    connection = GraphitiOraclePGConnection(
        dsn='database',
        user='user',
        password='password',
        graph_id='project',
    )

    client = GraphitiOraclePGClient.from_config(connection, run_async=_run_async)

    assert isinstance(client.client, _FactoryGraphiti)
    assert _FactoryDriver.calls[0] == {
        'dsn': 'database',
        'user': 'user',
        'password': 'password',
        'graph_id': 'project',
        'log_queries': False,
    }


def test_batch_limited_embedder_chunks_create_batch() -> None:
    class _Embedder:
        def __init__(self) -> None:
            self.config = object()
            self.calls: list[list[str]] = []

        async def create(self, input_data: Any) -> list[float]:
            return [float(len(str(input_data)))]

        async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
            self.calls.append(input_data_list)
            return [[float(len(item))] for item in input_data_list]

    embedder = _Embedder()
    wrapper = BatchLimitedEmbedder(embedder, max_batch_size=2)

    assert _run_async(wrapper.create('abc')) == [3.0]
    assert _run_async(wrapper.create_batch(['a', 'bb', 'ccc', 'dddd', 'eeeee'])) == [
        [1.0],
        [2.0],
        [3.0],
        [4.0],
        [5.0],
    ]
    assert wrapper.config is embedder.config
    assert embedder.calls == [['a', 'bb'], ['ccc', 'dddd'], ['eeeee']]


def test_from_connection_can_wrap_passed_embedder_with_explicit_batch_limit() -> None:
    embedder = object()

    GraphitiOraclePGClient.from_connection(
        dsn='database',
        user='user',
        password='password',
        graph_id='project',
        embedder=embedder,
        embedder_max_batch_size=3,
        run_async=_run_async,
    )

    wrapped = _FactoryGraphiti.calls[0]['embedder']
    assert isinstance(wrapped, BatchLimitedEmbedder)
    assert wrapped.max_batch_size == 3
    assert wrapped._embedder is embedder


def test_graph_add_forwards_zep_arguments_to_graphiti_add_episode() -> None:
    client, graphiti = _client()

    episode = client.graph.add(
        graph_id='graph-1',
        type='text',
        data='hello',
        created_at='2026-01-02T00:00:00+00:00',
        source_description='upload',
    )

    assert episode.uuid_ == 'episode-1'
    assert episode.uuid == 'episode-1'
    assert episode.content == 'hello'
    call = graphiti.add_episode_calls[0]
    assert call['episode_body'] == 'hello'
    assert call['group_id'] == 'graph-1'
    assert call['source'] == _EpisodeType.text
    assert call['source_description'] == 'upload'
    assert call['reference_time'] == datetime(2026, 1, 2, tzinfo=UTC)


def test_graph_add_batch_forwards_raw_episodes() -> None:
    client, graphiti = _client()

    episodes = client.graph.add_batch(
        graph_id='graph-1',
        episodes=[
            {'uuid': 'episode-a', 'data': 'a', 'type': 'text'},
            SimpleNamespace(data='b', type='json'),
        ],
    )

    assert [episode.uuid_ for episode in episodes] == ['episode-a', 'episode-1']
    call = graphiti.add_episode_bulk_calls[0]
    assert call['group_id'] == 'graph-1'
    assert [episode.content for episode in call['bulk_episodes']] == ['a', 'b']
    assert [episode.source for episode in call['bulk_episodes']] == [
        _EpisodeType.text,
        _EpisodeType.json,
    ]


def test_ontology_kwargs_are_applied_to_add_and_add_batch() -> None:
    client, graphiti = _client()

    client.graph.set_ontology(
        graph_ids=['graph-1'],
        entities={'Person': _Person},
        edges={
            'KNOWS': {
                'edge_type': _Knows,
                'source_targets': [{'source': 'Person', 'target': 'Person'}],
            }
        },
    )
    client.graph.add(graph_id='graph-1', type='text', data='hello')
    client.graph.add_batch(graph_id='graph-1', episodes=[{'data': 'batch', 'type': 'text'}])

    add_call = graphiti.add_episode_calls[0]
    batch_call = graphiti.add_episode_bulk_calls[0]
    assert add_call['entity_types'] == {'Person': _Person}
    assert add_call['edge_types'] == {'KNOWS': _Knows}
    assert add_call['edge_type_map'] == {('Person', 'Person'): ['KNOWS']}
    assert batch_call['entity_types'] == {'Person': _Person}


def test_episode_client_reads_by_uuid_and_graph_id() -> None:
    client, graphiti = _client()

    episode = client.graph.episode.get(uuid_='episode-1')
    response = client.graph.episode.get_by_graph_id('graph-1', lastn=2)

    assert episode.uuid == 'episode-1'
    assert response.episodes is not None
    assert response.episodes[0].uuid == 'episode-1'
    assert graphiti.episode_ops.calls == [
        ('get_by_uuid', (graphiti.driver, 'episode-1'), {}),
        ('get_by_group_ids', (graphiti.driver, ['graph-1']), {'limit': 2}),
    ]


def test_node_and_edge_clients_forward_to_driver_ops() -> None:
    client, graphiti = _client()

    assert client.graph.node.get_by_graph_id('graph-1', limit=10, uuid_cursor='node-0') == [
        'node-1'
    ]
    assert client.graph.node.get(uuid_='node-1') == 'node-1'
    assert client.graph.node.get_edges(node_uuid='node-1') == ['edge-1']
    assert client.graph.node.get_entity_edges(node_uuid='node-1') == ['edge-1']
    assert client.graph.edge.get_by_graph_id('graph-1', limit=5, uuid_cursor='edge-0') == [
        'edge-1'
    ]
    assert client.graph.edge.get(uuid_='edge-1') == 'edge-1'

    assert graphiti.node_ops.calls == [
        (
            'get_by_group_ids',
            (graphiti.driver, ['graph-1']),
            {'limit': 10, 'uuid_cursor': 'node-0'},
        ),
        ('get_by_uuid', (graphiti.driver, 'node-1'), {}),
    ]
    assert graphiti.edge_ops.calls == [
        ('get_by_node_uuid', (graphiti.driver, 'node-1'), {}),
        ('get_by_node_uuid', (graphiti.driver, 'node-1'), {}),
        (
            'get_by_group_ids',
            (graphiti.driver, ['graph-1']),
            {'limit': 5, 'uuid_cursor': 'edge-0'},
        ),
        ('get_by_uuid', (graphiti.driver, 'edge-1'), {}),
    ]


def test_search_returns_zep_like_nodes_and_edges() -> None:
    client, graphiti = _client()

    result = client.graph.search(graph_id='graph-1', query='alice', limit=7, scope='both')

    assert result.nodes == ['node-result']
    assert result.edges == ['edge-result']
    call = graphiti.search_calls[0]
    assert call['query'] == 'alice'
    assert call['group_ids'] == ['graph-1']
    assert call['config'].limit == 7


def test_graph_create_delete_and_episode_delete_use_maintenance_ops() -> None:
    client, graphiti = _client()

    assert client.graph.create('graph-1').success is True
    assert client.graph.delete('graph-1').success is True
    assert client.graph.episode.delete(uuid_='episode-1').success is True

    assert graphiti.index_calls == 1
    assert graphiti.graph_ops.calls == [('clear_data', (graphiti.driver, ['graph-1']), {})]
    assert graphiti.episode_ops.calls == [('delete_by_uuids', (graphiti.driver, ['episode-1']), {})]
