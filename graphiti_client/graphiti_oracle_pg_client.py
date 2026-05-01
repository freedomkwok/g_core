from __future__ import annotations

import asyncio
import logging
import inspect
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from graphiti_client.ontology import GraphitiOntologyRegistry

if TYPE_CHECKING:
    from graphiti_core import Graphiti
    from graphiti_core.driver.driver import GraphDriver
    from graphiti_core.nodes import EpisodeType

RunAsync = Callable[[Coroutine[Any, Any, Any]], Any]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GraphitiOraclePGEpisode:
    content: str
    created_at: str
    uuid_: str
    metadata: dict[str, Any] | None = None
    processed: bool | None = True
    relevance: float | None = None
    role: str | None = None
    role_type: str | None = None
    score: float | None = None
    source: str | None = None
    source_description: str | None = None
    task_id: str | None = None
    thread_id: str | None = None

    @property
    def uuid(self) -> str:
        return self.uuid_


@dataclass(frozen=True)
class GraphitiOraclePGEpisodeResponse:
    episodes: list[GraphitiOraclePGEpisode] | None = None


@dataclass(frozen=True)
class GraphitiOraclePGSearchResults:
    nodes: list[Any] = field(default_factory=list)
    edges: list[Any] = field(default_factory=list)


@dataclass(frozen=True)
class GraphitiOraclePGSuccessResponse:
    success: bool = True


@dataclass(frozen=True)
class GraphitiOraclePGConnection:
    dsn: str
    user: str
    password: str
    graph_id: str
    connect_kwargs: dict[str, int] | None = None
    max_coroutines: int | None = None
    log_queries: bool = False


class BatchLimitedEmbedder:
    def __init__(self, embedder: Any, max_batch_size: int) -> None:
        if max_batch_size <= 0:
            raise ValueError('max_batch_size must be greater than 0')
        self._embedder = embedder
        self.max_batch_size = max_batch_size
        if hasattr(embedder, 'config'):
            self.config = embedder.config

    async def create(self, input_data: Any) -> list[float]:
        return await self._embedder.create(input_data)

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        if len(input_data_list) <= self.max_batch_size:
            return await self._embedder.create_batch(input_data_list)

        results: list[list[float]] = []
        for index in range(0, len(input_data_list), self.max_batch_size):
            chunk = input_data_list[index : index + self.max_batch_size]
            results.extend(await self._embedder.create_batch(chunk))
        return results


def _run_in_current_thread(coro: Coroutine[Any, Any, Any]) -> Any:
    return asyncio.run(coro)


def _paged_kwargs(limit: int | None, uuid_cursor: str | None) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if limit is not None:
        kwargs['limit'] = limit
    if uuid_cursor is not None:
        kwargs['uuid_cursor'] = uuid_cursor
    return kwargs


def _datetime(value: Any | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized_value = value.replace('Z', '+00:00')
        return datetime.fromisoformat(normalized_value)
    raise TypeError(f'Unsupported datetime value: {value!r}')


def _episode_source(value: Any) -> EpisodeType:
    episode_type = _episode_type()
    source = str(getattr(value, 'value', value) or '').lower()
    if source == 'json':
        return episode_type.json
    if source == 'message':
        return episode_type.message
    return episode_type.text


def _episode_name(group_id: str | None, prefix: str = 'episode') -> str:
    normalized_group_id = GraphitiOntologyRegistry.graph_id(group_id) or 'graphiti'
    return f'{prefix}_{normalized_group_id}_{datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")}'


def _source_description(graph_id: str | None, user_id: str | None, value: str | None = None) -> str:
    if value:
        return value
    group_id = graph_id or user_id or 'graphiti'
    return f'{group_id}_episodes'


def _filter_supported_kwargs(func: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return kwargs

    parameters = signature.parameters
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
        return kwargs

    return {key: value for key, value in kwargs.items() if key in parameters}


def _episode_from_node(node: Any) -> GraphitiOraclePGEpisode:
    created_at = getattr(node, 'created_at', None)
    valid_at = getattr(node, 'valid_at', None)
    source = getattr(node, 'source', None)
    return GraphitiOraclePGEpisode(
        content=getattr(node, 'content', '') or '',
        created_at=_time_string(created_at or valid_at),
        uuid_=getattr(node, 'uuid', '') or getattr(node, 'uuid_', ''),
        metadata=getattr(node, 'attributes', None),
        processed=True,
        source=getattr(source, 'value', source),
        source_description=getattr(node, 'source_description', None),
    )


def _time_string(value: Any | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    return str(value)


class GraphitiOraclePGClient:
    def __init__(
        self,
        client: Graphiti,
        run_async: RunAsync | None = None,
        ontology: GraphitiOntologyRegistry | None = None,
    ) -> None:
        self._client = client
        self._run_async = run_async or _run_in_current_thread
        self._ontology = ontology or GraphitiOntologyRegistry()
        self.graph = GraphitiOraclePGGraphClient(client, self._run_async, self._ontology)

    @classmethod
    def from_connection(
        cls,
        *,
        dsn: str,
        user: str,
        password: str,
        graph_id: str,
        llm_client: Any | None = None,
        embedder: Any | None = None,
        cross_encoder: Any | None = None,
        tracer: Any | None = None,
        trace_span_prefix: str = 'graphiti.oracle',
        connect_kwargs: dict[str, int] | None = None,
        max_coroutines: int | None = None,
        log_queries: bool = False,
        store_raw_episode_content: bool = True,
        embedder_max_batch_size: int | None = None,
        run_async: RunAsync | None = None,
        ontology: GraphitiOntologyRegistry | None = None,
    ) -> GraphitiOraclePGClient:
        logger.info('GraphitiOraclePGClient.from_connection graph_id=%r', graph_id)
        connection = GraphitiOraclePGConnection(
            dsn=dsn,
            user=user,
            password=password,
            graph_id=graph_id,
            connect_kwargs=connect_kwargs,
            max_coroutines=max_coroutines,
            log_queries=log_queries,
        )
        return cls.from_config(
            connection,
            llm_client=llm_client,
            embedder=embedder,
            cross_encoder=cross_encoder,
            tracer=tracer,
            trace_span_prefix=trace_span_prefix,
            store_raw_episode_content=store_raw_episode_content,
            embedder_max_batch_size=embedder_max_batch_size,
            run_async=run_async,
            ontology=ontology,
        )

    @classmethod
    def from_config(
        cls,
        connection: GraphitiOraclePGConnection,
        *,
        llm_client: Any | None = None,
        embedder: Any | None = None,
        cross_encoder: Any | None = None,
        tracer: Any | None = None,
        trace_span_prefix: str = 'graphiti.oracle',
        store_raw_episode_content: bool = True,
        embedder_max_batch_size: int | None = None,
        run_async: RunAsync | None = None,
        ontology: GraphitiOntologyRegistry | None = None,
    ) -> GraphitiOraclePGClient:
        logger.info('GraphitiOraclePGClient.from_config graph_id=%r', connection.graph_id)
        driver_kwargs: dict[str, Any] = {
            'dsn': connection.dsn,
            'user': connection.user,
            'password': connection.password,
            'graph_id': connection.graph_id,
            'log_queries': connection.log_queries,
        }
        if connection.max_coroutines is not None:
            driver_kwargs['max_coroutines'] = connection.max_coroutines
        if connection.connect_kwargs:
            driver_kwargs['connect_kwargs'] = dict(connection.connect_kwargs)

        graphiti_kwargs: dict[str, Any] = {
            'graph_driver': _oracle_pg_driver_class()(**driver_kwargs),
            'store_raw_episode_content': store_raw_episode_content,
            'trace_span_prefix': trace_span_prefix,
        }
        if llm_client is not None:
            graphiti_kwargs['llm_client'] = llm_client
        if embedder is not None:
            graphiti_kwargs['embedder'] = (
                BatchLimitedEmbedder(embedder, embedder_max_batch_size)
                if embedder_max_batch_size is not None
                else embedder
            )
        if cross_encoder is not None:
            graphiti_kwargs['cross_encoder'] = cross_encoder
        if tracer is not None:
            graphiti_kwargs['tracer'] = tracer

        return cls(_graphiti_class()(**graphiti_kwargs), run_async=run_async, ontology=ontology)

    @property
    def client(self) -> Graphiti:
        return self._client

    @property
    def driver(self) -> GraphDriver:
        return self._client.driver

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class GraphitiOraclePGGraphClient:
    def __init__(
        self,
        client: Graphiti,
        run_async: RunAsync,
        ontology: GraphitiOntologyRegistry,
    ) -> None:
        self._client = client
        self._run_async = run_async
        self._ontology = ontology
        self.episode = GraphitiOraclePGEpisodeClient(client, run_async)
        self.node = GraphitiOraclePGNodeClient(client, run_async)
        self.edge = GraphitiOraclePGEdgeClient(client, run_async)

    @property
    def with_raw_response(self) -> GraphitiOraclePGGraphClient:
        return self

    def create(
        self,
        graph_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        request_options: Any | None = None,
    ) -> GraphitiOraclePGSuccessResponse:
        del graph_id, name, description, request_options
        self._run_async(self._client.build_indices_and_constraints())
        return GraphitiOraclePGSuccessResponse()

    def delete(
        self,
        graph_id: str,
        *,
        request_options: Any | None = None,
    ) -> GraphitiOraclePGSuccessResponse:
        del request_options

        async def delete_graph() -> None:
            graph_ops = self._client.driver.graph_ops
            if graph_ops is not None:
                await graph_ops.clear_data(self._client.driver, [graph_id])
                return

            from graphiti_core.utils.maintenance.graph_data_operations import clear_data

            await clear_data(self._client.driver, [graph_id])

        self._run_async(delete_graph())
        self._ontology.remove(graph_id)
        return GraphitiOraclePGSuccessResponse()

    def set_ontology(
        self,
        *,
        graph_ids: Sequence[str],
        entities: dict[str, Any] | None = None,
        edges: dict[str, Any] | None = None,
        request_options: Any | None = None,
    ) -> GraphitiOraclePGSuccessResponse:
        del request_options
        for graph_id in graph_ids:
            self._ontology.set(graph_id, entities, edges)
        return GraphitiOraclePGSuccessResponse()

    def add(
        self,
        *,
        data: str,
        type: Any,
        created_at: str | None = None,
        graph_id: str | None = None,
        source_description: str | None = None,
        user_id: str | None = None,
        request_options: Any | None = None,
    ) -> GraphitiOraclePGEpisode:
        del request_options
        group_id = graph_id or user_id

        async def add_episode() -> GraphitiOraclePGEpisode:
            call_kwargs = {
                'name': _episode_name(group_id),
                'episode_body': data,
                'source': _episode_source(type),
                'source_description': _source_description(graph_id, user_id, source_description),
                'reference_time': _datetime(created_at),
                'group_id': group_id,
            }
            call_kwargs.update(self._ontology.graphiti_kwargs(group_id))
            result = await self._client.add_episode(
                **_filter_supported_kwargs(self._client.add_episode, call_kwargs)
            )
            return _episode_from_node(result.episode)

        return self._run_async(add_episode())

    def add_batch(
        self,
        *,
        episodes: Sequence[Any],
        graph_id: str | None = None,
        user_id: str | None = None,
        request_options: Any | None = None,
    ) -> list[GraphitiOraclePGEpisode]:
        del request_options
        group_id = graph_id or user_id
        raw_episodes = [
            _raw_episode_class()(
                name=_episode_name(group_id, prefix=f'episode_{index}'),
                uuid=_episode_value(episode, 'uuid') or _episode_value(episode, 'uuid_'),
                content=_episode_data(episode),
                source_description=_source_description(
                    graph_id,
                    user_id,
                    _episode_value(episode, 'source_description'),
                ),
                source=_episode_source(_episode_value(episode, 'type', 'text')),
                reference_time=_datetime(_episode_value(episode, 'created_at')),
            )
            for index, episode in enumerate(episodes)
        ]

        async def add_episode_bulk() -> list[GraphitiOraclePGEpisode]:
            call_kwargs = {
                'bulk_episodes': raw_episodes,
                'group_id': group_id,
            }
            call_kwargs.update(self._ontology.graphiti_kwargs(group_id))
            result = await self._client.add_episode_bulk(
                **_filter_supported_kwargs(self._client.add_episode_bulk, call_kwargs)
            )
            return [_episode_from_node(episode) for episode in result.episodes]

        return self._run_async(add_episode_bulk())

    def search(
        self,
        *,
        graph_id: str | None = None,
        query: str,
        limit: int = 10,
        scope: str = 'edges',
        reranker: str = 'cross_encoder',
        user_id: str | None = None,
        request_options: Any | None = None,
        **kwargs: Any,
    ) -> GraphitiOraclePGSearchResults:
        del request_options
        group_id = graph_id or user_id

        async def search_graph() -> GraphitiOraclePGSearchResults:
            nodes: list[Any] = []
            edges: list[Any] = []

            if hasattr(self._client, 'search_'):
                if scope == 'nodes':
                    _combined_config, _edge_config, node_config = _search_config_recipes()
                    config = _search_config(node_config, limit)
                    result = await self._client.search_(
                        query=query,
                        config=config,
                        group_ids=[group_id] if group_id else None,
                        **kwargs,
                    )
                    nodes = list(getattr(result, 'nodes', []) or [])
                elif scope == 'edges':
                    _combined_config, edge_config, _node_config = _search_config_recipes()
                    config = _search_config(edge_config, limit)
                    result = await self._client.search_(
                        query=query,
                        config=config,
                        group_ids=[group_id] if group_id else None,
                        **kwargs,
                    )
                    edges = list(getattr(result, 'edges', []) or [])
                else:
                    combined_config, _edge_config, _node_config = _search_config_recipes()
                    config = _search_config(combined_config, limit)
                    if reranker != 'cross_encoder':
                        config = _search_config(combined_config, limit)
                    result = await self._client.search_(
                        query=query,
                        config=config,
                        group_ids=[group_id] if group_id else None,
                        **kwargs,
                    )
                    nodes = list(getattr(result, 'nodes', []) or [])
                    edges = list(getattr(result, 'edges', []) or [])
                return GraphitiOraclePGSearchResults(nodes=nodes, edges=edges)

            results = await self._client.search(
                query=query,
                group_ids=[group_id] if group_id else None,
                num_results=limit,
            )
            return GraphitiOraclePGSearchResults(edges=list(results or []))

        return self._run_async(search_graph())


class GraphitiOraclePGEpisodeClient:
    def __init__(self, client: Graphiti, run_async: RunAsync) -> None:
        self._client = client
        self._run_async = run_async

    @property
    def with_raw_response(self) -> GraphitiOraclePGEpisodeClient:
        return self

    def get(self, uuid_: str, *, request_options: Any | None = None) -> GraphitiOraclePGEpisode:
        del request_options

        async def get_episode() -> GraphitiOraclePGEpisode:
            episode_ops = self._client.driver.episode_node_ops
            if episode_ops is None:
                from graphiti_core.nodes import EpisodicNode

                return _episode_from_node(await EpisodicNode.get_by_uuid(self._client.driver, uuid_))
            return _episode_from_node(await episode_ops.get_by_uuid(self._client.driver, uuid_))

        return self._run_async(get_episode())

    def get_by_graph_id(
        self,
        graph_id: str,
        *,
        lastn: int | None = None,
        request_options: Any | None = None,
    ) -> GraphitiOraclePGEpisodeResponse:
        del request_options

        async def get_episodes() -> GraphitiOraclePGEpisodeResponse:
            episode_ops = self._client.driver.episode_node_ops
            if episode_ops is None:
                return GraphitiOraclePGEpisodeResponse([])
            episodes = await episode_ops.get_by_group_ids(
                self._client.driver,
                [graph_id],
                limit=lastn,
            )
            return GraphitiOraclePGEpisodeResponse([_episode_from_node(ep) for ep in episodes])

        return self._run_async(get_episodes())

    def get_by_user_id(
        self,
        user_id: str,
        *,
        lastn: int | None = None,
        request_options: Any | None = None,
    ) -> GraphitiOraclePGEpisodeResponse:
        return self.get_by_graph_id(user_id, lastn=lastn, request_options=request_options)

    def delete(
        self,
        uuid_: str,
        *,
        request_options: Any | None = None,
    ) -> GraphitiOraclePGSuccessResponse:
        del request_options

        async def delete_episode() -> None:
            episode_ops = self._client.driver.episode_node_ops
            if episode_ops is None:
                return
            await episode_ops.delete_by_uuids(self._client.driver, [uuid_])

        self._run_async(delete_episode())
        return GraphitiOraclePGSuccessResponse()

    def get_nodes_and_edges(self, uuid_: str, *, request_options: Any | None = None) -> Any:
        del request_options
        return self._run_async(self._client.get_nodes_and_edges_by_episode([uuid_]))


class GraphitiOraclePGNodeClient:
    def __init__(self, client: Graphiti, run_async: RunAsync) -> None:
        self._client = client
        self._run_async = run_async

    @property
    def with_raw_response(self) -> GraphitiOraclePGNodeClient:
        return self

    def get_by_graph_id(
        self,
        graph_id: str,
        *,
        limit: int | None = None,
        uuid_cursor: str | None = None,
        request_options: Any | None = None,
    ) -> list[Any]:
        del request_options

        async def get_nodes() -> list[Any]:
            node_ops = self._client.driver.entity_node_ops
            if node_ops is None:
                return []
            return await node_ops.get_by_group_ids(
                self._client.driver,
                [graph_id],
                **_paged_kwargs(limit, uuid_cursor),
            )

        return self._run_async(get_nodes())

    def get_by_user_id(
        self,
        user_id: str,
        *,
        limit: int | None = None,
        uuid_cursor: str | None = None,
        request_options: Any | None = None,
    ) -> list[Any]:
        return self.get_by_graph_id(
            user_id,
            limit=limit,
            uuid_cursor=uuid_cursor,
            request_options=request_options,
        )

    def get(self, uuid_: str, *, request_options: Any | None = None) -> Any:
        del request_options

        async def get_node() -> Any:
            node_ops = self._client.driver.entity_node_ops
            if node_ops is None:
                return None
            return await node_ops.get_by_uuid(self._client.driver, uuid_)

        return self._run_async(get_node())

    def get_edges(self, node_uuid: str, *, request_options: Any | None = None) -> list[Any]:
        del request_options

        async def get_edges_for_node() -> list[Any]:
            edge_ops = self._client.driver.entity_edge_ops
            if edge_ops is None:
                return []
            return await edge_ops.get_by_node_uuid(self._client.driver, node_uuid)

        return self._run_async(get_edges_for_node())

    def get_entity_edges(self, node_uuid: str, *, request_options: Any | None = None) -> list[Any]:
        return self.get_edges(node_uuid, request_options=request_options)


class GraphitiOraclePGEdgeClient:
    def __init__(self, client: Graphiti, run_async: RunAsync) -> None:
        self._client = client
        self._run_async = run_async

    @property
    def with_raw_response(self) -> GraphitiOraclePGEdgeClient:
        return self

    def get_by_graph_id(
        self,
        graph_id: str,
        *,
        limit: int | None = None,
        uuid_cursor: str | None = None,
        request_options: Any | None = None,
    ) -> list[Any]:
        del request_options

        async def get_edges() -> list[Any]:
            edge_ops = self._client.driver.entity_edge_ops
            if edge_ops is None:
                return []
            return await edge_ops.get_by_group_ids(
                self._client.driver,
                [graph_id],
                **_paged_kwargs(limit, uuid_cursor),
            )

        return self._run_async(get_edges())

    def get_by_user_id(
        self,
        user_id: str,
        *,
        limit: int | None = None,
        uuid_cursor: str | None = None,
        request_options: Any | None = None,
    ) -> list[Any]:
        return self.get_by_graph_id(
            user_id,
            limit=limit,
            uuid_cursor=uuid_cursor,
            request_options=request_options,
        )

    def get(self, uuid_: str, *, request_options: Any | None = None) -> Any:
        del request_options

        async def get_edge() -> Any:
            edge_ops = self._client.driver.entity_edge_ops
            if edge_ops is None:
                return None
            return await edge_ops.get_by_uuid(self._client.driver, uuid_)

        return self._run_async(get_edge())


def _episode_data(episode: Any) -> str:
    return str(_episode_value(episode, 'data', ''))


def _episode_value(episode: Any, name: str, default: Any = None) -> Any:
    if isinstance(episode, dict):
        return episode.get(name, default)
    return getattr(episode, name, default)


def _search_config(config: Any, limit: int) -> Any:
    copied_config = config.model_copy(deep=True) if hasattr(config, 'model_copy') else config
    copied_config.limit = limit
    return copied_config


def _episode_type() -> Any:
    from graphiti_core.nodes import EpisodeType

    return EpisodeType


def _graphiti_class() -> Any:
    from graphiti_core import Graphiti

    return Graphiti


def _oracle_pg_driver_class() -> Any:
    from graphiti_core.driver.oracle_pg_driver import OraclePGDriver

    return OraclePGDriver


def _raw_episode_class() -> Any:
    from graphiti_core.utils.bulk_utils import RawEpisode

    return RawEpisode


def _search_config_recipes() -> tuple[Any, Any, Any]:
    from graphiti_core.search.search_config_recipes import (
        COMBINED_HYBRID_SEARCH_CROSS_ENCODER,
        EDGE_HYBRID_SEARCH_RRF,
        NODE_HYBRID_SEARCH_RRF,
    )

    return (
        COMBINED_HYBRID_SEARCH_CROSS_ENCODER,
        EDGE_HYBRID_SEARCH_RRF,
        NODE_HYBRID_SEARCH_RRF,
    )
