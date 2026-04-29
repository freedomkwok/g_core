from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any


class GraphitiOntologyRegistry:
    def __init__(self, max_size: int = 200) -> None:
        self._max_size = max_size
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, dict[str, dict[str, Any]]] = OrderedDict()

    @staticmethod
    def graph_id(value: Any) -> str:
        return str(value or '').strip()

    def set(
        self,
        graph_id: str,
        entities: dict[str, Any] | None = None,
        edges: dict[str, Any] | None = None,
    ) -> None:
        normalized_graph_id = self.graph_id(graph_id)
        if not normalized_graph_id:
            return

        with self._lock:
            self._entries.pop(normalized_graph_id, None)
            self._entries[normalized_graph_id] = {
                'entities': dict(entities or {}),
                'edges': dict(edges or {}),
            }
            self._entries.move_to_end(normalized_graph_id)
            while len(self._entries) > self._max_size:
                self._entries.popitem(last=False)

    def remove(self, graph_id: str) -> None:
        normalized_graph_id = self.graph_id(graph_id)
        if not normalized_graph_id:
            return

        with self._lock:
            self._entries.pop(normalized_graph_id, None)

    def get(self, graph_id: str) -> dict[str, dict[str, Any]]:
        normalized_graph_id = self.graph_id(graph_id)
        if not normalized_graph_id:
            return {}

        with self._lock:
            entry = self._entries.get(normalized_graph_id)
            if entry is None:
                return {}
            self._entries.move_to_end(normalized_graph_id)
            return {
                'entities': dict(entry.get('entities') or {}),
                'edges': dict(entry.get('edges') or {}),
            }

    def graphiti_kwargs(self, graph_id: str | None) -> dict[str, Any]:
        if graph_id is None:
            return {}

        entry = self.get(graph_id)
        if not entry:
            return {}

        entity_types = entry.get('entities')
        if not isinstance(entity_types, dict):
            entity_types = {}

        edges = entry.get('edges')
        if not isinstance(edges, dict):
            edges = {}

        edge_types: dict[str, Any] = {}
        edge_type_map: dict[tuple[str, str], list[str]] = {}

        for edge_name, edge_definition in edges.items():
            normalized_edge_name = str(edge_name or '').strip()
            if not normalized_edge_name:
                continue

            edge_class, source_targets = self._edge_class_and_source_targets(edge_definition)
            if edge_class is not None:
                edge_types[normalized_edge_name] = edge_class

            for source_target in source_targets:
                pair = self._source_target_pair(source_target)
                if pair is None:
                    continue
                edge_type_map.setdefault(pair, [])
                if normalized_edge_name not in edge_type_map[pair]:
                    edge_type_map[pair].append(normalized_edge_name)

        kwargs: dict[str, Any] = {}
        if entity_types:
            kwargs['entity_types'] = entity_types
        if edge_types:
            kwargs['edge_types'] = edge_types
        if edge_type_map:
            kwargs['edge_type_map'] = edge_type_map
        return kwargs

    @staticmethod
    def _edge_class_and_source_targets(edge_definition: Any) -> tuple[Any | None, list[Any]]:
        if isinstance(edge_definition, tuple) and len(edge_definition) >= 2:
            return edge_definition[0], list(edge_definition[1] or [])
        if isinstance(edge_definition, dict):
            return (
                edge_definition.get('edge_type') or edge_definition.get('model'),
                list(edge_definition.get('source_targets', []) or []),
            )
        return edge_definition, []

    @staticmethod
    def _source_target_pair(source_target: Any) -> tuple[str, str] | None:
        if isinstance(source_target, dict):
            source = str(source_target.get('source', '')).strip()
            target = str(source_target.get('target', '')).strip()
        else:
            source = str(getattr(source_target, 'source', '')).strip()
            target = str(getattr(source_target, 'target', '')).strip()
        if source and target:
            return source, target
        return None
