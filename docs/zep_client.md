# Zep Graph Client Surface

This package provides a small Graphiti-backed facade for the Zep graph client shape used by `zep_graph`.

## Zep Shape Used By The Backend

The backend mostly talks to Zep through `client.graph.node` and `client.graph.edge`.

### Node Client

```python
client.graph.node.get_by_graph_id(
    graph_id: str,
    *,
    limit: int | None = None,
    uuid_cursor: str | None = None,
    request_options: object | None = None,
) -> list[EntityNode]

client.graph.node.get(
    uuid_: str,
    *,
    request_options: object | None = None,
) -> EntityNode

client.graph.node.get_edges(
    node_uuid: str,
    *,
    request_options: object | None = None,
) -> list[EntityEdge]
```

`zep_graph` also has a call site for `client.graph.node.get_entity_edges(...)`. The installed Zep SDK exposes `get_edges(...)`, so the Graphiti facade supports both names and sends both to the same implementation.

### Edge Client

```python
client.graph.edge.get_by_graph_id(
    graph_id: str,
    *,
    limit: int | None = None,
    uuid_cursor: str | None = None,
    request_options: object | None = None,
) -> list[EntityEdge]

client.graph.edge.get(
    uuid_: str,
    *,
    request_options: object | None = None,
) -> EntityEdge
```

## Graphiti Mapping

Graphiti partitions graph data by `group_id`. For this backend, Zep `graph_id` maps directly to Graphiti `group_id`.

| Zep-compatible method | Graphiti core operation |
| --- | --- |
| `graph.node.get_by_graph_id(graph_id, ...)` | `driver.entity_node_ops.get_by_group_ids(driver, [graph_id], ...)` |
| `graph.node.get(uuid_)` | `driver.entity_node_ops.get_by_uuid(driver, uuid_)` |
| `graph.node.get_edges(node_uuid)` | `driver.entity_edge_ops.get_by_node_uuid(driver, node_uuid)` |
| `graph.node.get_entity_edges(node_uuid)` | `driver.entity_edge_ops.get_by_node_uuid(driver, node_uuid)` |
| `graph.edge.get_by_graph_id(graph_id, ...)` | `driver.entity_edge_ops.get_by_group_ids(driver, [graph_id], ...)` |
| `graph.edge.get(uuid_)` | `driver.entity_edge_ops.get_by_uuid(driver, uuid_)` |

The facade returns raw Graphiti `EntityNode` and `EntityEdge` objects. The backend adapter remains responsible for converting those objects into `GraphNode` and `GraphEdge` dataclasses.

## Pagination

Zep's graph-wide node and edge reads accept `limit` and `uuid_cursor`. Graphiti's `get_by_group_ids(...)` supports the same cursor shape, so the facade forwards those values when supplied.

Node incident-edge reads do not paginate in the inspected Zep SDK and map to Graphiti's `get_by_node_uuid(...)`.
