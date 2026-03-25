"""
Oracle SQL helper utilities for Graphiti's native Oracle backend.
"""

from __future__ import annotations

import json
from typing import Any, TypeVar

T = TypeVar('T')


def dumps_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, default=str)


def loads_json(value: Any, default: T) -> T:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value  # type: ignore[return-value]
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        if value.strip() == '':
            return default
        try:
            return json.loads(value)
        except Exception:
            return default
    return default


def build_in_clause(
    column: str, param_prefix: str, values: list[Any]
) -> tuple[str, dict[str, Any]]:
    if not values:
        return '1 = 0', {}

    params: dict[str, Any] = {}
    placeholders: list[str] = []
    for idx, value in enumerate(values):
        key = f'{param_prefix}_{idx}'
        placeholders.append(f'${key}')
        params[key] = value

    return f'{column} IN ({", ".join(placeholders)})', params
