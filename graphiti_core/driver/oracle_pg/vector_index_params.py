"""
Optional parameters for Oracle PG vector (IVF) indexes.
"""

from __future__ import annotations

from dataclasses import dataclass

_ALLOWED_INDEX_TYPES = frozenset({'IVF'})
_ALLOWED_DISTANCE_METRICS = frozenset({'COSINE', 'EUCLIDEAN', 'DOT'})

_DEFAULT_INDEX_TYPE = 'IVF'
_DEFAULT_DISTANCE_METRIC = 'COSINE'
_DEFAULT_TARGET_ACCURACY = 90
_DEFAULT_NEIGHBOR_PARTITIONS = 10


def _strict_positive_int(value: int | None, default: int) -> int:
    if type(value) is int and value > 0:
        return value
    return default


@dataclass(frozen=True)
class OraclePGVectorIndexParams:
    """All fields optional; defaults match common Oracle vector IVF settings."""

    index_type: str | None = None
    distance_metric: str | None = None
    target_accuracy: int | None = None
    neighbor_partitions: int | None = None

    def resolved_sql_tokens(self) -> tuple[str, str, int, int]:
        index_type = (self.index_type or _DEFAULT_INDEX_TYPE).strip().upper()
        if index_type not in _ALLOWED_INDEX_TYPES:
            raise ValueError(
                f'OraclePGVectorIndexParams.index_type must be one of {sorted(_ALLOWED_INDEX_TYPES)}, '
                f'got {self.index_type!r}'
            )
        distance_metric = (self.distance_metric or _DEFAULT_DISTANCE_METRIC).strip().upper()
        if distance_metric not in _ALLOWED_DISTANCE_METRICS:
            raise ValueError(
                f'OraclePGVectorIndexParams.distance_metric must be one of '
                f'{sorted(_ALLOWED_DISTANCE_METRICS)}, got {self.distance_metric!r}'
            )
        target_accuracy = _strict_positive_int(self.target_accuracy, _DEFAULT_TARGET_ACCURACY)
        neighbor_partitions = _strict_positive_int(
            self.neighbor_partitions, _DEFAULT_NEIGHBOR_PARTITIONS
        )
        return index_type, distance_metric, target_accuracy, neighbor_partitions
