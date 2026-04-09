"""Document walker — converts Python documents to (path, HelixType) observations."""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa

from helix_ir.exceptions import CyclicReferenceError
from helix_ir.types.core import HelixType

MAX_DEPTH = 50


def walk_document(
    doc: dict[str, Any],
    observations: dict[str, list[HelixType]] | None = None,
) -> dict[str, list[HelixType]]:
    """Walk a document and collect path -> [HelixType] observations.

    Args:
        doc: The document to walk.
        observations: Accumulator dict. If None, a new dict is created.

    Returns:
        Dict mapping path strings to lists of observed HelixType values.
    """
    if observations is None:
        observations = {}
    _walk_value("", doc, observations, depth=0, seen_ids=set())
    return observations


def _infer_type(value: Any) -> pa.DataType:
    """Map a Python value to its Arrow DataType."""
    if value is None:
        return pa.null()
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    if isinstance(value, str):
        return pa.string()
    if isinstance(value, bytes):
        return pa.binary()
    if isinstance(value, datetime.datetime):
        return pa.timestamp("us")
    if isinstance(value, datetime.date):
        return pa.date32()
    if isinstance(value, Decimal):
        return pa.decimal128(38, 18)
    if isinstance(value, list):
        return pa.list_(pa.string())  # placeholder; resolved during merge
    if isinstance(value, dict):
        # Build struct type from dict keys
        fields = []
        for k, v in value.items():
            fields.append(pa.field(k, _infer_type(v)))
        return pa.struct(fields)
    # Fallback
    return pa.string()


def _walk_value(  # noqa: C901
    path: str,
    value: Any,
    observations: dict[str, list[HelixType]],
    depth: int,
    seen_ids: set[int],
) -> None:
    """Recursively walk a value, recording observations."""
    if depth > MAX_DEPTH:
        raise CyclicReferenceError(
            f"Maximum recursion depth ({MAX_DEPTH}) exceeded at path {path!r}. "
            "This may indicate a cyclic reference."
        )

    if isinstance(value, dict):
        obj_id = id(value)
        if obj_id in seen_ids:
            raise CyclicReferenceError(
                f"Cyclic reference detected at path {path!r}"
            )
        seen_ids = seen_ids | {obj_id}

        for key, val in value.items():
            child_path = f"{path}.{key}" if path else key
            _walk_value(child_path, val, observations, depth + 1, seen_ids)

    elif isinstance(value, list):
        elem_path = f"{path}[]" if path else "[]"
        if not value:
            # Empty list: record null element
            if elem_path not in observations:
                observations[elem_path] = []
            observations[elem_path].append(HelixType(arrow_type=pa.null(), sample_count=1))
        else:
            for item in value:
                _walk_value(elem_path, item, observations, depth + 1, seen_ids)

    else:
        if not path:
            return
        if path not in observations:
            observations[path] = []
        arrow_type = _infer_type(value)
        ht = HelixType(
            arrow_type=arrow_type,
            null_ratio=1.0 if value is None else 0.0,
            sample_count=1,
        )
        observations[path].append(ht)
