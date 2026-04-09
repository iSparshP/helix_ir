"""Merge path observations into a Schema using the type lattice."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from helix_ir.infer.confidence import SimpleHyperLogLog, compute_confidence
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType
from helix_ir.types.lattice import join
from helix_ir.types.semantic import SEMANTIC_ENUM


def merge_observations(
    observations: dict[str, list[HelixType]],
    name: str,
    sample_values: dict[str, list[Any]] | None = None,
    enum_cardinality_threshold: int = 50,
    enum_sample_threshold: int = 200,
) -> Schema:
    """Fold per-path observations into a Schema.

    Args:
        observations: path string → list of observed HelixType values.
        name: Name for the resulting Schema.
        sample_values: path string → list of raw values (for cardinality/enum).
        enum_cardinality_threshold: Promote to enum if cardinality < this.
        enum_sample_threshold: Only promote to enum if sample_count > this.

    Returns:
        A Schema with one field per top-level path component.
    """
    if sample_values is None:
        sample_values = {}

    # Fold each path's observations via join()
    merged: dict[str, HelixType] = {}
    hlls: dict[str, SimpleHyperLogLog] = {}

    for path, types in observations.items():
        if not types:
            continue
        # Build HLL for this path
        hll = hlls.get(path)
        if hll is None:
            hll = SimpleHyperLogLog()
            hlls[path] = hll

        # Fold values using join
        result = types[0]
        for t in types[1:]:
            result = join(result, t)

        merged[path] = result

    # Add cardinality estimates from sample values
    for path, values in sample_values.items():
        hll = hlls.get(path)
        if hll is None:
            hll = SimpleHyperLogLog()
            hlls[path] = hll
        for v in values:
            if v is not None:
                hll.add(v)

    # Update merged types with cardinality, confidence, and enum promotion
    for path, ht in merged.items():
        hll = hlls.get(path)
        cardinality = hll.estimate() if hll else None

        sample_count = ht.sample_count
        null_ratio = ht.null_ratio
        confidence = compute_confidence(sample_count, null_ratio)

        semantic = ht.semantic
        # Promote to enum if cardinality is low and we have enough samples
        if (
            semantic is None
            and cardinality is not None
            and cardinality < enum_cardinality_threshold
            and sample_count > enum_sample_threshold
            and pa.types.is_string(ht.arrow_type)
        ):
            semantic = SEMANTIC_ENUM

        merged[path] = ht.evolve(
            cardinality_estimate=cardinality,
            confidence=confidence,
            semantic=semantic,
        )

    # Build top-level Schema from merged paths
    # We need to reconstruct nested structure from flat paths
    top_level_fields = _build_schema_fields(merged)
    return Schema(name=name, fields=tuple(top_level_fields))


def _build_schema_fields(
    merged: dict[str, HelixType],
) -> list[tuple[str, HelixType]]:
    """Reconstruct a flat list of (name, HelixType) from flattened path dict."""
    # We only emit top-level fields; nested fields are embedded in struct types
    # But since our walker already emits individual leaf paths, we need to
    # figure out which paths are "top-level" and which are nested.

    # Sort paths to process parents before children
    all_paths = sorted(merged.keys(), key=lambda p: p.count("."))

    # Find top-level paths (no dot, no [])
    top_level_names: list[str] = []
    seen_top: set[str] = set()

    for path in all_paths:
        top = path.split(".")[0].split("[")[0]
        if top and top not in seen_top:
            seen_top.add(top)
            top_level_names.append(top)

    result: list[tuple[str, HelixType]] = []
    for name in top_level_names:
        ht = _resolve_field(name, merged)
        result.append((name, ht))

    return result


def _resolve_field(  # noqa: C901
    name: str,
    merged: dict[str, HelixType],
) -> HelixType:
    """Build a HelixType for `name`, potentially with nested struct/list types."""
    import pyarrow as pa

    # Direct match
    if name in merged:
        base_ht = merged[name]
    else:
        base_ht = None

    # Find all children of this name
    prefix = name + "."
    array_prefix = name + "[]"

    child_paths = [p for p in merged if p.startswith(prefix) or p.startswith(array_prefix)]

    if not child_paths:
        if base_ht is not None:
            return base_ht.evolve(source_path=name)
        return HelixType(arrow_type=pa.null(), source_path=name)

    # Determine if this is an array field
    has_array = any(p.startswith(array_prefix) for p in child_paths)

    if has_array:
        # The field is an array; find element paths
        elem_direct = name + "[]"
        elem_prefix = name + "[]."

        # Children of the array elements
        elem_children = [p for p in child_paths if p.startswith(elem_prefix)]

        if elem_children:
            # Array of structs
            # Strip the "name[]." prefix
            sub_merged = {}
            for p in elem_children:
                sub_key = p[len(elem_direct) + 1:]  # remove "name[]."
                sub_merged[sub_key] = merged[p]
            # Also add direct elem observations
            if elem_direct in merged:
                elem_direct_ht = merged[elem_direct]
            else:
                elem_direct_ht = None

            sub_fields = _build_schema_fields(sub_merged)
            if sub_fields:
                struct_type = pa.struct([pa.field(n, t.arrow_type) for n, t in sub_fields])
                list_type = pa.list_(struct_type)
                return HelixType(
                    arrow_type=list_type,
                    null_ratio=base_ht.null_ratio if base_ht else 0.0,
                    sample_count=base_ht.sample_count if base_ht else 0,
                    source_path=name,
                )
            elif elem_direct_ht is not None:
                return HelixType(
                    arrow_type=pa.list_(elem_direct_ht.arrow_type),
                    null_ratio=base_ht.null_ratio if base_ht else 0.0,
                    sample_count=base_ht.sample_count if base_ht else 0,
                    source_path=name,
                )
        else:
            # Array of scalars
            if elem_direct in merged:
                elem_ht = merged[elem_direct]
                return HelixType(
                    arrow_type=pa.list_(elem_ht.arrow_type),
                    null_ratio=base_ht.null_ratio if base_ht else 0.0,
                    sample_count=base_ht.sample_count if base_ht else 0,
                    source_path=name,
                )

    # Struct field: find immediate children
    immediate_children: dict[str, bool] = {}
    for p in child_paths:
        rest = p[len(prefix):]
        top = rest.split(".")[0].split("[")[0]
        if top:
            immediate_children[top] = True

    if immediate_children:
        sub_merged = {}
        for p in child_paths:
            rest = p[len(prefix):]
            sub_merged[rest] = merged[p]

        sub_fields = _build_schema_fields(sub_merged)
        struct_type = pa.struct([pa.field(n, t.arrow_type) for n, t in sub_fields])
        return HelixType(
            arrow_type=struct_type,
            null_ratio=base_ht.null_ratio if base_ht else 0.0,
            sample_count=base_ht.sample_count if base_ht else 0,
            source_path=name,
        )

    if base_ht is not None:
        return base_ht.evolve(source_path=name)

    return HelixType(arrow_type=pa.null(), source_path=name)
