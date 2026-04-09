"""PyArrow interoperability: convert HelixType <-> pa.DataType."""

from __future__ import annotations

import pyarrow as pa

from helix_ir.types.core import HelixType


def helix_type_to_arrow(ht: HelixType) -> pa.DataType:
    """Return the Arrow DataType for this HelixType."""
    return ht.arrow_type


def arrow_to_helix_type(arrow_type: pa.DataType) -> HelixType:
    """Wrap an Arrow DataType in a HelixType with default metadata."""
    return HelixType(arrow_type=arrow_type)


def helix_schema_to_arrow(fields: list[tuple[str, HelixType]]) -> pa.Schema:
    """Convert a list of (name, HelixType) pairs to a pyarrow Schema."""
    arrow_fields: list[pa.Field] = []
    for name, ht in fields:
        metadata: dict[bytes, bytes] = {}
        if ht.semantic:
            metadata[b"helix.semantic"] = ht.semantic.encode()
        if ht.pii_class:
            metadata[b"helix.pii_class"] = ht.pii_class.encode()
        if ht.description:
            metadata[b"helix.description"] = ht.description.encode()
        field = pa.field(name, ht.arrow_type, nullable=ht.null_ratio > 0, metadata=metadata)
        arrow_fields.append(field)
    return pa.schema(arrow_fields)


def arrow_schema_to_helix(schema: pa.Schema) -> list[tuple[str, HelixType]]:
    """Convert a pyarrow Schema to a list of (name, HelixType) pairs."""
    result: list[tuple[str, HelixType]] = []
    for field in schema:
        meta = field.metadata or {}
        semantic = meta.get(b"helix.semantic", b"").decode() or None
        pii_class = meta.get(b"helix.pii_class", b"").decode() or None
        description = meta.get(b"helix.description", b"").decode() or None
        ht = HelixType(
            arrow_type=field.type,
            null_ratio=0.0 if not field.nullable else 0.0,
            semantic=semantic,
            pii_class=pii_class,
            description=description,
        )
        result.append((field.name, ht))
    return result
