"""Handle polymorphic (union) types during normalization."""

from __future__ import annotations

import pyarrow as pa

from helix_ir.types.core import HelixType
from helix_ir.types.semantic import JSONBLOB_TYPE


def resolve_union(ht: HelixType) -> HelixType:
    """Resolve a union or JsonBlob type to a concrete type for DDL.

    Union types are represented as semantic='union:<types>' or semantic='json_blob'.
    In DDL, these are typically rendered as VARCHAR/TEXT/JSON.
    """
    if ht.semantic == JSONBLOB_TYPE:
        return ht  # Already handled by dialect as JSON type

    if ht.semantic and ht.semantic.startswith("union:"):
        # Prefer the widest concrete type
        parts = ht.semantic[6:].split("|")
        if len(parts) == 1:
            # Single-member union: unwrap
            from helix_ir.schema.serialization import _str_to_arrow_type
            try:
                arrow = _str_to_arrow_type(parts[0])
                return ht.evolve(arrow_type=arrow, semantic=None)
            except Exception:
                pass
        # Multi-member union: fall back to string
        return ht.evolve(arrow_type=pa.string(), semantic=None)

    return ht
