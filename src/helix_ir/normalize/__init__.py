"""helix_ir.normalize — schema normalization."""

from __future__ import annotations

from helix_ir.normalize.decomposer import decompose
from helix_ir.normalize.plan import ForeignKey, NormalizationPlan
from helix_ir.schema.schema import Schema


def normalize(
    schema: Schema,
    strategy: str = "1nf",
    inline_threshold: int = 5,
) -> NormalizationPlan:
    """Normalize a nested Schema into a set of relational tables.

    Args:
        schema: The source schema (may contain nested structs and arrays).
        strategy: Normalization strategy:
            '1nf'          — extract all arrays to child tables, flatten structs.
            'mongo'        — keep arrays and structs as JSON/SUPER columns.
            'inline_small' — inline small arrays (cardinality <= inline_threshold).
            'custom'       — same as '1nf' (extend by subclassing decomposer).
        inline_threshold: For 'inline_small', max cardinality to inline.

    Returns:
        A NormalizationPlan with one or more Schema tables and FK relationships.
    """
    return decompose(
        schema=schema,
        strategy=strategy,
        inline_threshold=inline_threshold,
    )


__all__ = [
    "normalize",
    "NormalizationPlan",
    "ForeignKey",
]
