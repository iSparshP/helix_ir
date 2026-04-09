"""Normalization strategies for choosing how to handle each field."""

from __future__ import annotations

import pyarrow as pa

from helix_ir.types.core import HelixType


def choose_action(
    field_name: str,
    ht: HelixType,
    strategy: str,
    inline_threshold: int = 5,
) -> str:
    """Determine the action for a field given the normalization strategy.

    Returns one of:
        'keep'      — keep the field in the current table
        'extract'   — extract to a child table (1NF for arrays)
        'inline'    — inline as JSON/SUPER (MongoDB strategy for small arrays)
        'flatten'   — flatten struct fields into parent

    Args:
        field_name: The name of the field.
        ht: The HelixType of the field.
        strategy: One of '1nf', 'mongo', 'inline_small'.
        inline_threshold: Max estimated cardinality for inline_small.
    """
    if pa.types.is_list(ht.arrow_type):
        if strategy == "1nf":
            return "extract"
        elif strategy == "mongo":
            return "inline"
        elif strategy == "inline_small":
            # Inline if small cardinality, else extract
            card = ht.cardinality_estimate
            if card is not None and card <= inline_threshold:
                return "inline"
            return "extract"
        else:
            return "extract"

    if pa.types.is_struct(ht.arrow_type):
        if strategy in ("1nf", "inline_small"):
            return "flatten"
        elif strategy == "mongo":
            return "inline"
        else:
            return "flatten"

    return "keep"
