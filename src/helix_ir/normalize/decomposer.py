"""Recursive schema decomposer — splits nested schemas into normalized tables."""

from __future__ import annotations

import uuid

import pyarrow as pa

from helix_ir.normalize.plan import ForeignKey, NormalizationPlan
from helix_ir.normalize.strategies import choose_action
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


def _new_id() -> str:
    """Generate a ULID or UUID for synthetic primary keys."""
    try:
        from ulid import ULID  # type: ignore[import]
        return str(ULID())
    except ImportError:
        return str(uuid.uuid4())


def decompose(
    schema: Schema,
    strategy: str = "1nf",
    plan: NormalizationPlan | None = None,
    parent_table: str | None = None,
    parent_fk_col: str | None = None,
    inline_threshold: int = 5,
) -> NormalizationPlan:
    """Recursively decompose a schema into normalized tables.

    Args:
        schema: The schema to decompose.
        strategy: '1nf', 'mongo', 'inline_small', or 'custom'.
        plan: Accumulator plan (created if None).
        parent_table: Name of parent table (for adding __parent_id FK).
        parent_fk_col: Column in parent that this table references.
        inline_threshold: Used by inline_small strategy.

    Returns:
        A NormalizationPlan with all tables and foreign keys.
    """
    if plan is None:
        plan = NormalizationPlan()

    # Build root table fields
    root_fields: list[tuple[str, HelixType]] = []

    # Add primary key
    id_type = HelixType(
        arrow_type=pa.string(),
        null_ratio=0.0,
        description="Synthetic primary key",
        tags=frozenset(["primary_key"]),
    )
    root_fields.append(("__id", id_type))

    # Add parent FK if this is a child table
    if parent_table is not None and parent_fk_col is not None:
        parent_id_type = HelixType(
            arrow_type=pa.string(),
            null_ratio=0.0,
            description=f"Foreign key to {parent_table}.__id",
            tags=frozenset(["foreign_key"]),
        )
        root_fields.append(("__parent_id", parent_id_type))

        ordinal_type = HelixType(
            arrow_type=pa.int64(),
            null_ratio=0.0,
            description="Array element ordinal position",
        )
        root_fields.append(("__ordinal", ordinal_type))

    # Process each field in the schema
    for fname, ht in schema.fields:
        if fname.startswith("__"):
            # Preserve synthetic fields
            root_fields.append((fname, ht))
            continue

        action = choose_action(fname, ht, strategy, inline_threshold)

        if action == "keep":
            root_fields.append((fname, ht))

        elif action == "inline":
            # Keep as JSON/SUPER column
            from helix_ir.types.semantic import JSONBLOB_TYPE
            inline_ht = ht.evolve(semantic=JSONBLOB_TYPE)
            root_fields.append((fname, inline_ht))

        elif action == "flatten":
            # Flatten struct fields into parent
            if pa.types.is_struct(ht.arrow_type):
                for i in range(ht.arrow_type.num_fields):
                    sub_field = ht.arrow_type.field(i)
                    sub_ht = HelixType(
                        arrow_type=sub_field.type,
                        null_ratio=ht.null_ratio,
                        sample_count=ht.sample_count,
                        source_path=f"{fname}.{sub_field.name}",
                    )
                    flattened_name = f"{fname}__{sub_field.name}"
                    root_fields.append((flattened_name, sub_ht))
            else:
                root_fields.append((fname, ht))

        elif action == "extract":
            # Extract array to child table
            child_schema_name = f"{schema.name}__{fname}"

            if pa.types.is_list(ht.arrow_type):
                elem_type = ht.arrow_type.value_type

                if pa.types.is_struct(elem_type):
                    # Build child schema from struct fields
                    child_fields_raw: list[tuple[str, HelixType]] = []
                    for i in range(elem_type.num_fields):
                        sf = elem_type.field(i)
                        child_fields_raw.append(
                            (sf.name, HelixType(arrow_type=sf.type))
                        )
                    child_schema = Schema(
                        name=child_schema_name,
                        fields=tuple(child_fields_raw),
                    )
                else:
                    # Scalar array
                    child_schema = Schema(
                        name=child_schema_name,
                        fields=(("value", HelixType(arrow_type=elem_type)),),
                    )

                # Recursively decompose child
                decompose(
                    schema=child_schema,
                    strategy=strategy,
                    plan=plan,
                    parent_table=schema.name,
                    parent_fk_col="__id",
                    inline_threshold=inline_threshold,
                )

                # Record FK: child.__parent_id → parent.__id
                plan.foreign_keys.append(
                    ForeignKey(
                        from_table=child_schema_name,
                        from_column="__parent_id",
                        to_table=schema.name,
                        to_column="__id",
                    )
                )

                # Record lineage
                from helix_ir.schema.path import Path
                plan.lineage.record(
                    source=Path.parse(f"{schema.name}.{fname}"),
                    target=Path.parse(child_schema_name),
                    transform="extract_array",
                )

            else:
                root_fields.append((fname, ht))

    # Build and register root schema
    root_schema = Schema(name=schema.name, fields=tuple(root_fields))

    # Insert at position 0 if this is the root table; else append
    if parent_table is None:
        plan.tables.insert(0, root_schema)
    else:
        plan.tables.append(root_schema)

    return plan
