"""DDL compilation: produce CREATE TABLE / ALTER TABLE SQL from a NormalizationPlan."""

from __future__ import annotations

from typing import Any

from helix_ir.ddl.dialects import DDLOptions, DDLScript, get_dialect
from helix_ir.ddl.dialects.base import BaseDialect
from helix_ir.diff.classifier import SchemaDiff, SchemaChange
from helix_ir.exceptions import DDLCompilationError
from helix_ir.schema.schema import Schema


def compile_ddl(
    schema_or_plan: Any,
    dialect: str = "duckdb",
    options: DDLOptions | None = None,
) -> DDLScript:
    """Compile a Schema or NormalizationPlan to a DDL script.

    Args:
        schema_or_plan: A Schema instance or NormalizationPlan instance.
        dialect: Target SQL dialect name.
        options: DDL generation options.

    Returns:
        A DDLScript with all CREATE TABLE statements.
    """
    if options is None:
        options = DDLOptions()

    try:
        engine = get_dialect(dialect)
    except ValueError as e:
        raise DDLCompilationError(str(e)) from e

    script = DDLScript(dialect=dialect)

    # Handle NormalizationPlan
    if hasattr(schema_or_plan, "tables"):
        plan = schema_or_plan
        for table_schema in plan.tables:
            stmt = engine.compile_create_table(table_schema, options)
            script.add(stmt)
        # Add foreign key constraints
        for fk in plan.foreign_keys:
            fk_stmt = _compile_foreign_key(fk, engine, dialect)
            if fk_stmt:
                script.add(fk_stmt)
    elif isinstance(schema_or_plan, Schema):
        stmt = engine.compile_create_table(schema_or_plan, options)
        script.add(stmt)
    else:
        raise DDLCompilationError(
            f"Expected a Schema or NormalizationPlan, got {type(schema_or_plan).__name__}"
        )

    return script


def compile_migration(
    schema_diff: SchemaDiff,
    dialect: str = "duckdb",
    table_name: str | None = None,
    options: DDLOptions | None = None,
    skip_breaking: bool = False,
) -> DDLScript:
    """Compile a SchemaDiff into ALTER TABLE migration statements.

    Args:
        schema_diff: The diff between old and new schema.
        dialect: Target SQL dialect.
        table_name: Table name to alter. Defaults to schema_diff.new_name.
        options: DDL options.
        skip_breaking: If True, skip breaking changes (dangerous!).

    Returns:
        A DDLScript with ALTER TABLE statements.
    """
    if options is None:
        options = DDLOptions()

    try:
        engine = get_dialect(dialect)
    except ValueError as e:
        raise DDLCompilationError(str(e)) from e

    tname = table_name or schema_diff.new_name
    script = DDLScript(dialect=dialect)

    if schema_diff.has_breaking_changes and not skip_breaking:
        breaking = [c for c in schema_diff.changes if c.severity == "breaking"]
        msg = "\n".join(f"  - {c.description}" for c in breaking)
        raise DDLCompilationError(
            f"Schema diff contains breaking changes:\n{msg}\n"
            "Pass skip_breaking=True to generate migration anyway."
        )

    for change in schema_diff.changes:
        if change.severity == "breaking" and skip_breaking:
            script.add(f"-- BREAKING CHANGE SKIPPED: {change.description}")
            continue

        stmt = _compile_change(change, tname, engine)
        if stmt:
            script.add(stmt)

    return script


def _compile_change(
    change: SchemaChange,
    table_name: str,
    engine: BaseDialect,
) -> str | None:
    """Compile a single SchemaChange to a SQL statement."""
    if change.kind == "added" and change.new_type is not None:
        return engine.compile_add_column(
            table_name,
            str(change.path),
            change.new_type,
        )
    elif change.kind == "removed":
        return engine.compile_drop_column(table_name, str(change.path))
    elif change.kind == "type_changed" and change.new_type is not None:
        return engine.compile_alter_column_type(
            table_name,
            str(change.path),
            change.new_type,
        )
    elif change.kind in ("nullable_changed", "semantic_changed", "pii_changed"):
        return f"-- {change.severity.upper()}: {change.description}"
    return None


def _compile_foreign_key(fk: Any, engine: BaseDialect, dialect: str) -> str | None:
    """Compile a ForeignKey to a SQL ALTER TABLE ADD CONSTRAINT statement."""
    from_table = engine.quote_identifier(fk.from_table)
    from_col = engine.quote_identifier(fk.from_column)
    to_table = engine.quote_identifier(fk.to_table)
    to_col = engine.quote_identifier(fk.to_column)
    constraint_name = engine.quote_identifier(
        f"fk_{fk.from_table}_{fk.from_column}"
    )
    return (
        f"ALTER TABLE {from_table} "
        f"ADD CONSTRAINT {constraint_name} "
        f"FOREIGN KEY ({from_col}) REFERENCES {to_table}({to_col});"
    )
