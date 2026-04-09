"""Base DDL dialect with common SQL generation logic."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from helix_ir.exceptions import DDLCompilationError
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


@dataclass
class DDLOptions:
    """Options controlling DDL generation."""

    if_not_exists: bool = True
    include_comments: bool = True
    primary_key: str | None = "__id"
    schema_prefix: str | None = None
    extra_options: dict[str, Any] = field(default_factory=dict)


@dataclass
class DDLScript:
    """A compiled DDL script."""

    dialect: str
    statements: list[str] = field(default_factory=list)

    def add(self, stmt: str) -> None:
        self.statements.append(stmt.strip())

    def to_sql(self) -> str:
        """Return all statements joined by double newlines."""
        return "\n\n".join(self.statements)

    def __str__(self) -> str:
        return self.to_sql()


class BaseDialect:
    """Base class for DDL dialects."""

    DIALECT_NAME: str = "base"

    # Type mapping: Arrow type string → SQL type string
    TYPE_MAP: dict[str, str] = {
        "null": "VARCHAR",
        "bool": "BOOLEAN",
        "int8": "SMALLINT",
        "int16": "SMALLINT",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "uint8": "SMALLINT",
        "uint16": "INTEGER",
        "uint32": "BIGINT",
        "uint64": "BIGINT",
        "float16": "REAL",
        "float32": "REAL",
        "float64": "DOUBLE PRECISION",
        "string": "VARCHAR",
        "large_string": "VARCHAR",
        "binary": "BYTEA",
        "date32": "DATE",
        "date64": "DATE",
    }

    def arrow_to_sql_type(self, arrow_type: pa.DataType, ht: HelixType | None = None) -> str:  # noqa: C901
        """Map an Arrow DataType to a SQL type string."""
        from helix_ir.schema.serialization import _arrow_type_to_str
        from helix_ir.types.semantic import JSONBLOB_TYPE

        # Check semantic override
        if ht and ht.semantic == JSONBLOB_TYPE:
            return self._json_type()

        key = _arrow_type_to_str(arrow_type)

        if key in self.TYPE_MAP:
            return self.TYPE_MAP[key]

        if pa.types.is_timestamp(arrow_type):
            return self._timestamp_type(arrow_type)

        if pa.types.is_decimal(arrow_type):
            return f"DECIMAL({arrow_type.precision},{arrow_type.scale})"

        if pa.types.is_list(arrow_type):
            return self._list_type(arrow_type)

        if pa.types.is_struct(arrow_type):
            return self._struct_type(arrow_type)

        if pa.types.is_null(arrow_type):
            return "VARCHAR"

        # Fallback
        return "VARCHAR"

    def _timestamp_type(self, t: pa.TimestampType) -> str:
        return "TIMESTAMP"

    def _json_type(self) -> str:
        return "VARCHAR"

    def _list_type(self, t: pa.ListType) -> str:
        inner = self.arrow_to_sql_type(t.value_type)
        return f"{inner}[]"

    def _struct_type(self, t: pa.StructType) -> str:
        return "VARCHAR"  # Fallback; overridden in dialect subclasses

    def quote_identifier(self, name: str) -> str:
        """Quote a SQL identifier."""
        return f'"{name}"'

    def compile_create_table(
        self,
        schema: Schema,
        options: DDLOptions,
    ) -> str:
        """Generate a CREATE TABLE statement."""
        table_name = self._table_name(schema.name, options)
        column_defs: list[str] = []

        for fname, ht in schema.fields:
            col_def = self._column_def(fname, ht)
            column_defs.append(f"  {col_def}")

        # Primary key constraint
        if options.primary_key and options.primary_key in dict(schema.fields):
            column_defs.append(f"  PRIMARY KEY ({self.quote_identifier(options.primary_key)})")

        exists_clause = "IF NOT EXISTS " if options.if_not_exists else ""
        body = ",\n".join(column_defs)
        return f"CREATE TABLE {exists_clause}{table_name} (\n{body}\n);"

    def _table_name(self, name: str, options: DDLOptions) -> str:
        if options.schema_prefix:
            return f"{self.quote_identifier(options.schema_prefix)}.{self.quote_identifier(name)}"
        return self.quote_identifier(name)

    def _column_def(self, name: str, ht: HelixType) -> str:
        sql_type = self.arrow_to_sql_type(ht.arrow_type, ht)
        nullable = "NULL" if ht.null_ratio > 0 else "NOT NULL"
        col = f"{self.quote_identifier(name)} {sql_type} {nullable}"
        if ht.description:
            col += f" -- {ht.description}"
        return col

    def compile_add_column(
        self,
        table_name: str,
        column_name: str,
        ht: HelixType,
    ) -> str:
        sql_type = self.arrow_to_sql_type(ht.arrow_type, ht)
        return (
            f"ALTER TABLE {self.quote_identifier(table_name)} "
            f"ADD COLUMN {self.quote_identifier(column_name)} {sql_type};"
        )

    def compile_drop_column(self, table_name: str, column_name: str) -> str:
        return (
            f"ALTER TABLE {self.quote_identifier(table_name)} "
            f"DROP COLUMN {self.quote_identifier(column_name)};"
        )

    def compile_alter_column_type(
        self,
        table_name: str,
        column_name: str,
        new_ht: HelixType,
    ) -> str:
        sql_type = self.arrow_to_sql_type(new_ht.arrow_type, new_ht)
        return (
            f"ALTER TABLE {self.quote_identifier(table_name)} "
            f"ALTER COLUMN {self.quote_identifier(column_name)} TYPE {sql_type};"
        )
