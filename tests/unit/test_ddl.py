"""Unit tests for DDL compilation."""

from __future__ import annotations

import re

import pytest
import pyarrow as pa

from helix_ir.ddl import DDLOptions, compile_ddl
from helix_ir.ddl.dialects import get_dialect
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


def ht(arrow_type: pa.DataType, **kwargs) -> HelixType:
    return HelixType(arrow_type=arrow_type, **kwargs)


# -------------------------------------------------------------------------
# DuckDB dialect (reference)
# -------------------------------------------------------------------------


class TestDuckDBDialect:
    def test_basic_create_table(self, flat_schema) -> None:
        script = compile_ddl(flat_schema, dialect="duckdb")
        sql = script.to_sql()
        assert "CREATE TABLE" in sql
        assert "orders" in sql

    def test_if_not_exists(self, flat_schema) -> None:
        opts = DDLOptions(if_not_exists=True)
        script = compile_ddl(flat_schema, dialect="duckdb", options=opts)
        sql = script.to_sql()
        assert "IF NOT EXISTS" in sql

    def test_bigint_type(self) -> None:
        schema = Schema(
            name="test",
            fields=(("count", ht(pa.int64())),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        assert "BIGINT" in script.to_sql()

    def test_varchar_type(self) -> None:
        schema = Schema(
            name="test",
            fields=(("name", ht(pa.string())),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        assert "VARCHAR" in script.to_sql()

    def test_double_type(self) -> None:
        schema = Schema(
            name="test",
            fields=(("price", ht(pa.float64())),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        assert "DOUBLE" in script.to_sql()

    def test_boolean_type(self) -> None:
        schema = Schema(
            name="test",
            fields=(("active", ht(pa.bool_())),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        assert "BOOLEAN" in script.to_sql()

    def test_timestamp_type(self) -> None:
        schema = Schema(
            name="test",
            fields=(("ts", ht(pa.timestamp("us"))),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        assert "TIMESTAMP" in script.to_sql()

    def test_date_type(self) -> None:
        schema = Schema(
            name="test",
            fields=(("dt", ht(pa.date32())),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        assert "DATE" in script.to_sql()

    def test_null_nullable(self) -> None:
        schema = Schema(
            name="test",
            fields=(("x", ht(pa.int64(), null_ratio=0.1)),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        assert "NULL" in script.to_sql()

    def test_not_null(self) -> None:
        schema = Schema(
            name="test",
            fields=(("x", ht(pa.int64(), null_ratio=0.0)),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        assert "NOT NULL" in script.to_sql()

    def test_json_blob(self) -> None:
        from helix_ir.types.semantic import JSONBLOB_TYPE
        schema = Schema(
            name="test",
            fields=(("data", ht(pa.string(), semantic=JSONBLOB_TYPE)),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        assert "JSON" in script.to_sql()

    def test_struct_type(self) -> None:
        struct_type = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.string())])
        schema = Schema(
            name="test",
            fields=(("nested", ht(struct_type)),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        assert "STRUCT" in script.to_sql()

    def test_list_type(self) -> None:
        schema = Schema(
            name="test",
            fields=(("tags", ht(pa.list_(pa.string()))),),
        )
        script = compile_ddl(schema, dialect="duckdb")
        sql = script.to_sql()
        assert "VARCHAR[]" in sql or "[]" in sql


# -------------------------------------------------------------------------
# PostgreSQL dialect
# -------------------------------------------------------------------------


class TestPostgresDialect:
    def test_basic_create_table(self, flat_schema) -> None:
        script = compile_ddl(flat_schema, dialect="postgres")
        sql = script.to_sql()
        assert "CREATE TABLE" in sql

    def test_bigint(self) -> None:
        schema = Schema(name="test", fields=(("n", ht(pa.int64())),))
        assert "BIGINT" in compile_ddl(schema, dialect="postgres").to_sql()

    def test_double_precision(self) -> None:
        schema = Schema(name="test", fields=(("v", ht(pa.float64())),))
        assert "DOUBLE PRECISION" in compile_ddl(schema, dialect="postgres").to_sql()

    def test_timestamptz(self) -> None:
        import pyarrow as pa
        schema = Schema(name="test", fields=(("ts", ht(pa.timestamp("us", tz="UTC"))),))
        sql = compile_ddl(schema, dialect="postgres").to_sql()
        assert "TIMESTAMPTZ" in sql

    def test_jsonb_for_json_blob(self) -> None:
        from helix_ir.types.semantic import JSONBLOB_TYPE
        schema = Schema(name="test", fields=(("data", ht(pa.string(), semantic=JSONBLOB_TYPE)),))
        sql = compile_ddl(schema, dialect="postgres").to_sql()
        assert "JSONB" in sql

    def test_struct_as_jsonb(self) -> None:
        struct_type = pa.struct([pa.field("x", pa.int64())])
        schema = Schema(name="test", fields=(("obj", ht(struct_type)),))
        sql = compile_ddl(schema, dialect="postgres").to_sql()
        assert "JSONB" in sql


# -------------------------------------------------------------------------
# Normalization plan DDL
# -------------------------------------------------------------------------


class TestNormalizationPlanDDL:
    def test_plan_produces_multiple_tables(self, nested_schema) -> None:
        from helix_ir.normalize import normalize
        plan = normalize(nested_schema, strategy="1nf")
        script = compile_ddl(plan, dialect="duckdb")
        sql = script.to_sql()
        # Should have multiple CREATE TABLE statements
        assert sql.count("CREATE TABLE") >= 2

    def test_plan_includes_fks(self, nested_schema) -> None:
        from helix_ir.normalize import normalize
        plan = normalize(nested_schema, strategy="1nf")
        script = compile_ddl(plan, dialect="duckdb")
        sql = script.to_sql()
        # Foreign key constraints (if any FKs exist)
        if plan.foreign_keys:
            assert "FOREIGN KEY" in sql or "REFERENCES" in sql


# -------------------------------------------------------------------------
# All dialects
# -------------------------------------------------------------------------


@pytest.mark.parametrize("dialect", ["duckdb", "postgres", "redshift", "bigquery", "snowflake", "databricks"])
def test_all_dialects_produce_sql(flat_schema, dialect: str) -> None:
    """All dialects should produce non-empty SQL."""
    script = compile_ddl(flat_schema, dialect=dialect)
    sql = script.to_sql()
    assert "CREATE TABLE" in sql
    assert len(sql) > 50


def test_unknown_dialect_raises(flat_schema) -> None:
    from helix_ir.exceptions import DDLCompilationError
    with pytest.raises(DDLCompilationError):
        compile_ddl(flat_schema, dialect="oracle")
