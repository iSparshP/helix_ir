"""Lazy Table class — the primary user-facing transform API."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from helix_ir.schema.schema import Schema
from helix_ir.transform.compiler.logical import (
    Aggregate,
    Filter,
    Join,
    Limit,
    Project,
    RawSQL,
    Scan,
    Sort,
    Union,
)
from helix_ir.transform.expression import Expression, SortExpr, col, star


class Table:
    """A lazy relational table backed by a LogicalPlan.

    Operations return new Table instances without executing queries.
    Call .to_sql() to compile or .to_arrow() to execute via DuckDB.
    """

    def __init__(self, source_name: str, schema: Schema | None = None) -> None:
        self._plan: object = Scan(source_name=source_name, schema=schema)
        self._schema = schema

    @classmethod
    def _from_plan(cls, plan: object, schema: Schema | None = None) -> "Table":
        t = cls.__new__(cls)
        t._plan = plan
        t._schema = schema
        return t

    # -------------------------------------------------------------------------
    # Transformations (all lazy)
    # -------------------------------------------------------------------------

    def filter(self, predicate: Expression) -> "Table":
        """Filter rows by a predicate expression."""
        return Table._from_plan(Filter(input=self._plan, predicate=predicate))

    def where(self, predicate: Expression) -> "Table":
        """Alias for filter()."""
        return self.filter(predicate)

    def select(self, *exprs: Expression) -> "Table":
        """Select specific columns."""
        return Table._from_plan(Project(input=self._plan, columns=list(exprs)))

    def with_column(self, name: str, expr: Expression) -> "Table":
        """Add or replace a column."""
        aliased = expr.alias(name)
        current_cols = self._current_columns()
        if current_cols:
            # Replace existing or add
            new_cols = [aliased if c._alias == name else c for c in current_cols]
            if all(c._alias != name for c in current_cols):
                new_cols.append(aliased)
        else:
            new_cols = [star(), aliased]
        return Table._from_plan(Project(input=self._plan, columns=new_cols))

    def drop(self, *columns: str) -> "Table":
        """Drop columns by name."""
        current = self._current_columns()
        if current:
            new_cols = [c for c in current if not (isinstance(c, type(col("x"))) and c.name in columns)]
        else:
            # Can't drop without knowing columns; use EXCEPT
            # DuckDB supports SELECT * EXCEPT (col1, col2)
            from helix_ir.transform.expression import Expression as Expr
            class ExceptExpr(Expr):
                def __init__(self, cols_to_drop: list[str]) -> None:
                    super().__init__()
                    self.cols_to_drop = cols_to_drop
                def _copy(self) -> "ExceptExpr":
                    return ExceptExpr(self.cols_to_drop)
                def to_sql(self, dialect: str = "duckdb") -> str:
                    drops = ", ".join(f'"{c}"' for c in self.cols_to_drop)
                    return f"* EXCEPT ({drops})"
            new_cols = [ExceptExpr(list(columns))]
        return Table._from_plan(Project(input=self._plan, columns=new_cols))

    def rename(self, **mapping: str) -> "Table":
        """Rename columns: rename(old_name='new_name')."""
        current = self._current_columns()
        new_cols: list[Expression] = []
        if current:
            for c in current:
                from helix_ir.transform.expression import Column
                if isinstance(c, Column) and c.name in mapping:
                    new_cols.append(c.alias(mapping[c.name]))
                else:
                    new_cols.append(c)
        else:
            # Build explicit renames
            new_cols = [star()]
            for old, new in mapping.items():
                new_cols.append(col(old).alias(new))
        return Table._from_plan(Project(input=self._plan, columns=new_cols))

    def sort(self, *exprs: Expression | SortExpr) -> "Table":
        """Sort by expressions."""
        return Table._from_plan(Sort(input=self._plan, by=list(exprs)))

    def order_by(self, *exprs: Expression | SortExpr) -> "Table":
        """Alias for sort()."""
        return self.sort(*exprs)

    def limit(self, n: int, offset: int = 0) -> "Table":
        """Limit to n rows, with optional offset."""
        return Table._from_plan(Limit(input=self._plan, n=n, offset=offset))

    def head(self, n: int = 5) -> "Table":
        """Return first n rows."""
        return self.limit(n)

    def group_by(self, *exprs: Expression) -> "operators.GroupedTable":
        """Group by expressions — returns a GroupedTable for .agg()."""
        from helix_ir.transform.operators import GroupedTable
        return GroupedTable(self, list(exprs))

    def join(
        self,
        other: "Table",
        on: Expression,
        how: str = "inner",
    ) -> "Table":
        """Join with another table."""
        return Table._from_plan(
            Join(left=self._plan, right=other._plan, on=on, how=how)
        )

    def union(self, other: "Table", all: bool = True) -> "Table":
        """Union with another table."""
        return Table._from_plan(
            Union(left=self._plan, right=other._plan, all=all)
        )

    def distinct(self) -> "Table":
        """Return distinct rows (wraps in a SELECT DISTINCT)."""
        return Table._from_plan(
            RawSQL(sql=f"SELECT DISTINCT * FROM ({self.to_sql()})", alias="distinct")
        )

    # -------------------------------------------------------------------------
    # Compilation / execution
    # -------------------------------------------------------------------------

    def to_sql(self, dialect: str = "duckdb") -> str:
        """Compile this table to a SQL SELECT statement."""
        from helix_ir.transform.compiler.emitters import get_emitter
        from helix_ir.transform.compiler.optimizer import optimize

        optimized = optimize(self._plan)
        emitter = get_emitter(dialect)
        return emitter.emit(optimized)

    def to_arrow(self) -> pa.Table:
        """Execute this query via DuckDB and return a PyArrow Table."""
        import duckdb

        sql = self.to_sql(dialect="duckdb")
        conn = duckdb.connect()
        return conn.execute(sql).arrow()

    def _current_columns(self) -> list[Expression]:
        """Return current projected columns if a Project node is at top."""
        if isinstance(self._plan, Project):
            return list(self._plan.columns)
        return []

    def __repr__(self) -> str:
        try:
            sql = self.to_sql()
            return f"Table(\n{sql}\n)"
        except Exception:
            return f"Table({type(self._plan).__name__})"
