"""Expression DSL for building SQL expressions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from helix_ir.types.core import HelixType


class Expression:
    """Base class for all expressions in the Helix IR query DSL."""

    def __init__(self) -> None:
        self._alias: str | None = None

    def alias(self, name: str) -> "Expression":
        """Assign an alias to this expression."""
        e = self._copy()
        e._alias = name
        return e

    def _copy(self) -> "Expression":
        raise NotImplementedError

    # Comparison operators
    def __eq__(self, other: Any) -> "BinaryExpr":  # type: ignore[override]
        return BinaryExpr(self, "=", _wrap(other))

    def __ne__(self, other: Any) -> "BinaryExpr":  # type: ignore[override]
        return BinaryExpr(self, "<>", _wrap(other))

    def __lt__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "<", _wrap(other))

    def __le__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "<=", _wrap(other))

    def __gt__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, ">", _wrap(other))

    def __ge__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, ">=", _wrap(other))

    # Arithmetic operators
    def __add__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "+", _wrap(other))

    def __radd__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(_wrap(other), "+", self)

    def __sub__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "-", _wrap(other))

    def __rsub__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(_wrap(other), "-", self)

    def __mul__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "*", _wrap(other))

    def __rmul__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(_wrap(other), "*", self)

    def __truediv__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "/", _wrap(other))

    def __rtruediv__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(_wrap(other), "/", self)

    # Logical operators
    def __and__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "AND", _wrap(other))

    def __or__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "OR", _wrap(other))

    def __invert__(self) -> "UnaryExpr":
        return UnaryExpr("NOT", self)

    # Null checks
    def is_null(self) -> "FunctionExpr":
        return FunctionExpr("IS NULL", [self], postfix=True)

    def is_not_null(self) -> "FunctionExpr":
        return FunctionExpr("IS NOT NULL", [self], postfix=True)

    # String methods
    def like(self, pattern: str) -> "BinaryExpr":
        return BinaryExpr(self, "LIKE", Literal(pattern))

    def ilike(self, pattern: str) -> "BinaryExpr":
        return BinaryExpr(self, "ILIKE", Literal(pattern))

    def contains(self, substring: str) -> "FunctionExpr":
        return FunctionExpr("CONTAINS", [self, Literal(substring)])

    def startswith(self, prefix: str) -> "BinaryExpr":
        return BinaryExpr(self, "LIKE", Literal(f"{prefix}%"))

    def endswith(self, suffix: str) -> "BinaryExpr":
        return BinaryExpr(self, "LIKE", Literal(f"%{suffix}"))

    def upper(self) -> "FunctionExpr":
        return FunctionExpr("UPPER", [self])

    def lower(self) -> "FunctionExpr":
        return FunctionExpr("LOWER", [self])

    def trim(self) -> "FunctionExpr":
        return FunctionExpr("TRIM", [self])

    def cast(self, sql_type: str) -> "CastExpr":
        return CastExpr(self, sql_type)

    def in_(self, values: list[Any]) -> "InExpr":
        return InExpr(self, [_wrap(v) for v in values])

    def between(self, low: Any, high: Any) -> "BetweenExpr":
        return BetweenExpr(self, _wrap(low), _wrap(high))

    # Aggregation
    def sum(self) -> "FunctionExpr":
        return FunctionExpr("SUM", [self])

    def avg(self) -> "FunctionExpr":
        return FunctionExpr("AVG", [self])

    def min(self) -> "FunctionExpr":
        return FunctionExpr("MIN", [self])

    def max(self) -> "FunctionExpr":
        return FunctionExpr("MAX", [self])

    def count(self) -> "FunctionExpr":
        return FunctionExpr("COUNT", [self])

    def count_distinct(self) -> "FunctionExpr":
        return FunctionExpr("COUNT_DISTINCT", [self])

    # Sorting helpers
    def asc(self) -> "SortExpr":
        return SortExpr(self, descending=False)

    def desc(self) -> "SortExpr":
        return SortExpr(self, descending=True)

    def to_sql(self, dialect: str = "duckdb") -> str:
        """Compile this expression to a SQL string."""
        raise NotImplementedError(f"{type(self).__name__} must implement to_sql()")

    def __repr__(self) -> str:
        try:
            return f"{type(self).__name__}({self.to_sql()})"
        except Exception:
            return f"{type(self).__name__}(?)"


class Column(Expression):
    """A reference to a column by name."""

    def __init__(self, name: str, table: str | None = None) -> None:
        super().__init__()
        self.name = name
        self.table = table

    def _copy(self) -> "Column":
        c = Column(self.name, self.table)
        c._alias = self._alias
        return c

    def to_sql(self, dialect: str = "duckdb") -> str:
        if self.table:
            base = f'"{self.table}"."{self.name}"'
        else:
            base = f'"{self.name}"'
        if self._alias:
            return f"{base} AS \"{self._alias}\""
        return base


class Literal(Expression):
    """A literal scalar value."""

    def __init__(self, value: Any) -> None:
        super().__init__()
        self.value = value

    def _copy(self) -> "Literal":
        l = Literal(self.value)
        l._alias = self._alias
        return l

    def to_sql(self, dialect: str = "duckdb") -> str:
        sql = _literal_to_sql(self.value)
        if self._alias:
            return f"{sql} AS \"{self._alias}\""
        return sql


class BinaryExpr(Expression):
    """A binary operation: left OP right."""

    def __init__(self, left: Expression, op: str, right: Expression) -> None:
        super().__init__()
        self.left = left
        self.op = op
        self.right = right

    def _copy(self) -> "BinaryExpr":
        e = BinaryExpr(self.left, self.op, self.right)
        e._alias = self._alias
        return e

    def to_sql(self, dialect: str = "duckdb") -> str:
        left_sql = self.left.to_sql(dialect)
        right_sql = self.right.to_sql(dialect)
        base = f"({left_sql} {self.op} {right_sql})"
        if self._alias:
            return f"{base} AS \"{self._alias}\""
        return base


class UnaryExpr(Expression):
    """A unary operation: OP expr."""

    def __init__(self, op: str, operand: Expression) -> None:
        super().__init__()
        self.op = op
        self.operand = operand

    def _copy(self) -> "UnaryExpr":
        e = UnaryExpr(self.op, self.operand)
        e._alias = self._alias
        return e

    def to_sql(self, dialect: str = "duckdb") -> str:
        base = f"({self.op} {self.operand.to_sql(dialect)})"
        if self._alias:
            return f"{base} AS \"{self._alias}\""
        return base


class FunctionExpr(Expression):
    """A function call or postfix operator."""

    def __init__(
        self,
        func_name: str,
        args: list[Expression],
        postfix: bool = False,
    ) -> None:
        super().__init__()
        self.func_name = func_name
        self.args = args
        self.postfix = postfix

    def _copy(self) -> "FunctionExpr":
        e = FunctionExpr(self.func_name, list(self.args), self.postfix)
        e._alias = self._alias
        return e

    def to_sql(self, dialect: str = "duckdb") -> str:
        if self.postfix:
            # e.g. "col IS NULL"
            arg_sql = self.args[0].to_sql(dialect) if self.args else ""
            base = f"({arg_sql} {self.func_name})"
        elif self.func_name == "COUNT_DISTINCT":
            arg_sql = self.args[0].to_sql(dialect) if self.args else "*"
            base = f"COUNT(DISTINCT {arg_sql})"
        else:
            args_sql = ", ".join(a.to_sql(dialect) for a in self.args)
            base = f"{self.func_name}({args_sql})"
        if self._alias:
            return f"{base} AS \"{self._alias}\""
        return base


class CastExpr(Expression):
    """A CAST expression."""

    def __init__(self, expr: Expression, sql_type: str) -> None:
        super().__init__()
        self.expr = expr
        self.sql_type = sql_type

    def _copy(self) -> "CastExpr":
        e = CastExpr(self.expr, self.sql_type)
        e._alias = self._alias
        return e

    def to_sql(self, dialect: str = "duckdb") -> str:
        base = f"CAST({self.expr.to_sql(dialect)} AS {self.sql_type})"
        if self._alias:
            return f"{base} AS \"{self._alias}\""
        return base


class InExpr(Expression):
    """A value IN (...) expression."""

    def __init__(self, expr: Expression, values: list[Expression]) -> None:
        super().__init__()
        self.expr = expr
        self.values = values

    def _copy(self) -> "InExpr":
        e = InExpr(self.expr, list(self.values))
        e._alias = self._alias
        return e

    def to_sql(self, dialect: str = "duckdb") -> str:
        vals = ", ".join(v.to_sql(dialect) for v in self.values)
        base = f"({self.expr.to_sql(dialect)} IN ({vals}))"
        if self._alias:
            return f"{base} AS \"{self._alias}\""
        return base


class BetweenExpr(Expression):
    """A BETWEEN expression."""

    def __init__(self, expr: Expression, low: Expression, high: Expression) -> None:
        super().__init__()
        self.expr = expr
        self.low = low
        self.high = high

    def _copy(self) -> "BetweenExpr":
        e = BetweenExpr(self.expr, self.low, self.high)
        e._alias = self._alias
        return e

    def to_sql(self, dialect: str = "duckdb") -> str:
        base = (
            f"({self.expr.to_sql(dialect)} BETWEEN "
            f"{self.low.to_sql(dialect)} AND {self.high.to_sql(dialect)})"
        )
        if self._alias:
            return f"{base} AS \"{self._alias}\""
        return base


class SortExpr:
    """A sort specification: expression + direction."""

    def __init__(self, expr: Expression, descending: bool = False) -> None:
        self.expr = expr
        self.descending = descending

    def to_sql(self, dialect: str = "duckdb") -> str:
        direction = "DESC" if self.descending else "ASC"
        return f"{self.expr.to_sql(dialect)} {direction}"


class StarExpr(Expression):
    """SELECT * expression."""

    def _copy(self) -> "StarExpr":
        return StarExpr()

    def to_sql(self, dialect: str = "duckdb") -> str:
        return "*"


# -------------------------------------------------------------------------
# Convenience constructors
# -------------------------------------------------------------------------


def col(name: str, table: str | None = None) -> Column:
    """Create a Column reference."""
    return Column(name, table)


def lit(value: Any) -> Literal:
    """Create a Literal value."""
    return Literal(value)


def star() -> StarExpr:
    """Create a SELECT * expression."""
    return StarExpr()


def _wrap(value: Any) -> Expression:
    """Wrap a Python value in an Expression if it isn't one already."""
    if isinstance(value, Expression):
        return value
    return Literal(value)


def _literal_to_sql(value: Any) -> str:
    """Convert a Python value to a SQL literal string."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    import datetime
    if isinstance(value, datetime.datetime):
        return f"TIMESTAMP '{value.isoformat()}'"
    if isinstance(value, datetime.date):
        return f"DATE '{value.isoformat()}'"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"
