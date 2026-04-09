"""Aggregation and scalar function helpers."""

from __future__ import annotations

from helix_ir.transform.expression import Expression, FunctionExpr, col


def sum_(expr: Expression | str) -> FunctionExpr:
    """SUM aggregate."""
    e = col(expr) if isinstance(expr, str) else expr
    return FunctionExpr("SUM", [e])


def avg_(expr: Expression | str) -> FunctionExpr:
    """AVG aggregate."""
    e = col(expr) if isinstance(expr, str) else expr
    return FunctionExpr("AVG", [e])


def min_(expr: Expression | str) -> FunctionExpr:
    """MIN aggregate."""
    e = col(expr) if isinstance(expr, str) else expr
    return FunctionExpr("MIN", [e])


def max_(expr: Expression | str) -> FunctionExpr:
    """MAX aggregate."""
    e = col(expr) if isinstance(expr, str) else expr
    return FunctionExpr("MAX", [e])


def count_(expr: Expression | str = "*") -> FunctionExpr:
    """COUNT aggregate."""
    if isinstance(expr, str):
        if expr == "*":
            from helix_ir.transform.expression import StarExpr
            return FunctionExpr("COUNT", [StarExpr()])
        e = col(expr)
    else:
        e = expr
    return FunctionExpr("COUNT", [e])


def count_distinct_(expr: Expression | str) -> FunctionExpr:
    """COUNT(DISTINCT ...) aggregate."""
    e = col(expr) if isinstance(expr, str) else expr
    return FunctionExpr("COUNT_DISTINCT", [e])


def coalesce(*exprs: Expression | str) -> FunctionExpr:
    """COALESCE function."""
    args = [col(e) if isinstance(e, str) else e for e in exprs]
    return FunctionExpr("COALESCE", args)


def if_(condition: Expression, then: Expression, else_: Expression) -> FunctionExpr:
    """IIF/IF function."""
    return FunctionExpr("IIF", [condition, then, else_])


def date_trunc(unit: str, expr: Expression | str) -> FunctionExpr:
    """DATE_TRUNC function."""
    from helix_ir.transform.expression import Literal
    e = col(expr) if isinstance(expr, str) else expr
    return FunctionExpr("DATE_TRUNC", [Literal(unit), e])


def to_date(expr: Expression | str) -> FunctionExpr:
    """CAST to DATE."""
    e = col(expr) if isinstance(expr, str) else expr
    from helix_ir.transform.expression import CastExpr
    return CastExpr(e, "DATE")  # type: ignore[return-value]


def length(expr: Expression | str) -> FunctionExpr:
    """LENGTH/LEN string function."""
    e = col(expr) if isinstance(expr, str) else expr
    return FunctionExpr("LENGTH", [e])


def concat(*exprs: Expression | str) -> FunctionExpr:
    """CONCAT function."""
    args = [col(e) if isinstance(e, str) else e for e in exprs]
    return FunctionExpr("CONCAT", args)
