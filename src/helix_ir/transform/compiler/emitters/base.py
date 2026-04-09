"""Base SQL emitter — dispatches on LogicalPlan node types."""

from __future__ import annotations

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
from helix_ir.transform.expression import (
    BetweenExpr,
    BinaryExpr,
    CastExpr,
    Column,
    Expression,
    FunctionExpr,
    InExpr,
    Literal,
    SortExpr,
    StarExpr,
    UnaryExpr,
)


class BaseEmitter:
    """Base SQL emitter with dispatch on plan/expression types."""

    DIALECT: str = "base"

    def emit(self, plan: object) -> str:  # noqa: C901
        """Emit SQL for a logical plan node."""
        if isinstance(plan, Scan):
            return self.emit_scan(plan)
        if isinstance(plan, Filter):
            return self.emit_filter(plan)
        if isinstance(plan, Project):
            return self.emit_project(plan)
        if isinstance(plan, Join):
            return self.emit_join(plan)
        if isinstance(plan, Aggregate):
            return self.emit_aggregate(plan)
        if isinstance(plan, Sort):
            return self.emit_sort(plan)
        if isinstance(plan, Limit):
            return self.emit_limit(plan)
        if isinstance(plan, Union):
            return self.emit_union(plan)
        if isinstance(plan, RawSQL):
            return f"({plan.sql}) AS {self.quote(plan.alias)}"
        raise ValueError(f"Unknown plan node type: {type(plan).__name__}")

    def emit_scan(self, plan: Scan) -> str:
        name = self.quote(plan.source_name)
        if plan.alias:
            return f"{name} AS {self.quote(plan.alias)}"
        return name

    def emit_filter(self, plan: Filter) -> str:
        inner = self.emit(plan.input)
        predicate = self.emit_expression(plan.predicate)
        return f"SELECT * FROM {inner} WHERE {predicate}"

    def emit_project(self, plan: Project) -> str:
        inner = self.emit(plan.input)
        cols = ", ".join(self.emit_expression(c) for c in plan.columns)
        return f"SELECT {cols} FROM ({inner}) AS _projected"

    def emit_join(self, plan: Join) -> str:
        left = self.emit(plan.left)
        right = self.emit(plan.right)
        on = self.emit_expression(plan.on)
        how = plan.how.upper()
        return (
            f"SELECT * FROM ({left}) AS _left\n"
            f"{how} JOIN ({right}) AS _right\n"
            f"ON {on}"
        )

    def emit_aggregate(self, plan: Aggregate) -> str:
        inner = self.emit(plan.input)
        agg_cols = ", ".join(self.emit_expression(a) for a in plan.agg)
        if plan.group_by:
            group_cols = ", ".join(self.emit_expression(g) for g in plan.group_by)
            all_cols = group_cols + (f", {agg_cols}" if agg_cols else "")
            return (
                f"SELECT {all_cols}\n"
                f"FROM ({inner}) AS _agg\n"
                f"GROUP BY {group_cols}"
            )
        return f"SELECT {agg_cols} FROM ({inner}) AS _agg"

    def emit_sort(self, plan: Sort) -> str:
        inner = self.emit(plan.input)
        order_parts: list[str] = []
        for i, expr in enumerate(plan.by):
            if isinstance(expr, SortExpr):
                order_parts.append(expr.to_sql(self.DIALECT))
            else:
                direction = "DESC" if (i < len(plan.desc) and plan.desc[i]) else "ASC"
                order_parts.append(f"{self.emit_expression(expr)} {direction}")
        order_by = ", ".join(order_parts)
        return f"SELECT * FROM ({inner}) AS _sorted\nORDER BY {order_by}"

    def emit_limit(self, plan: Limit) -> str:
        inner = self.emit(plan.input)
        stmt = f"SELECT * FROM ({inner}) AS _limited\nLIMIT {plan.n}"
        if plan.offset:
            stmt += f" OFFSET {plan.offset}"
        return stmt

    def emit_union(self, plan: Union) -> str:
        left = self.emit(plan.left)
        right = self.emit(plan.right)
        union_kw = "UNION ALL" if plan.all else "UNION"
        return f"({left})\n{union_kw}\n({right})"

    def emit_expression(self, expr: object) -> str:  # noqa: C901
        """Emit SQL for an expression."""
        if isinstance(expr, SortExpr):
            return expr.to_sql(self.DIALECT)
        if isinstance(expr, Expression):
            return expr.to_sql(self.DIALECT)
        # Fallback: str conversion
        return str(expr)

    def quote(self, name: str) -> str:
        """Quote a SQL identifier."""
        return f'"{name}"'
