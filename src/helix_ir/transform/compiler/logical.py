"""Logical plan nodes for the Helix IR query compiler."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from helix_ir.schema.schema import Schema
    from helix_ir.transform.expression import Expression


# Base type for all logical plan nodes
LogicalPlan = (
    "Scan | Filter | Project | Join | Aggregate | Sort | Limit | Union | RawSQL"
)


@dataclass
class Scan:
    """Scan a source table/relation."""
    source_name: str
    schema: "Schema | None" = None
    alias: str | None = None


@dataclass
class Filter:
    """Filter rows by a predicate."""
    input: "object"  # LogicalPlan
    predicate: "Expression"


@dataclass
class Project:
    """Project (select) specific columns."""
    input: "object"  # LogicalPlan
    columns: "list[Expression]"


@dataclass
class Join:
    """Join two relations."""
    left: "object"  # LogicalPlan
    right: "object"  # LogicalPlan
    on: "Expression"
    how: str = "inner"  # 'inner', 'left', 'right', 'full', 'cross'


@dataclass
class Aggregate:
    """Group by and aggregate."""
    input: "object"  # LogicalPlan
    group_by: "list[Expression]"
    agg: "list[Expression]"


@dataclass
class Sort:
    """Sort rows."""
    input: "object"  # LogicalPlan
    by: "list[Expression | object]"  # Expression or SortExpr
    desc: "list[bool]" = field(default_factory=list)


@dataclass
class Limit:
    """Limit the number of rows."""
    input: "object"  # LogicalPlan
    n: int
    offset: int = 0


@dataclass
class Union:
    """Union two relations."""
    left: "object"  # LogicalPlan
    right: "object"  # LogicalPlan
    by_name: bool = False
    all: bool = True


@dataclass
class RawSQL:
    """A raw SQL subquery (escape hatch)."""
    sql: str
    alias: str = "raw"
