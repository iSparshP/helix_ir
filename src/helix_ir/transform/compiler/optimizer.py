"""Logical plan optimizer: predicate pushdown, projection pushdown."""

from __future__ import annotations

from helix_ir.transform.compiler.logical import (
    Aggregate,
    Filter,
    Join,
    Limit,
    LogicalPlan,
    Project,
    Scan,
    Sort,
    Union,
    RawSQL,
)
from helix_ir.transform.expression import Expression


def optimize(plan: object) -> object:
    """Apply all optimization passes to a logical plan."""
    plan = predicate_pushdown(plan)
    plan = projection_pushdown(plan)
    return plan


def predicate_pushdown(plan: object) -> object:  # noqa: C901
    """Push filters as close to scans as possible."""
    if isinstance(plan, Filter):
        child = predicate_pushdown(plan.input)

        # Push through Project if possible
        if isinstance(child, Project):
            # Try to push the filter below the project
            new_filter = Filter(input=predicate_pushdown(child.input), predicate=plan.predicate)
            return Project(input=new_filter, columns=child.columns)

        # Push through Sort
        if isinstance(child, Sort):
            new_filter = Filter(input=predicate_pushdown(child.input), predicate=plan.predicate)
            return Sort(input=new_filter, by=child.by, desc=child.desc)

        # Push through Limit — can't push past limit safely, keep above
        if isinstance(child, Limit):
            return Filter(input=child, predicate=plan.predicate)

        # Push into Join (simplified — only push to left side)
        if isinstance(child, Join):
            return Filter(
                input=Join(
                    left=predicate_pushdown(child.left),
                    right=predicate_pushdown(child.right),
                    on=child.on,
                    how=child.how,
                ),
                predicate=plan.predicate,
            )

        return Filter(input=child, predicate=plan.predicate)

    elif isinstance(plan, Project):
        return Project(
            input=predicate_pushdown(plan.input),
            columns=plan.columns,
        )

    elif isinstance(plan, Aggregate):
        return Aggregate(
            input=predicate_pushdown(plan.input),
            group_by=plan.group_by,
            agg=plan.agg,
        )

    elif isinstance(plan, Sort):
        return Sort(
            input=predicate_pushdown(plan.input),
            by=plan.by,
            desc=plan.desc,
        )

    elif isinstance(plan, Limit):
        return Limit(
            input=predicate_pushdown(plan.input),
            n=plan.n,
            offset=plan.offset,
        )

    elif isinstance(plan, Join):
        return Join(
            left=predicate_pushdown(plan.left),
            right=predicate_pushdown(plan.right),
            on=plan.on,
            how=plan.how,
        )

    elif isinstance(plan, Union):
        return Union(
            left=predicate_pushdown(plan.left),
            right=predicate_pushdown(plan.right),
            by_name=plan.by_name,
            all=plan.all,
        )

    # Scan, RawSQL: no-op
    return plan


def projection_pushdown(plan: object) -> object:  # noqa: C901
    """Push projections closer to scans to reduce column loading."""
    if isinstance(plan, Project):
        child = projection_pushdown(plan.input)
        # If child is also a project, collapse them
        if isinstance(child, Project):
            # Use the outer projection (plan.columns override child.columns)
            return Project(input=child.input, columns=plan.columns)
        return Project(input=child, columns=plan.columns)

    elif isinstance(plan, Filter):
        return Filter(
            input=projection_pushdown(plan.input),
            predicate=plan.predicate,
        )

    elif isinstance(plan, Aggregate):
        return Aggregate(
            input=projection_pushdown(plan.input),
            group_by=plan.group_by,
            agg=plan.agg,
        )

    elif isinstance(plan, Sort):
        return Sort(
            input=projection_pushdown(plan.input),
            by=plan.by,
            desc=plan.desc,
        )

    elif isinstance(plan, Limit):
        return Limit(
            input=projection_pushdown(plan.input),
            n=plan.n,
            offset=plan.offset,
        )

    elif isinstance(plan, Join):
        return Join(
            left=projection_pushdown(plan.left),
            right=projection_pushdown(plan.right),
            on=plan.on,
            how=plan.how,
        )

    elif isinstance(plan, Union):
        return Union(
            left=projection_pushdown(plan.left),
            right=projection_pushdown(plan.right),
            by_name=plan.by_name,
            all=plan.all,
        )

    return plan
