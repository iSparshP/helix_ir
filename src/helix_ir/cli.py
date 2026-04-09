"""Helix IR command-line interface."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.text import Text

app = typer.Typer(
    name="helix-ir",
    help="Helix IR — Schema inference, normalization, DDL generation.",
    add_completion=False,
)
console = Console()
err_console = Console(stderr=True)


# -------------------------------------------------------------------------
# infer command
# -------------------------------------------------------------------------


@app.command("infer")
def cmd_infer(
    input_file: Path = typer.Argument(..., help="Path to JSON/NDJSON/Parquet file"),
    name: str = typer.Option("inferred", "--name", "-n", help="Schema name"),
    sample_size: int = typer.Option(2000, "--sample-size", "-s"),
    seed: Optional[int] = typer.Option(None, "--seed"),
    no_pii: bool = typer.Option(False, "--no-pii", help="Disable PII detection"),
    locale: str = typer.Option("in", "--locale", "-l", help="PII locale: in/us/eu/all"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Write schema JSON here"),
    dialect: str = typer.Option("duckdb", "--dialect", "-d", help="DDL dialect for preview"),
) -> None:
    """Infer a schema from a JSON/NDJSON/Parquet file."""
    from helix_ir.infer import infer
    from helix_ir.sources.json_source import JSONSource
    from helix_ir.sources.parquet_source import ParquetSource

    ext = input_file.suffix.lower()
    if ext == ".parquet":
        source = ParquetSource(str(input_file))
    else:
        source = JSONSource(str(input_file))

    console.print(f"[bold cyan]Inferring schema from[/] {input_file} ...")
    try:
        schema = infer(
            source.read(),
            name=name,
            sample_size=sample_size,
            seed=seed,
            detect_pii=not no_pii,
            pii_locale=locale,
        )
    except Exception as e:
        err_console.print(f"[bold red]Error:[/] {e}")
        raise typer.Exit(1)

    _print_schema_table(schema)

    if output:
        schema_json = schema.to_json()
        output.write_text(json.dumps(schema_json, indent=2))
        console.print(f"\n[green]Schema written to[/] {output}")


# -------------------------------------------------------------------------
# ddl command
# -------------------------------------------------------------------------


@app.command("ddl")
def cmd_ddl(
    schema_file: Path = typer.Argument(..., help="Path to schema JSON file"),
    dialect: str = typer.Option("duckdb", "--dialect", "-d"),
    output: Optional[Path] = typer.Option(None, "--output", "-o"),
    if_not_exists: bool = typer.Option(True, "--if-not-exists/--no-if-not-exists"),
) -> None:
    """Generate DDL SQL from a schema JSON file."""
    from helix_ir.ddl import DDLOptions, compile_ddl
    from helix_ir.schema.schema import Schema

    try:
        data = json.loads(schema_file.read_text())
        schema = Schema.from_json(data)
    except Exception as e:
        err_console.print(f"[bold red]Error loading schema:[/] {e}")
        raise typer.Exit(1)

    opts = DDLOptions(if_not_exists=if_not_exists)
    try:
        script = compile_ddl(schema, dialect=dialect, options=opts)
    except Exception as e:
        err_console.print(f"[bold red]DDL error:[/] {e}")
        raise typer.Exit(1)

    sql = script.to_sql()
    if output:
        output.write_text(sql)
        console.print(f"[green]DDL written to[/] {output}")
    else:
        console.print(sql)


# -------------------------------------------------------------------------
# normalize command
# -------------------------------------------------------------------------


@app.command("normalize")
def cmd_normalize(
    schema_file: Path = typer.Argument(..., help="Path to schema JSON file"),
    strategy: str = typer.Option("1nf", "--strategy", "-s", help="1nf/mongo/inline_small"),
    dialect: str = typer.Option("duckdb", "--dialect", "-d"),
    output: Optional[Path] = typer.Option(None, "--output", "-o"),
) -> None:
    """Normalize a schema and generate multi-table DDL."""
    from helix_ir.ddl import DDLOptions, compile_ddl
    from helix_ir.normalize import normalize
    from helix_ir.schema.schema import Schema

    try:
        data = json.loads(schema_file.read_text())
        schema = Schema.from_json(data)
    except Exception as e:
        err_console.print(f"[bold red]Error loading schema:[/] {e}")
        raise typer.Exit(1)

    plan = normalize(schema, strategy=strategy)
    console.print(f"[bold]Tables:[/] {', '.join(plan.table_names())}")
    console.print(f"[bold]Foreign keys:[/] {len(plan.foreign_keys)}")

    opts = DDLOptions()
    try:
        script = compile_ddl(plan, dialect=dialect, options=opts)
    except Exception as e:
        err_console.print(f"[bold red]DDL error:[/] {e}")
        raise typer.Exit(1)

    sql = script.to_sql()
    if output:
        output.write_text(sql)
        console.print(f"[green]DDL written to[/] {output}")
    else:
        console.print(sql)


# -------------------------------------------------------------------------
# diff command
# -------------------------------------------------------------------------


@app.command("diff")
def cmd_diff(
    old_schema: Path = typer.Argument(..., help="Path to old schema JSON"),
    new_schema: Path = typer.Argument(..., help="Path to new schema JSON"),
    output: Optional[Path] = typer.Option(None, "--output", "-o"),
    show_safe: bool = typer.Option(True, "--safe/--no-safe"),
) -> None:
    """Diff two schemas and display changes."""
    from helix_ir.diff import diff
    from helix_ir.schema.schema import Schema

    try:
        old = Schema.from_json(json.loads(old_schema.read_text()))
        new = Schema.from_json(json.loads(new_schema.read_text()))
    except Exception as e:
        err_console.print(f"[bold red]Error loading schemas:[/] {e}")
        raise typer.Exit(1)

    schema_diff = diff(old, new)

    if not schema_diff:
        console.print("[green]No changes detected.[/]")
        return

    tbl = Table(title=f"Schema Diff: {old.name} → {new.name}")
    tbl.add_column("Path", style="cyan")
    tbl.add_column("Kind")
    tbl.add_column("Severity")
    tbl.add_column("Description")

    severity_colors = {"safe": "green", "risky": "yellow", "breaking": "red"}

    for change in schema_diff.changes:
        if not show_safe and change.severity == "safe":
            continue
        color = severity_colors.get(change.severity, "white")
        tbl.add_row(
            str(change.path),
            change.kind,
            Text(change.severity.upper(), style=f"bold {color}"),
            change.description,
        )

    console.print(tbl)

    summary = schema_diff.summary()
    console.print(
        f"[green]{summary['safe']} safe[/], "
        f"[yellow]{summary['risky']} risky[/], "
        f"[red]{summary['breaking']} breaking[/]"
    )

    if schema_diff.has_breaking_changes:
        raise typer.Exit(1)


# -------------------------------------------------------------------------
# lineage command
# -------------------------------------------------------------------------


@app.command("lineage")
def cmd_lineage(
    schema_file: Path = typer.Argument(..., help="Path to schema JSON"),
    format: str = typer.Option("dot", "--format", "-f", help="dot/openlineage"),
    output: Optional[Path] = typer.Option(None, "--output", "-o"),
    strategy: str = typer.Option("1nf", "--strategy", "-s"),
) -> None:
    """Generate lineage graph from schema normalization."""
    from helix_ir.normalize import normalize
    from helix_ir.schema.schema import Schema

    try:
        data = json.loads(schema_file.read_text())
        schema = Schema.from_json(data)
    except Exception as e:
        err_console.print(f"[bold red]Error:[/] {e}")
        raise typer.Exit(1)

    plan = normalize(schema, strategy=strategy)

    if format == "dot":
        result = plan.lineage.to_dot()
    elif format == "openlineage":
        result = json.dumps(plan.lineage.to_openlineage(), indent=2)
    else:
        err_console.print(f"[bold red]Unknown format:[/] {format}")
        raise typer.Exit(1)

    if output:
        output.write_text(result)
        console.print(f"[green]Lineage written to[/] {output}")
    else:
        console.print(result)


# -------------------------------------------------------------------------
# test command
# -------------------------------------------------------------------------


@app.command("test")
def cmd_test(
    schema_file: Path = typer.Argument(..., help="Path to schema JSON"),
    output: Optional[Path] = typer.Option(None, "--output", "-o"),
    sensitivity: float = typer.Option(1.5, "--sensitivity"),
) -> None:
    """Generate data quality tests from a schema."""
    from helix_ir.schema.schema import Schema
    from helix_ir.test.generator import generate_tests

    try:
        data = json.loads(schema_file.read_text())
        schema = Schema.from_json(data)
    except Exception as e:
        err_console.print(f"[bold red]Error:[/] {e}")
        raise typer.Exit(1)

    tests = generate_tests(schema, sensitivity=sensitivity)
    console.print(f"[bold]Generated {len(tests)} tests[/]")

    tbl = Table(title="Generated Tests")
    tbl.add_column("Name", style="cyan")
    tbl.add_column("Kind")
    tbl.add_column("Severity")
    tbl.add_column("Description")

    for test in tests:
        tbl.add_row(test.name, test.kind, test.severity, test.description)

    console.print(tbl)

    if output:
        test_data = [
            {
                "name": t.name,
                "path": str(t.path),
                "kind": t.kind,
                "severity": t.severity,
                "description": t.description,
                "sql_template": t.sql_template,
                "metadata": t.metadata,
            }
            for t in tests
        ]
        output.write_text(json.dumps(test_data, indent=2))
        console.print(f"[green]Tests written to[/] {output}")


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------


def _print_schema_table(schema: "Schema") -> None:
    """Print a schema as a rich table."""
    tbl = Table(title=f"Schema: {schema.name}")
    tbl.add_column("Field", style="cyan")
    tbl.add_column("Type")
    tbl.add_column("Null%")
    tbl.add_column("Semantic")
    tbl.add_column("PII")
    tbl.add_column("Cardinality")

    for fname, ht in schema.fields:
        tbl.add_row(
            fname,
            str(ht.arrow_type),
            f"{ht.null_ratio:.1%}",
            ht.semantic or "",
            Text(ht.pii_class or "", style="bold red") if ht.pii_class else "",
            str(ht.cardinality_estimate) if ht.cardinality_estimate else "",
        )

    console.print(tbl)


if __name__ == "__main__":
    app()
