"""helix_ir — schema inference, normalization, transformation, and DDL generation.

Quick start::

    from helix_ir import infer, normalize, compile_ddl, col, lit, diff

    schema = infer(documents)
    plan = normalize(schema)
    ddl = compile_ddl(plan, dialect='duckdb')
    print(ddl)
"""

from helix_ir.ddl import compile_ddl
from helix_ir.diff import diff
from helix_ir.infer import infer
from helix_ir.normalize import normalize
from helix_ir.transform.expression import col, lit

__version__ = "0.1.0"

__all__ = [
    "infer",
    "normalize",
    "compile_ddl",
    "col",
    "lit",
    "diff",
    "__version__",
]
