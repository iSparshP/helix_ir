"""helix_ir.types — type system for Helix IR."""

from helix_ir.types.arrow_interop import arrow_to_helix_type, helix_type_to_arrow
from helix_ir.types.core import HelixType
from helix_ir.types.lattice import join, meet, subsumes
from helix_ir.types.semantic import (
    ALL_SEMANTICS,
    JSONBLOB_TYPE,
    SEMANTIC_EMAIL,
    SEMANTIC_ENUM,
    SEMANTIC_PHONE,
    SEMANTIC_URL,
)

__all__ = [
    "HelixType",
    "join",
    "meet",
    "subsumes",
    "helix_type_to_arrow",
    "arrow_to_helix_type",
    "SEMANTIC_EMAIL",
    "SEMANTIC_ENUM",
    "SEMANTIC_PHONE",
    "SEMANTIC_URL",
    "JSONBLOB_TYPE",
    "ALL_SEMANTICS",
]
