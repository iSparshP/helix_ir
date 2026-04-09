"""Helix IR exception hierarchy."""


class HelixError(Exception):
    """Base exception for all helix_ir errors."""


class EmptySourceError(HelixError):
    """Raised when a data source yields no documents."""


class InferenceError(HelixError):
    """Raised when schema inference fails."""


class CyclicReferenceError(HelixError):
    """Raised when a cyclic reference is detected during document walking."""


class PathNotFoundError(HelixError):
    """Raised when a path does not exist in a schema."""


class TypeCheckError(HelixError):
    """Raised when a type check fails."""


class NormalizationError(HelixError):
    """Raised when normalization fails."""


class DDLCompilationError(HelixError):
    """Raised when DDL compilation fails."""
