"""helix_ir.infer — schema inference from document streams."""

from __future__ import annotations

from typing import Any, Iterable

from helix_ir.exceptions import EmptySourceError
from helix_ir.infer.merger import merge_observations
from helix_ir.infer.sampler import reservoir_sample
from helix_ir.infer.walker import walk_document
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


def infer(
    documents: Iterable[dict[str, Any]],
    name: str = "inferred",
    sample_size: int = 2000,
    seed: int | None = None,
    detect_pii: bool = True,
    pii_locale: str = "in",
    max_union_members: int = 4,
    fail_on_empty: bool = True,
) -> Schema:
    """Infer a Schema from an iterable of documents.

    Args:
        documents: Iterable of dicts representing rows/documents.
        name: Name for the resulting Schema.
        sample_size: Maximum number of documents to sample (Algorithm R).
        seed: Random seed for reproducible sampling.
        detect_pii: If True, annotate fields with PII classification.
        pii_locale: Locale string for PII detection ('in', 'us', 'eu', 'all').
        max_union_members: Max union members before falling back to JsonBlob.
        fail_on_empty: If True, raise EmptySourceError when input is empty.

    Returns:
        A Schema inferred from the document stream.

    Raises:
        EmptySourceError: If documents is empty and fail_on_empty is True.
    """
    # Reservoir-sample documents
    sample = reservoir_sample(documents, k=sample_size, seed=seed)

    if not sample:
        if fail_on_empty:
            raise EmptySourceError(
                "No documents provided. "
                "Pass fail_on_empty=False to return an empty schema."
            )
        return Schema(name=name, fields=())

    # Walk all sampled documents and accumulate observations + sample values
    all_observations: dict[str, list[HelixType]] = {}
    sample_values: dict[str, list[Any]] = {}

    for doc in sample:
        try:
            obs = walk_document(doc)
        except Exception:
            continue  # Skip malformed documents
        for path, types in obs.items():
            if path not in all_observations:
                all_observations[path] = []
            all_observations[path].extend(types)

    # Also record raw values for cardinality estimation
    for doc in sample:
        _collect_values(doc, "", sample_values)

    # Merge into a Schema
    schema = merge_observations(
        observations=all_observations,
        name=name,
        sample_values=sample_values,
    )

    # PII detection
    if detect_pii:
        from helix_ir.pii.classifier import detect_pii as _detect_pii
        schema = _detect_pii(schema, sample_values=sample_values, locale=pii_locale)

    return schema


def _collect_values(
    doc: Any,
    path: str,
    sample_values: dict[str, list[Any]],
    max_per_path: int = 500,
) -> None:
    """Recursively collect raw scalar values by path."""
    if isinstance(doc, dict):
        for key, val in doc.items():
            child_path = f"{path}.{key}" if path else key
            _collect_values(val, child_path, sample_values, max_per_path)
    elif isinstance(doc, list):
        elem_path = f"{path}[]" if path else "[]"
        for item in doc:
            _collect_values(item, elem_path, sample_values, max_per_path)
    else:
        if not path:
            return
        if path not in sample_values:
            sample_values[path] = []
        lst = sample_values[path]
        if len(lst) < max_per_path:
            lst.append(doc)
