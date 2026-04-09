"""PII detection: annotate a Schema with PII classes."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from helix_ir.pii.heuristics import detect_pii_from_field_name
from helix_ir.pii.regex_patterns import get_all_patterns, get_patterns
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


def detect_pii(
    schema: Schema,
    sample_values: dict[str, list[Any]] | None = None,
    locale: str = "in",
    layers: list[str] | None = None,
    confidence_threshold: float = 0.8,
) -> Schema:
    """Annotate schema fields with PII classes.

    Args:
        schema: The schema to annotate.
        sample_values: Dict mapping path strings to sample values for regex matching.
        locale: Locale for PII patterns ('in', 'us', 'eu', 'all').
        layers: Detection layers to use. Default: ['name', 'regex'].
        confidence_threshold: Minimum fraction of matched values to assign PII class.

    Returns:
        A new Schema with pii_class annotations on relevant fields.
    """
    if layers is None:
        layers = ["name", "regex"]

    if locale == "all":
        patterns = get_all_patterns()
    else:
        patterns = get_patterns(locale)

    new_fields: list[tuple[str, HelixType]] = []
    for fname, ht in schema.fields:
        pii_class = ht.pii_class  # preserve existing

        # Layer 1: field name heuristics
        if pii_class is None and "name" in layers:
            pii_class = detect_pii_from_field_name(fname)

        # Layer 2: regex matching on sample values
        if pii_class is None and "regex" in layers and sample_values:
            values = sample_values.get(fname, [])
            if values:
                pii_class = _detect_pii_from_values(values, patterns, confidence_threshold)

        new_fields.append((fname, ht.evolve(pii_class=pii_class)))

    return Schema(name=schema.name, fields=tuple(new_fields))


def _detect_pii_from_values(
    values: list[Any],
    patterns: dict,
    confidence_threshold: float,
) -> str | None:
    """Check sample values against regex patterns and return the best PII class."""
    import re

    str_values = [str(v) for v in values if v is not None and isinstance(v, str)]
    if not str_values:
        return None

    # Count matches per PII class
    counts: dict[str, int] = {}
    for pii_class, pattern in patterns.items():
        matched = sum(1 for v in str_values if pattern.fullmatch(v))
        if matched > 0:
            counts[pii_class] = matched

    if not counts:
        return None

    # Find best match
    best_class = max(counts, key=lambda c: counts[c])
    ratio = counts[best_class] / len(str_values)
    if ratio >= confidence_threshold:
        return best_class

    return None
