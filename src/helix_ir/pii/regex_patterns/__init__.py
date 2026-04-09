"""PII regex pattern collections by locale."""

from __future__ import annotations

import re
from typing import Pattern

from helix_ir.pii.regex_patterns.common import PATTERNS as COMMON_PATTERNS
from helix_ir.pii.regex_patterns.europe import PATTERNS as EU_PATTERNS
from helix_ir.pii.regex_patterns.india import PATTERNS as IN_PATTERNS
from helix_ir.pii.regex_patterns.usa import PATTERNS as USA_PATTERNS

LOCALE_PATTERNS: dict[str, dict[str, Pattern[str]]] = {
    "common": COMMON_PATTERNS,
    "in": IN_PATTERNS,
    "us": USA_PATTERNS,
    "eu": EU_PATTERNS,
}


def get_patterns(locale: str = "in") -> dict[str, Pattern[str]]:
    """Return combined patterns for a locale (always includes common patterns)."""
    combined: dict[str, Pattern[str]] = dict(COMMON_PATTERNS)
    locale_specific = LOCALE_PATTERNS.get(locale, {})
    combined.update(locale_specific)
    return combined


def get_all_patterns() -> dict[str, Pattern[str]]:
    """Return all patterns from all locales."""
    combined: dict[str, Pattern[str]] = {}
    for patterns in LOCALE_PATTERNS.values():
        combined.update(patterns)
    return combined


__all__ = [
    "LOCALE_PATTERNS",
    "get_patterns",
    "get_all_patterns",
    "COMMON_PATTERNS",
    "IN_PATTERNS",
    "USA_PATTERNS",
    "EU_PATTERNS",
]
