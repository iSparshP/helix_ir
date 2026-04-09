"""European PII regex patterns: VAT, IBAN, SWIFT."""

from __future__ import annotations

import re
from typing import Pattern

# IBAN: up to 34 alphanumeric chars, starts with 2-letter country code
IBAN_PATTERN: Pattern[str] = re.compile(
    r"^[A-Z]{2}\d{2}[A-Z0-9]{4,30}$",
    re.IGNORECASE,
)

# SWIFT/BIC: 8 or 11 alphanumeric chars
SWIFT_PATTERN: Pattern[str] = re.compile(
    r"^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$",
    re.IGNORECASE,
)

# EU VAT (covers most EU formats)
# Format varies per country; simplified generic pattern
EU_VAT_PATTERN: Pattern[str] = re.compile(
    r"^(AT|BE|BG|CY|CZ|DE|DK|EE|EL|ES|FI|FR|GB|HR|HU|IE|IT|LT|LU|LV|MT|NL|PL|PT|RO|SE|SI|SK)"
    r"[0-9A-Z+\*\.]{2,13}$",
    re.IGNORECASE,
)

# UK National Insurance Number
UK_NIN_PATTERN: Pattern[str] = re.compile(
    r"^(?!BG|GB|NK|KN|TN|NT|ZZ)[A-CEGHJ-PR-TW-Z][A-CEGHJ-NPR-TW-Z]\d{6}[A-D]$",
    re.IGNORECASE,
)

PATTERNS: dict[str, Pattern[str]] = {
    "iban": IBAN_PATTERN,
    "swift": SWIFT_PATTERN,
    "vat": EU_VAT_PATTERN,
    "uk_nin": UK_NIN_PATTERN,
}
