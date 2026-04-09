"""Indian PII regex patterns: PAN, Aadhaar, GSTIN."""

from __future__ import annotations

import re
from typing import Pattern

# PAN: 5 letters + 4 digits + 1 letter
PAN_PATTERN: Pattern[str] = re.compile(
    r"^[A-Z]{5}[0-9]{4}[A-Z]$",
    re.IGNORECASE,
)

# Aadhaar: 12 digits (not starting with 0 or 1)
AADHAAR_PATTERN: Pattern[str] = re.compile(
    r"^[2-9]\d{11}$"
)

# GSTIN: 15-character alphanumeric
# Format: 2-digit state + 10-char PAN + 1 entity + 1 Z + 1 checksum
GSTIN_PATTERN: Pattern[str] = re.compile(
    r"^[0-3][0-9][A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$",
    re.IGNORECASE,
)

# Indian Mobile: 10 digits starting with 6-9
INDIA_MOBILE_PATTERN: Pattern[str] = re.compile(
    r"^[6-9]\d{9}$"
)

# IFSC Code: 4 letters + 0 + 6 alphanumeric
IFSC_PATTERN: Pattern[str] = re.compile(
    r"^[A-Z]{4}0[A-Z0-9]{6}$",
    re.IGNORECASE,
)

PATTERNS: dict[str, Pattern[str]] = {
    "pan": PAN_PATTERN,
    "aadhaar": AADHAAR_PATTERN,
    "gstin": GSTIN_PATTERN,
    "india_mobile": INDIA_MOBILE_PATTERN,
    "ifsc": IFSC_PATTERN,
}
