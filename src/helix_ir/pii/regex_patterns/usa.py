"""US PII regex patterns: SSN, ZIP, EIN."""

from __future__ import annotations

import re
from typing import Pattern

# SSN: 9 digits optionally separated by dashes
SSN_PATTERN: Pattern[str] = re.compile(
    r"^(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}$"
    r"|^(?!000|666|9\d{2})\d{3}(?!00)\d{2}(?!0000)\d{4}$"
)

# US ZIP code
ZIP_PATTERN: Pattern[str] = re.compile(
    r"^\d{5}(-\d{4})?$"
)

# EIN (Employer Identification Number)
EIN_PATTERN: Pattern[str] = re.compile(
    r"^\d{2}-\d{7}$"
)

PATTERNS: dict[str, Pattern[str]] = {
    "ssn": SSN_PATTERN,
    "us_zip": ZIP_PATTERN,
    "ein": EIN_PATTERN,
}
