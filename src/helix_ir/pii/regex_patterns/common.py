"""Common PII regex patterns (email, phone, URL, IP, credit card)."""

from __future__ import annotations

import re
from typing import Pattern

# Email: RFC-5321 simplified
EMAIL_PATTERN: Pattern[str] = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$",
    re.IGNORECASE,
)

# E.164 international phone (includes optional country code)
PHONE_PATTERN: Pattern[str] = re.compile(
    r"^\+?[\d\s\-\(\)]{7,20}$"
)

# Strict phone check (more digits)
PHONE_DIGITS_PATTERN: Pattern[str] = re.compile(
    r"^\+?1?\s*[\(\-]?\s*\d{3}\s*[\)\-]?\s*\d{3}\s*[\-]?\s*\d{4}$"
)

# URL
URL_PATTERN: Pattern[str] = re.compile(
    r"^https?://[^\s/$.?#].[^\s]*$",
    re.IGNORECASE,
)

# IPv4
IPV4_PATTERN: Pattern[str] = re.compile(
    r"^(\d{1,3}\.){3}\d{1,3}$"
)

# IPv6 (simplified)
IPV6_PATTERN: Pattern[str] = re.compile(
    r"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
    r"|^(([0-9a-fA-F]{1,4}:)*[0-9a-fA-F]{1,4})?::.*$"
)

# Credit card (Luhn-validated separately; pattern just checks structure)
CREDIT_CARD_PATTERN: Pattern[str] = re.compile(
    r"^(?:4[0-9]{12}(?:[0-9]{3})?"         # Visa
    r"|5[1-5][0-9]{14}"                      # MC
    r"|3[47][0-9]{13}"                       # Amex
    r"|3(?:0[0-5]|[68][0-9])[0-9]{11}"      # Diners
    r"|6(?:011|5[0-9]{2})[0-9]{12}"         # Discover
    r"|(?:2131|1800|35\d{3})\d{11})$"       # JCB
)

# UUID
UUID_PATTERN: Pattern[str] = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

PATTERNS: dict[str, Pattern[str]] = {
    "email": EMAIL_PATTERN,
    "phone": PHONE_PATTERN,
    "url": URL_PATTERN,
    "ip": IPV4_PATTERN,
    "credit_card": CREDIT_CARD_PATTERN,
    "uuid": UUID_PATTERN,
}
