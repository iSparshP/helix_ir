"""Semantic type constants and helpers."""

from __future__ import annotations

# Semantic type name constants
SEMANTIC_EMAIL = "email"
SEMANTIC_PHONE = "phone"
SEMANTIC_URL = "url"
SEMANTIC_IP = "ip"
SEMANTIC_UUID = "uuid"
SEMANTIC_ENUM = "enum"
SEMANTIC_DATE_STRING = "date_string"
SEMANTIC_DATETIME_STRING = "datetime_string"
JSONBLOB_TYPE = "json_blob"
SEMANTIC_PAN = "pan"
SEMANTIC_AADHAAR = "aadhaar"
SEMANTIC_GSTIN = "gstin"
SEMANTIC_SSN = "ssn"
SEMANTIC_CREDIT_CARD = "credit_card"
SEMANTIC_IBAN = "iban"
SEMANTIC_VAT = "vat"
SEMANTIC_SWIFT = "swift"

# All known semantic types
ALL_SEMANTICS: frozenset[str] = frozenset(
    [
        SEMANTIC_EMAIL,
        SEMANTIC_PHONE,
        SEMANTIC_URL,
        SEMANTIC_IP,
        SEMANTIC_UUID,
        SEMANTIC_ENUM,
        SEMANTIC_DATE_STRING,
        SEMANTIC_DATETIME_STRING,
        JSONBLOB_TYPE,
        SEMANTIC_PAN,
        SEMANTIC_AADHAAR,
        SEMANTIC_GSTIN,
        SEMANTIC_SSN,
        SEMANTIC_CREDIT_CARD,
        SEMANTIC_IBAN,
        SEMANTIC_VAT,
        SEMANTIC_SWIFT,
    ]
)
