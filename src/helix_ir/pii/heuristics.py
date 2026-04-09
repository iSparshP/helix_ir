"""Field name heuristics for PII classification."""

from __future__ import annotations

import re

# Map of regex pattern → pii_class
# Patterns are matched case-insensitively against field names (last component).
_FIELD_HEURISTICS: list[tuple[re.Pattern[str], str]] = [
    # IP address — must come BEFORE address to take priority for ip_address
    (re.compile(r"(^|_)ip_?addr(ess)?(_|$)|(^|_)ip(_|$)|(^|_)client_?ip(_|$)|(^|_)remote_?addr(_|$)", re.IGNORECASE), "ip"),
    # Email — match if 'email' or 'mail' appears as a whole word or at word boundaries
    (re.compile(r"(^|_)email(_|$)|(^|_)mail(_|$)|(^|_)email_addr(ess)?(_|$)", re.IGNORECASE), "email"),
    # Phone
    (re.compile(r"(^|_)phone(_|$)|(^|_)mobile(_|$)|(^|_)cell(_|$)|(^|_)tel(ephone)?(_|$)|(^|_)contact_no(_|$)", re.IGNORECASE), "phone"),
    # Full name
    (re.compile(r"(^|_)(full_?)?name(_|$)|(^|_)first_?name(_|$)|(^|_)last_?name(_|$)|(^|_)display_?name(_|$)", re.IGNORECASE), "name"),
    # Address
    (re.compile(r"(^|_)address(_|$)|(^|_)addr(_|$)|(^|_)street(_|$)|(^|_)city(_|$)|(^|_)state(_|$)|(^|_)pincode(_|$)|(^|_)zip(_|$)|(^|_)postal(_|$)", re.IGNORECASE), "address"),
    # Date of birth
    (re.compile(r"(^|_)dob(_|$)|(^|_)date_of_birth(_|$)|(^|_)birth_?date(_|$)|(^|_)birthday(_|$)", re.IGNORECASE), "dob"),
    # SSN
    (re.compile(r"(^|_)ssn(_|$)|(^|_)social_?security(_|$)", re.IGNORECASE), "ssn"),
    # PAN
    (re.compile(r"(^|_)pan(_|$)|(^|_)pan_?no(_|$)|(^|_)pan_?number(_|$)", re.IGNORECASE), "pan"),
    # Aadhaar
    (re.compile(r"(^|_)aadhaar(_|$)|(^|_)aadhar(_|$)|(^|_)uid(_|$)|(^|_)undp_id(_|$)", re.IGNORECASE), "aadhaar"),
    # GSTIN
    (re.compile(r"(^|_)gstin(_|$)|(^|_)gst_?no(_|$)|(^|_)gst_?number(_|$)", re.IGNORECASE), "gstin"),
    # URL
    (re.compile(r"(^|_)url(_|$)|(^|_)website(_|$)|(^|_)homepage(_|$)|(^|_)link(_|$)", re.IGNORECASE), "url"),
    # Credit card
    (re.compile(r"(^|_)card_?no(_|$)|(^|_)credit_?card(_|$)|(^|_)cc_?no(_|$)|(^|_)card_?number(_|$)", re.IGNORECASE), "credit_card"),
    # IBAN
    (re.compile(r"(^|_)iban(_|$)|(^|_)bank_?account(_|$)|(^|_)account_?no(_|$)", re.IGNORECASE), "iban"),
    # SWIFT
    (re.compile(r"(^|_)swift(_|$)|(^|_)bic(_|$)", re.IGNORECASE), "swift"),
    # Password (not PII per se, but sensitive)
    (re.compile(r"(^|_)password(_|$)|(^|_)passwd(_|$)|(^|_)pwd(_|$)|(^|_)secret(_|$)|(^|_)token(_|$)", re.IGNORECASE), "secret"),
    # Location
    (re.compile(r"(^|_)latitude(_|$)|(^|_)longitude(_|$)|(^|_)lat(_|$)|(^|_)lon(_|$)|(^|_)geo(_|$)|(^|_)coords?(_|$)", re.IGNORECASE), "geo"),
    # Gender
    (re.compile(r"(^|_)gender(_|$)|(^|_)sex(_|$)", re.IGNORECASE), "gender"),
    # Age
    (re.compile(r"(^|_)age(_|$)|(^|_)age_?group(_|$)", re.IGNORECASE), "age"),
    # National ID
    (re.compile(r"(^|_)national_?id(_|$)|(^|_)passport(_|$)|(^|_)nid(_|$)", re.IGNORECASE), "national_id"),
]


def detect_pii_from_field_name(field_name: str) -> str | None:
    """Return a PII class label for a field name, or None if not detected.

    Uses the last component of a dotted path for matching.
    """
    # Use last component of path
    last = field_name.split(".")[-1].split("[")[0]

    for pattern, pii_class in _FIELD_HEURISTICS:
        if pattern.search(last):
            return pii_class

    return None
