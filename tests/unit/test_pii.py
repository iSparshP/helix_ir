"""Unit tests for PII detection."""

from __future__ import annotations

import pytest
import pyarrow as pa

from helix_ir.pii.classifier import detect_pii
from helix_ir.pii.heuristics import detect_pii_from_field_name
from helix_ir.pii.regex_patterns.common import EMAIL_PATTERN, PHONE_PATTERN
from helix_ir.pii.regex_patterns.india import AADHAAR_PATTERN, GSTIN_PATTERN, PAN_PATTERN
from helix_ir.pii.regex_patterns.usa import SSN_PATTERN
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


def ht(arrow_type: pa.DataType, **kwargs) -> HelixType:
    return HelixType(arrow_type=arrow_type, **kwargs)


# -------------------------------------------------------------------------
# Field name heuristics
# -------------------------------------------------------------------------


class TestFieldNameHeuristics:
    @pytest.mark.parametrize("field_name,expected_pii", [
        ("email", "email"),
        ("customer_email", "email"),
        ("email_address", "email"),
        ("phone", "phone"),
        ("mobile", "phone"),
        ("cell_number", "phone"),
        ("full_name", "name"),
        ("first_name", "name"),
        ("address", "address"),
        ("street", "address"),
        ("pincode", "address"),
        ("dob", "dob"),
        ("date_of_birth", "dob"),
        ("ssn", "ssn"),
        ("social_security", "ssn"),
        ("pan", "pan"),
        ("aadhaar", "aadhaar"),
        ("aadhar", "aadhaar"),
        ("gstin", "gstin"),
        ("ip_address", "ip"),
        ("client_ip", "ip"),
        ("url", "url"),
        ("website", "url"),
        ("credit_card", "credit_card"),
        ("card_no", "credit_card"),
    ])
    def test_field_name_detection(self, field_name: str, expected_pii: str) -> None:
        result = detect_pii_from_field_name(field_name)
        assert result == expected_pii, f"Field '{field_name}': expected {expected_pii!r}, got {result!r}"

    def test_non_pii_field(self) -> None:
        assert detect_pii_from_field_name("order_id") is None
        assert detect_pii_from_field_name("amount") is None
        assert detect_pii_from_field_name("quantity") is None


# -------------------------------------------------------------------------
# Regex patterns
# -------------------------------------------------------------------------


class TestEmailRegex:
    @pytest.mark.parametrize("email", [
        "user@example.com",
        "alice.bob+tag@sub.domain.org",
        "test@mail.co.uk",
    ])
    def test_valid_emails(self, email: str) -> None:
        assert EMAIL_PATTERN.fullmatch(email), f"Should match: {email}"

    @pytest.mark.parametrize("invalid", [
        "notanemail",
        "@nodomain.com",
        "user@",
        "user@domain",
    ])
    def test_invalid_emails(self, invalid: str) -> None:
        assert not EMAIL_PATTERN.fullmatch(invalid), f"Should not match: {invalid}"


class TestPANRegex:
    @pytest.mark.parametrize("pan", [
        "ABCDE1234F",
        "FGHIJ5678K",
        "KLMNO9012P",
    ])
    def test_valid_pan(self, pan: str) -> None:
        assert PAN_PATTERN.fullmatch(pan), f"Should match PAN: {pan}"

    @pytest.mark.parametrize("invalid", [
        "ABC1234F",       # too short
        "ABCDE12345",     # no letter at end
        "12345ABCDE",     # starts with digits
    ])
    def test_invalid_pan(self, invalid: str) -> None:
        assert not PAN_PATTERN.fullmatch(invalid), f"Should not match PAN: {invalid}"


class TestAadhaarRegex:
    @pytest.mark.parametrize("aadhaar", [
        "234567890123",
        "345678901234",
        "456789012345",
    ])
    def test_valid_aadhaar(self, aadhaar: str) -> None:
        assert AADHAAR_PATTERN.fullmatch(aadhaar), f"Should match Aadhaar: {aadhaar}"

    def test_invalid_aadhaar_starts_with_zero(self) -> None:
        assert not AADHAAR_PATTERN.fullmatch("012345678901")

    def test_invalid_aadhaar_too_short(self) -> None:
        assert not AADHAAR_PATTERN.fullmatch("23456789012")


class TestSSNRegex:
    @pytest.mark.parametrize("ssn", [
        "123-45-6789",
        "234-56-7890",
        "345-67-8901",
    ])
    def test_valid_ssn(self, ssn: str) -> None:
        assert SSN_PATTERN.fullmatch(ssn), f"Should match SSN: {ssn}"

    def test_invalid_ssn_all_zeros(self) -> None:
        assert not SSN_PATTERN.fullmatch("000-00-0000")


# -------------------------------------------------------------------------
# detect_pii() function
# -------------------------------------------------------------------------


class TestDetectPII:
    def test_detects_email_by_name(self) -> None:
        schema = Schema(
            name="test",
            fields=(("email", ht(pa.string())),),
        )
        result = detect_pii(schema, locale="in")
        assert result.field("email").pii_class == "email"

    def test_detects_phone_by_name(self) -> None:
        schema = Schema(
            name="test",
            fields=(("phone", ht(pa.string())),),
        )
        result = detect_pii(schema, locale="in")
        assert result.field("phone").pii_class == "phone"

    def test_detects_pan_by_values(self) -> None:
        schema = Schema(
            name="test",
            fields=(("doc_number", ht(pa.string())),),
        )
        sample_values = {
            "doc_number": ["ABCDE1234F", "FGHIJ5678K", "KLMNO9012P"] * 30,
        }
        result = detect_pii(
            schema,
            sample_values=sample_values,
            locale="in",
            layers=["regex"],
        )
        assert result.field("doc_number").pii_class == "pan"

    def test_email_value_detection(self) -> None:
        schema = Schema(
            name="test",
            fields=(("contact", ht(pa.string())),),
        )
        sample_values = {
            "contact": ["alice@example.com", "bob@test.org", "carol@mail.co"] * 30,
        }
        result = detect_pii(
            schema,
            sample_values=sample_values,
            locale="in",
            layers=["regex"],
        )
        assert result.field("contact").pii_class == "email"

    def test_existing_pii_class_preserved(self) -> None:
        schema = Schema(
            name="test",
            fields=(("field1", ht(pa.string(), pii_class="ssn")),),
        )
        result = detect_pii(schema, locale="in")
        assert result.field("field1").pii_class == "ssn"

    def test_non_pii_field_has_no_class(self) -> None:
        schema = Schema(
            name="test",
            fields=(("order_id", ht(pa.string())),),
        )
        result = detect_pii(schema, locale="in")
        assert result.field("order_id").pii_class is None
