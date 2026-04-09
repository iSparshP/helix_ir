"""Unit tests for the type system and lattice."""

from __future__ import annotations

import pytest
import pyarrow as pa

from helix_ir.types.core import HelixType
from helix_ir.types.lattice import join, meet, subsumes


def ht(arrow_type: pa.DataType, **kwargs) -> HelixType:
    """Helper: create a HelixType with given arrow_type."""
    return HelixType(arrow_type=arrow_type, **kwargs)


# -------------------------------------------------------------------------
# Test join() properties
# -------------------------------------------------------------------------


class TestJoinIdempotence:
    """join(a, a) = a."""

    @pytest.mark.parametrize("arrow_type", [
        pa.int64(),
        pa.float64(),
        pa.string(),
        pa.bool_(),
        pa.date32(),
        pa.timestamp("us"),
    ])
    def test_idempotent(self, arrow_type: pa.DataType) -> None:
        a = ht(arrow_type, sample_count=10)
        result = join(a, a)
        assert result.arrow_type == arrow_type


class TestNullAbsorption:
    """join(Null, a) = a with null_ratio incremented."""

    def test_null_absorbed_by_string(self) -> None:
        null_t = ht(pa.null(), sample_count=5)
        str_t = ht(pa.string(), sample_count=10, null_ratio=0.0)
        result = join(null_t, str_t)
        assert result.arrow_type == pa.string()
        assert result.null_ratio > 0.0

    def test_null_absorbed_by_int(self) -> None:
        null_t = ht(pa.null(), sample_count=2)
        int_t = ht(pa.int64(), sample_count=8)
        result = join(null_t, int_t)
        assert result.arrow_type == pa.int64()
        # 2 nulls out of 10 total
        assert abs(result.null_ratio - 0.2) < 0.01

    def test_symmetric(self) -> None:
        null_t = ht(pa.null(), sample_count=3)
        int_t = ht(pa.int64(), sample_count=7)
        r1 = join(null_t, int_t)
        r2 = join(int_t, null_t)
        assert r1.arrow_type == r2.arrow_type
        assert abs(r1.null_ratio - r2.null_ratio) < 1e-9


class TestNumericWidening:
    """Numeric types should widen correctly."""

    @pytest.mark.parametrize("narrow, wide", [
        (pa.int8(), pa.int16()),
        (pa.int8(), pa.int32()),
        (pa.int8(), pa.int64()),
        (pa.int16(), pa.int32()),
        (pa.int16(), pa.int64()),
        (pa.int32(), pa.int64()),
        (pa.float32(), pa.float64()),
    ])
    def test_int_widening(self, narrow: pa.DataType, wide: pa.DataType) -> None:
        result = join(ht(narrow), ht(wide))
        assert result.arrow_type == wide

    def test_int_to_float(self) -> None:
        result = join(ht(pa.int64()), ht(pa.float64()))
        assert result.arrow_type == pa.float64()

    def test_float_widens_int(self) -> None:
        result = join(ht(pa.int32()), ht(pa.float32()))
        assert result.arrow_type == pa.float64()

    def test_commutativity_numeric(self) -> None:
        """join(a, b) == join(b, a) for numeric types."""
        pairs = [
            (pa.int32(), pa.int64()),
            (pa.int64(), pa.float64()),
            (pa.float32(), pa.float64()),
        ]
        for a, b in pairs:
            r1 = join(ht(a), ht(b))
            r2 = join(ht(b), ht(a))
            assert r1.arrow_type == r2.arrow_type, f"join({a},{b}) != join({b},{a})"


class TestStringFallback:
    """join(String, X) = String for non-null X."""

    @pytest.mark.parametrize("other", [
        pa.int64(),
        pa.float64(),
        pa.bool_(),
        pa.date32(),
    ])
    def test_string_absorbs_others(self, other: pa.DataType) -> None:
        result = join(ht(pa.string()), ht(other))
        assert result.arrow_type == pa.string()

    def test_string_commutative(self) -> None:
        for other in [pa.int64(), pa.bool_()]:
            r1 = join(ht(pa.string()), ht(other))
            r2 = join(ht(other), ht(pa.string()))
            assert r1.arrow_type == r2.arrow_type == pa.string()


class TestListRecursive:
    """join(List(a), List(b)) = List(join(a, b))."""

    def test_list_int_widen(self) -> None:
        a = ht(pa.list_(pa.int32()))
        b = ht(pa.list_(pa.int64()))
        result = join(a, b)
        assert pa.types.is_list(result.arrow_type)
        assert result.arrow_type.value_type == pa.int64()

    def test_list_string_absorbs(self) -> None:
        a = ht(pa.list_(pa.string()))
        b = ht(pa.list_(pa.int64()))
        result = join(a, b)
        assert pa.types.is_list(result.arrow_type)
        assert result.arrow_type.value_type == pa.string()


class TestStructRecursive:
    """Struct types are merged field-by-field."""

    def test_struct_same_fields(self) -> None:
        t1 = pa.struct([pa.field("x", pa.int32()), pa.field("y", pa.string())])
        t2 = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.string())])
        result = join(ht(t1), ht(t2))
        assert pa.types.is_struct(result.arrow_type)
        st = result.arrow_type
        idx = st.get_field_index("x")
        assert st.field(idx).type == pa.int64()

    def test_struct_extra_field(self) -> None:
        """If one struct has an extra field, it's included."""
        t1 = pa.struct([pa.field("x", pa.int32())])
        t2 = pa.struct([pa.field("x", pa.int32()), pa.field("y", pa.string())])
        result = join(ht(t1), ht(t2))
        assert pa.types.is_struct(result.arrow_type)
        names = [result.arrow_type.field(i).name for i in range(result.arrow_type.num_fields)]
        assert "x" in names
        assert "y" in names


class TestJsonBlobFallback:
    """Union with >4 members falls back to JsonBlob."""

    def test_jsonblob_fallback(self) -> None:
        from helix_ir.types.semantic import JSONBLOB_TYPE

        # Start with a union that has 4 members, then add one more
        a = ht(pa.int64())
        b = ht(pa.string())  # String absorbs, so use a different approach

        # Build a 5-way union manually by having semantic = 'union:a|b|c|d'
        from helix_ir.types.core import HelixType
        union4 = HelixType(
            arrow_type=pa.string(),
            semantic="union:int64|float64|bool|date32",
        )
        new_type = HelixType(arrow_type=pa.binary())
        result = join(union4, new_type)
        # With 5 members, should become JsonBlob
        assert result.semantic == JSONBLOB_TYPE


# -------------------------------------------------------------------------
# Test subsumes()
# -------------------------------------------------------------------------


class TestSubsumes:
    def test_same_type(self) -> None:
        assert subsumes(ht(pa.int64()), ht(pa.int64()))

    def test_wider_subsumes_narrower(self) -> None:
        assert subsumes(ht(pa.int64()), ht(pa.int32()))
        assert subsumes(ht(pa.float64()), ht(pa.float32()))

    def test_narrower_does_not_subsume_wider(self) -> None:
        assert not subsumes(ht(pa.int32()), ht(pa.int64()))

    def test_string_subsumes_all(self) -> None:
        for t in [pa.int64(), pa.float64(), pa.bool_(), pa.date32()]:
            assert subsumes(ht(pa.string()), ht(t))

    def test_null_subsumed_by_all(self) -> None:
        for t in [pa.int64(), pa.float64(), pa.string(), pa.bool_()]:
            assert subsumes(ht(t), ht(pa.null()))

    def test_timestamp_subsumes_date(self) -> None:
        assert subsumes(ht(pa.timestamp("us")), ht(pa.date32()))


# -------------------------------------------------------------------------
# Test meet()
# -------------------------------------------------------------------------


class TestMeet:
    def test_same_type(self) -> None:
        result = meet(ht(pa.int64()), ht(pa.int64()))
        assert result.arrow_type == pa.int64()

    def test_narrower_int(self) -> None:
        result = meet(ht(pa.int32()), ht(pa.int64()))
        assert result.arrow_type == pa.int32()

    def test_null_is_bottom(self) -> None:
        result = meet(ht(pa.null()), ht(pa.int64()))
        assert result.arrow_type == pa.null()


# -------------------------------------------------------------------------
# Test HelixType.evolve()
# -------------------------------------------------------------------------


class TestHelixTypeEvolve:
    def test_evolve_changes_field(self) -> None:
        original = HelixType(arrow_type=pa.int64(), null_ratio=0.0)
        evolved = original.evolve(null_ratio=0.5)
        assert evolved.null_ratio == 0.5
        assert evolved.arrow_type == pa.int64()
        assert original.null_ratio == 0.0  # Immutable

    def test_evolve_multiple_fields(self) -> None:
        original = HelixType(arrow_type=pa.string())
        evolved = original.evolve(semantic="email", pii_class="email", sample_count=100)
        assert evolved.semantic == "email"
        assert evolved.pii_class == "email"
        assert evolved.sample_count == 100
