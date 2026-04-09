"""Confidence scoring and HyperLogLog cardinality estimation."""

from __future__ import annotations

import hashlib
import math


class SimpleHyperLogLog:
    """A simple HyperLogLog cardinality estimator with 2^14 registers.

    This implementation uses 14 bits of precision (16384 registers).
    For small cardinalities (<= 10000), we maintain an exact set for accuracy.
    """

    EXACT_THRESHOLD = 10_000
    PRECISION = 14
    M = 1 << PRECISION  # 16384

    def __init__(self) -> None:
        self._registers: list[int] = [0] * self.M
        self._exact: set[str] | None = set()  # None means we switched to HLL mode

    def add(self, value: object) -> None:
        """Add a value to the estimator."""
        key = str(value)

        # While in exact mode, track items directly
        if self._exact is not None:
            self._exact.add(key)
            if len(self._exact) > self.EXACT_THRESHOLD:
                # Switch to HLL mode
                exact_copy = self._exact
                self._exact = None
                for v in exact_copy:
                    self._add_to_registers(v)
            return

        self._add_to_registers(key)

    def _add_to_registers(self, key: str) -> None:
        """Add a string key directly to the HLL registers."""
        h = int(hashlib.md5(key.encode(), usedforsecurity=False).hexdigest(), 16)
        # Use top PRECISION bits for bucket index
        bucket = h >> (128 - self.PRECISION)
        # Count leading zeros in remaining bits
        remainder = h & ((1 << (128 - self.PRECISION)) - 1)
        rho = _count_leading_zeros(remainder, 128 - self.PRECISION) + 1
        if rho > self._registers[bucket]:
            self._registers[bucket] = rho

    def estimate(self) -> int:
        """Return the estimated cardinality."""
        if self._exact is not None:
            return len(self._exact)

        alpha = _alpha(self.M)
        Z = sum(2.0 ** (-r) for r in self._registers)
        raw = alpha * (self.M ** 2) / Z

        # Small range correction
        if raw <= 2.5 * self.M:
            zeros = self._registers.count(0)
            if zeros > 0:
                raw = self.M * math.log(self.M / zeros)

        return int(round(raw))


def _count_leading_zeros(x: int, bits: int) -> int:
    """Count leading zeros in a `bits`-wide integer."""
    if x == 0:
        return bits
    return bits - x.bit_length()


def _alpha(m: int) -> float:
    """HyperLogLog bias correction constant for m registers."""
    if m == 16:
        return 0.673
    if m == 32:
        return 0.697
    if m == 64:
        return 0.709
    return 0.7213 / (1 + 1.079 / m)


def compute_confidence(sample_count: int, null_ratio: float) -> float:
    """Compute a confidence score [0, 1] based on sample size and null ratio.

    More samples and fewer nulls → higher confidence.
    """
    if sample_count == 0:
        return 0.0
    # Sigmoid-ish: reaches ~0.95 at sample_count=100
    size_factor = 1.0 - math.exp(-sample_count / 50.0)
    # Penalize columns with many nulls
    null_penalty = 1.0 - null_ratio * 0.5
    return min(1.0, size_factor * null_penalty)
