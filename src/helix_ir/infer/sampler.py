"""Algorithm R reservoir sampling (Vitter 1985)."""

from __future__ import annotations

import random
from typing import Iterable, TypeVar

T = TypeVar("T")


def reservoir_sample(
    stream: Iterable[T],
    k: int,
    seed: int | None = None,
) -> list[T]:
    """Sample k items from a stream using Algorithm R (Vitter 1985).

    This algorithm makes a single pass over the stream with O(k) memory.
    The result is reproducible when `seed` is provided.

    Args:
        stream: Any iterable of items.
        k: Maximum number of items to keep in the reservoir.
        seed: Optional random seed for reproducibility.

    Returns:
        A list of at most k items, uniformly sampled without replacement.
    """
    rng = random.Random(seed)
    reservoir: list[T] = []

    for i, item in enumerate(stream):
        if i < k:
            reservoir.append(item)
        else:
            # Replace elements in reservoir with decreasing probability
            j = rng.randint(0, i)
            if j < k:
                reservoir[j] = item

    return reservoir
