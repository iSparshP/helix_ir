"""Benchmark: schema inference throughput."""

from __future__ import annotations

import time
import random
import string


def _random_string(length: int = 10) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def generate_flat_docs(n: int) -> list[dict]:
    """Generate n flat order-like documents."""
    statuses = ["pending", "shipped", "delivered", "cancelled"]
    return [
        {
            "order_id": f"ORD-{i:06d}",
            "customer_email": f"user{i}@example.com",
            "amount": round(random.uniform(10.0, 500.0), 2),
            "quantity": random.randint(1, 10),
            "status": random.choice(statuses),
            "created_at": "2024-01-01T00:00:00",
            "notes": _random_string(20) if random.random() > 0.5 else None,
        }
        for i in range(n)
    ]


def generate_nested_docs(n: int) -> list[dict]:
    """Generate n nested order documents with items arrays."""
    statuses = ["pending", "shipped", "delivered"]
    categories = ["electronics", "clothing", "food", "books"]
    return [
        {
            "order_id": f"ORD-{i:06d}",
            "customer": {
                "name": f"Customer {i}",
                "email": f"user{i}@example.com",
                "address": {
                    "street": f"{i} Main St",
                    "city": random.choice(["Mumbai", "Delhi", "Bangalore"]),
                    "pincode": str(random.randint(100000, 999999)),
                },
            },
            "items": [
                {
                    "sku": f"SKU-{j:04d}",
                    "name": f"Product {j}",
                    "price": round(random.uniform(5.0, 200.0), 2),
                    "qty": random.randint(1, 5),
                    "category": random.choice(categories),
                }
                for j in range(random.randint(1, 5))
            ],
            "total": round(random.uniform(10.0, 1000.0), 2),
            "status": random.choice(statuses),
        }
        for i in range(n)
    ]


def bench_flat_inference(n: int = 10_000) -> None:
    from helix_ir.infer import infer

    docs = generate_flat_docs(n)
    start = time.perf_counter()
    schema = infer(docs, name="orders", detect_pii=False, sample_size=2000)
    elapsed = time.perf_counter() - start
    print(f"Flat inference ({n} docs): {elapsed:.3f}s ({n/elapsed:.0f} docs/s)")
    print(f"  Fields: {len(schema)}")


def bench_nested_inference(n: int = 5_000) -> None:
    from helix_ir.infer import infer

    docs = generate_nested_docs(n)
    start = time.perf_counter()
    schema = infer(docs, name="orders", detect_pii=False, sample_size=1000)
    elapsed = time.perf_counter() - start
    print(f"Nested inference ({n} docs): {elapsed:.3f}s ({n/elapsed:.0f} docs/s)")
    print(f"  Fields: {len(schema)}")


def bench_reservoir_sampling(n: int = 100_000, k: int = 2000) -> None:
    from helix_ir.infer.sampler import reservoir_sample

    docs = list(range(n))
    start = time.perf_counter()
    sample = reservoir_sample(docs, k=k, seed=42)
    elapsed = time.perf_counter() - start
    print(f"Reservoir sampling ({n} items → {k}): {elapsed:.3f}s")
    assert len(sample) == k


if __name__ == "__main__":
    print("=== Helix IR Inference Benchmarks ===\n")
    bench_reservoir_sampling()
    bench_flat_inference()
    bench_nested_inference()
