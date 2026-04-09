"""Sample fixture data for helix_ir tests."""

from __future__ import annotations

from typing import Any

# Flat order documents
FLAT_ORDERS: list[dict[str, Any]] = [
    {
        "order_id": "ORD-001",
        "customer_email": "alice@example.com",
        "amount": 99.99,
        "quantity": 2,
        "status": "shipped",
        "created_at": "2024-01-01T10:00:00",
    },
    {
        "order_id": "ORD-002",
        "customer_email": "bob@example.com",
        "amount": 149.50,
        "quantity": 1,
        "status": "pending",
        "created_at": "2024-01-02T11:30:00",
    },
    {
        "order_id": "ORD-003",
        "customer_email": "carol@example.com",
        "amount": 75.00,
        "quantity": 3,
        "status": "delivered",
        "created_at": "2024-01-03T09:15:00",
    },
    {
        "order_id": "ORD-004",
        "customer_email": None,  # nullable
        "amount": 200.00,
        "quantity": 1,
        "status": "cancelled",
        "created_at": "2024-01-04T14:00:00",
    },
    {
        "order_id": "ORD-005",
        "customer_email": "dave@example.com",
        "amount": 55.25,
        "quantity": 4,
        "status": "shipped",
        "created_at": "2024-01-05T08:00:00",
    },
]

# Nested MongoDB-style orders with embedded customer + items array
NESTED_ORDERS: list[dict[str, Any]] = [
    {
        "order_id": "ORD-001",
        "customer": {
            "name": "Alice Smith",
            "email": "alice@example.com",
            "phone": "+91-9876543210",
            "address": {
                "street": "123 Main St",
                "city": "Bangalore",
                "pincode": "560001",
            },
        },
        "items": [
            {"sku": "SKU-001", "name": "Widget A", "price": 25.00, "qty": 2},
            {"sku": "SKU-002", "name": "Widget B", "price": 49.99, "qty": 1},
        ],
        "total": 99.99,
        "status": "shipped",
        "tags": ["premium", "fast_shipping"],
    },
    {
        "order_id": "ORD-002",
        "customer": {
            "name": "Bob Jones",
            "email": "bob@example.com",
            "phone": "+91-9876543211",
            "address": {
                "street": "456 Oak Ave",
                "city": "Mumbai",
                "pincode": "400001",
            },
        },
        "items": [
            {"sku": "SKU-003", "name": "Gadget X", "price": 149.50, "qty": 1},
        ],
        "total": 149.50,
        "status": "pending",
        "tags": [],
    },
    {
        "order_id": "ORD-003",
        "customer": {
            "name": "Carol White",
            "email": "carol@example.com",
            "phone": None,  # optional phone
            "address": {
                "street": "789 Pine Rd",
                "city": "Delhi",
                "pincode": "110001",
            },
        },
        "items": [
            {"sku": "SKU-001", "name": "Widget A", "price": 25.00, "qty": 3},
        ],
        "total": 75.00,
        "status": "delivered",
        "tags": ["express"],
    },
]

# Documents with PAN and Aadhaar for PII testing
PII_DOCS: list[dict[str, Any]] = [
    {
        "user_id": "U001",
        "pan": "ABCDE1234F",
        "aadhaar": "234567890123",
        "email": "user1@example.com",
        "phone": "9876543210",
        "ssn": "123-45-6789",
    },
    {
        "user_id": "U002",
        "pan": "FGHIJ5678K",
        "aadhaar": "345678901234",
        "email": "user2@example.com",
        "phone": "8765432109",
        "ssn": "234-56-7890",
    },
    {
        "user_id": "U003",
        "pan": "KLMNO9012P",
        "aadhaar": "456789012345",
        "email": "user3@example.com",
        "phone": "7654321098",
        "ssn": "345-67-8901",
    },
]

# Documents for enum detection (>200 samples, <50 unique values)
def make_enum_docs(n: int = 300) -> list[dict[str, Any]]:
    """Generate n documents with a low-cardinality status field."""
    statuses = ["active", "inactive", "pending", "suspended"]
    categories = ["A", "B", "C"]
    return [
        {
            "id": i,
            "status": statuses[i % len(statuses)],
            "category": categories[i % len(categories)],
            "value": float(i),
        }
        for i in range(n)
    ]


# Documents with mixed types for union testing
MIXED_TYPE_DOCS: list[dict[str, Any]] = [
    {"id": 1, "value": 42},          # int
    {"id": 2, "value": "hello"},     # string
    {"id": 3, "value": 3.14},        # float
    {"id": 4, "value": True},        # bool
    {"id": 5, "value": None},        # null
]
