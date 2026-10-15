"""The order desk's object types.

Two types: ``Customer`` keyed by ``customer_id`` and ``Order`` keyed by ``order_id``. An order
references its customer by id through an ``x-ref`` field, so a read can expand it. Both carry
a JSON schema that persistence enforces on every write.
"""

from dbpersist_sdk import DBPersistClient, DuplicateKey

CUSTOMER = "Customer"
ORDER = "Order"

CUSTOMER_SCHEMA = {
    "type": "object",
    "required": ["customer_id", "name", "tier"],
    "properties": {
        "customer_id": {"type": "string"},
        "name": {"type": "string"},
        "tier": {"type": "string", "enum": ["standard", "vip"]},
        "email": {"type": "string"},
    },
}

ORDER_SCHEMA = {
    "type": "object",
    "required": ["order_id", "customer", "symbol", "side", "qty", "status"],
    "properties": {
        "order_id": {"type": "string"},
        "customer": {"type": "string", "x-ref": CUSTOMER},  # id of the Customer object
        "symbol": {"type": "string"},
        "side": {"type": "string", "enum": ["buy", "sell"]},
        "qty": {"type": "integer", "minimum": 1},
        "filled": {"type": "integer", "minimum": 0},
        "limit_price": {"type": ["number", "null"]},
        "status": {"type": "string", "enum": ["open", "partial", "filled", "cancelled"]},
        "notes": {"type": "array", "items": {"type": "string"}},
    },
}


async def ensure_types(client: DBPersistClient) -> None:
    """Register the two types; a second run finds them already there and moves on."""
    for name, key_fields, schema in (
        (CUSTOMER, ["customer_id"], CUSTOMER_SCHEMA),
        (ORDER, ["order_id"], ORDER_SCHEMA),
    ):
        try:
            await client.register_type(name, key_fields, schema)
            print(f"registered type {name}")
        except DuplicateKey:
            pass
        except Exception as exc:  # the API answers 409 for an existing type
            if getattr(exc, "status", None) != 409:
                raise
