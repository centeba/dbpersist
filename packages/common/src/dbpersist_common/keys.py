"""Object key derivation, shared by persistence (storage) and the SDK (cache)."""

from typing import Any

KEY_SEPARATOR = "\x1f"  # ASCII unit separator between key field values


def build_key(key_fields: list[str], data: dict[str, Any]) -> str:
    parts: list[str] = []
    for f in key_fields:
        if f not in data or data[f] is None:
            raise ValueError(f"key field {f!r} is missing or null")
        parts.append(str(data[f]))
    return KEY_SEPARATOR.join(parts)


def key_from_values(values: list[str]) -> str:
    return KEY_SEPARATOR.join(values)
