"""Tiny path and template language used by inbound mappings.

Paths address JSON documents: ``data.account.id``, ``items[0].symbol``, with an optional
leading ``$.``. Templates are strings with ``{path}`` placeholders: ``"{source}:{data.id}"``.
Expressions used in mappings are one of:

- a path (default): ``"data.accountId"``
- a template (contains ``{``): ``"ACC-{data.accountId}"``
- a literal: ``"const:Account"``
"""

import re
from typing import Any

MISSING = object()

_SEG = re.compile(r"([^.\[\]]+)|\[(\d+)\]")
_PLACEHOLDER = re.compile(r"\{([^{}]+)\}")


class ExtractError(ValueError):
    pass


def get_path(obj: Any, path: str) -> Any:
    """Walk ``path`` through dicts and lists; returns MISSING if any step is absent."""
    if path.startswith("$."):
        path = path[2:]
    elif path == "$":
        return obj
    cur = obj
    for m in _SEG.finditer(path):
        name, index = m.group(1), m.group(2)
        if index is not None:
            if not isinstance(cur, list) or int(index) >= len(cur):
                return MISSING
            cur = cur[int(index)]
        else:
            if not isinstance(cur, dict) or name not in cur:
                return MISSING
            cur = cur[name]
    return cur


def render(template: str, obj: Any) -> str:
    """Fill ``{path}`` placeholders; raises ExtractError when a path is missing or null."""

    def sub(m: re.Match[str]) -> str:
        v = get_path(obj, m.group(1).strip())
        if v is MISSING or v is None:
            raise ExtractError(f"template path {m.group(1)!r} not found")
        return str(v)

    return _PLACEHOLDER.sub(sub, template)


def evaluate(expr: str, obj: Any) -> Any:
    """Resolve a mapping expression (literal, template or path) against ``obj``.

    Returns MISSING for an absent path. Literals and templates always return strings.
    """
    if expr.startswith("const:"):
        return expr[len("const:") :]
    if "{" in expr:
        return render(expr, obj)
    return get_path(obj, expr)
