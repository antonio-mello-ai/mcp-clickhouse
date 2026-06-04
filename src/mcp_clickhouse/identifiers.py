"""Safe quoting of ClickHouse SQL identifiers.

These helpers prevent SQL injection when a database, table, or column name is
interpolated into a query string. Identifiers are validated against a strict
pattern and wrapped in backticks (ClickHouse's identifier-quoting syntax), so a
hostile value such as ``foo`; DROP TABLE bar; --`` is rejected instead of
breaking out of the intended statement.
"""

from __future__ import annotations

import re

# A single, unqualified identifier: starts with a letter or underscore, followed
# by letters, digits, or underscores. This is the safe subset accepted by
# ClickHouse for unquoted identifiers and rejects every shell/SQL metacharacter
# (backtick, quote, semicolon, space, dot, dash, etc.).
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(identifier: str) -> str:
    """Return *identifier* unchanged if it is a safe SQL identifier.

    Args:
        identifier: A single (unqualified) database, table, or column name.

    Returns:
        The validated identifier.

    Raises:
        ValueError: If *identifier* is empty or contains any character outside
            ``[A-Za-z0-9_]`` (or starts with a digit).
    """
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(
            f"Invalid SQL identifier: {identifier!r}. "
            "Identifiers must match ^[A-Za-z_][A-Za-z0-9_]*$ "
            "(letters, digits, and underscores only)."
        )
    return identifier


def quote_identifier(identifier: str) -> str:
    """Validate and backtick-quote a single SQL identifier.

    Args:
        identifier: A single (unqualified) database, table, or column name.

    Returns:
        The identifier wrapped in backticks, e.g. ``events`` -> ```` `events` ````.

    Raises:
        ValueError: If *identifier* is not a safe SQL identifier.
    """
    return f"`{validate_identifier(identifier)}`"


def quote_qualified_name(name: str) -> str:
    """Validate and backtick-quote a possibly-qualified identifier.

    Splits on ``.`` so ``db.table`` becomes ```` `db`.`table` ````. Each part is
    validated independently.

    Args:
        name: A plain (``table``) or qualified (``db.table``) identifier.

    Returns:
        The fully back-tick-quoted name.

    Raises:
        ValueError: If *name* is empty, has empty parts (e.g. ``db.``), or any
            part is not a safe SQL identifier.
    """
    parts = name.split(".")
    if not all(parts):
        raise ValueError(
            f"Invalid qualified name: {name!r}. "
            "Expected 'table' or 'database.table' with non-empty parts."
        )
    return ".".join(quote_identifier(part) for part in parts)
