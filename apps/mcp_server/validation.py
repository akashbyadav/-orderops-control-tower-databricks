"""Small validation helpers shared by the MCP tools."""

import re

# Tool parameters are interpolated only after matching these narrow contracts.
ORDER_ID_PATTERN = re.compile(r"^ORD-[0-9]{5,10}$")
# A fixed dimension allow-list is clearer than accepting arbitrary SQL fragments.
ALLOWED_REGIONS = {"ALL", "NORTH", "SOUTH", "EAST", "WEST"}


def validate_order_id(order_id):
    """Return a normalized order ID or raise a clear validation error."""
    normalized = str(order_id).strip().upper()
    if not ORDER_ID_PATTERN.fullmatch(normalized):
        raise ValueError("order_id must use the format ORD-00001")
    return normalized


def validate_region(region):
    """Return one of the supported regions."""
    normalized = str(region or "ALL").strip().upper()
    if normalized not in ALLOWED_REGIONS:
        raise ValueError("region must be ALL, NORTH, SOUTH, EAST, or WEST")
    return normalized


def clamp_limit(limit, maximum=50):
    """Keep list responses intentionally small."""
    value = int(limit)
    if value < 1:
        raise ValueError("limit must be at least 1")
    return min(value, maximum)


def clamp_hours(hours, maximum=168):
    """Limit history windows to seven days."""
    value = int(hours)
    if value < 1:
        raise ValueError("hours must be at least 1")
    return min(value, maximum)


def quote_table_name(full_name):
    """Quote a trusted three-level Unity Catalog table name."""
    # Table names originate from Databricks App resources, never tool arguments.
    parts = str(full_name).split(".")
    if len(parts) != 3:
        raise ValueError("A three-level Unity Catalog table name is required")

    for part in parts:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", part):
            raise ValueError("Table names may contain letters, numbers, underscores, and hyphens")

    return ".".join(f"`{part}`" for part in parts)
