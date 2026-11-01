"""Unit tests for values interpolated into fixed MCP SQL templates."""

import sys
from pathlib import Path

# Import the MCP helper directly without installing the App as a package.
APP_DIR = Path(__file__).parents[1] / "apps" / "mcp_server"
sys.path.insert(0, str(APP_DIR))

from validation import clamp_hours, clamp_limit, quote_table_name, validate_order_id, validate_region


# Business identifiers and dimensions are normalized before query construction.
def test_order_id_is_normalized():
    assert validate_order_id(" ord-00042 ") == "ORD-00042"


def test_invalid_order_id_is_rejected():
    try:
        validate_order_id("ORD-1' OR 1=1")
        assert False, "Expected validation to fail"
    except ValueError:
        assert True


def test_region_is_allowlisted():
    assert validate_region("south") == "SOUTH"
    try:
        validate_region("somewhere")
        assert False, "Expected validation to fail"
    except ValueError:
        assert True


# Numeric inputs stay small enough for predictable tool responses and SQL scans.
def test_limits_are_bounded():
    assert clamp_limit(500) == 50
    assert clamp_hours(500) == 168


# App resource names are restricted to three-part Unity Catalog identifiers.
def test_table_name_is_quoted():
    assert quote_table_name("orderops.gold.order_metrics") == (
        "`orderops`.`gold`.`order_metrics`"
    )
