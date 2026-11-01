"""Deterministic guardrails used before and after model inference."""

import re

# Output masking is a final safety net. The MCP tools already omit PII columns.
EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
# Start broad, then count digits in mask_sensitive_text to avoid masking dates.
PHONE_CANDIDATE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d ()-]{7,}\d)(?!\d)")

# These deterministic checks run before the LLM. They cover clear instruction
# bypass, mutation, and bulk-sensitive-data requests without pretending to be a
# complete semantic moderation system.
BLOCKED_REQUEST_PATTERNS = [
    re.compile(r"ignore\s+(all\s+)?(previous|prior|system)\s+instructions", re.I),
    re.compile(r"\b(drop|truncate)\s+(table|schema|catalog)\b", re.I),
    re.compile(r"\b(delete\s+from|insert\s+into|update\s+.+\s+set)\b", re.I),
    re.compile(r"\b(all|every|bulk)\b.{0,25}\b(e-?mails?|phone numbers?|credit cards?|ssn)\b", re.I),
    re.compile(r"\b(customer|user)\s+(e-?mails?|phone numbers?|credit cards?|ssn)\b", re.I),
]

# The Copilot is intentionally narrow. These patterns are allow-list intents, not
# a list of known bad prompts. A request must clearly look like an OrderOps
# question before it reaches the model.
ORDEROPS_INTENT_PATTERNS = [
    re.compile(r"\bORD-\d{5}\b", re.I),
    re.compile(
        r"(?=.*\border(s)?\b)(?=.*\b(status|current|snapshot|timeline|history|happened|changed)\b)",
        re.I,
    ),
    re.compile(
        r"(?=.*\b(delay|delayed|overdue|exception|exceptions|risk|risks)\b)"
        r"(?=.*\b(order(s)?|region|regional|north|south|east|west|shipment|delivery)\b)",
        re.I,
    ),
    re.compile(
        r"(?=.*\b(region|regional|north|south|east|west)\b)"
        r"(?=.*\b(order(s)?|delay|delayed|exception|exceptions|metric|metrics|summary|total|count)\b)",
        re.I,
    ),
    re.compile(
        r"(?=.*\b(metric|metrics|summary|total|count)\b)"
        r"(?=.*\b(order(s)?|region|regional|status|delay|delayed|exception|exceptions)\b)",
        re.I,
    ),
    re.compile(
        r"(?=.*\b(cdc|change|changes|change feed)\b)"
        r"(?=.*\b(order(s)?|history|timeline|status)\b)",
        re.I,
    ),
    re.compile(
        r"(?=.*\b(delivered|cancelled|canceled|shipped|processing|pending)\b)(?=.*\border(s)?\b)",
        re.I,
    ),
]

OFF_TOPIC_MESSAGE = (
    "This Copilot is scoped to OrderOps questions only. Ask about orders, order "
    "status, timelines, CDC changes, delays, exceptions, regions, or metrics."
)


def check_input(question):
    """Return whether a question may proceed and a user-safe explanation."""
    cleaned = str(question or "").strip()
    if not cleaned:
        return False, "Enter a question before sending it."
    if len(cleaned) > 1500:
        return False, "The question is too long. Keep it under 1,500 characters."

    # A single match is enough to stop the request before model inference.
    for pattern in BLOCKED_REQUEST_PATTERNS:
        if pattern.search(cleaned):
            return (
                False,
                "This request was blocked because it asks for sensitive data, "
                "instruction bypass, or a data-changing operation. OrderOps Control Tower is read-only.",
            )

    if not any(pattern.search(cleaned) for pattern in ORDEROPS_INTENT_PATTERNS):
        return False, OFF_TOPIC_MESSAGE

    return True, ""


def mask_sensitive_text(text):
    """Mask accidental email or phone-number output before displaying it."""
    value = str(text or "")
    value = EMAIL_PATTERN.sub("[masked-email]", value)

    def mask_phone_candidate(match):
        candidate = match.group(0)
        digit_count = len(re.sub(r"\D", "", candidate))
        # This avoids treating an eight-digit calendar date as a phone number.
        return "[masked-phone]" if 10 <= digit_count <= 15 else candidate

    value = PHONE_CANDIDATE_PATTERN.sub(mask_phone_candidate, value)
    return value


def classify_question(question):
    """Create a low-cardinality audit category without storing user text."""
    # Keep this deliberately simple and deterministic for transparent auditing.
    lowered = str(question).lower()
    if any(word in lowered for word in ["timeline", "changed", "history", "happened"]):
        return "order_timeline"
    if any(word in lowered for word in ["delay", "exception", "overdue", "risk"]):
        return "order_exception"
    if any(word in lowered for word in ["summary", "region", "total", "metric"]):
        return "operations_summary"
    if "order" in lowered:
        return "order_snapshot"
    if any(pattern.search(str(question or "")) for pattern in ORDEROPS_INTENT_PATTERNS):
        return "orderops_general"
    return "general"


def sql_identifier(full_name):
    """Quote a trusted three-level table name supplied by an app resource."""
    # Only bundle-provided catalog.schema.table values should reach this helper.
    parts = str(full_name).split(".")
    if len(parts) != 3:
        raise ValueError("A three-level Unity Catalog table name is required")
    if not all(re.fullmatch(r"[A-Za-z0-9_-]+", part) for part in parts):
        raise ValueError("Unexpected character in Unity Catalog table name")
    return ".".join(f"`{part}`" for part in parts)
