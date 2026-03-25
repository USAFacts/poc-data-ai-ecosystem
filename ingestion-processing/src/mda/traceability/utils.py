"""Traceability utilities.

Ported from model_D/mda/traceability/utils.py.
"""

import uuid


def mint_utid() -> str:
    """Mint a new Universal Trace ID (UTID).

    UTIDs are unique identifiers used for execution traceability.
    They link all artifacts produced during a single execution run.

    Returns:
        A unique UTID string in format: utid-{12-char-hex}.
    """
    return f"utid-{uuid.uuid4().hex[:12]}"
