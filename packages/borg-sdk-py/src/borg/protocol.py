"""
The wire messages, as this side of the connection sees them. ``crates/borg-protocol`` is the
contract; this is a transcription of it, and the scenarios are what keep the two honest.

**Every message is a single-key object**, including the payload-free ones, so dispatching is one
key lookup with no special cases (§17.4).

Engine → worker::

    {"invoke": {"producer": "12342029420047889112", "input": "Company:o-04068"}}
    {"value": "acme.ai"}   # or {"value": null} — the cell has never been written
    {"ok": {}}
    {"shutdown": {}}

Worker → engine::

    {"get": "Company:o-04068.website"}
    {"get_input": "Company:o-04068.founded"}     # migrations only (§9.3)
    {"set": {"cell": "Company:o-04068.isInvestible", "value": "true"}}
    {"done": {}}
    {"error": {"message": "…"}}

The messages are plain dicts rather than classes. There is nothing to model: each one is a single
key whose value is a string, ``None``, or a two-key object, and a dataclass layer over that would be
a second place for the shape to drift from ``borg-protocol``.
"""

from __future__ import annotations

__all__ = ["DESCRIBE_ARG", "TRANSPORT", "VERSION", "producer_id"]

#: The protocol version the engine announces in its hello.
VERSION = 1

#: The transport every repo written with this SDK asks for. See :mod:`borg.connection`.
TRANSPORT = "socket"

#: The one argument the engine passes an executable before it is ever a worker.
DESCRIBE_ARG = "describe"


def producer_id(name: str) -> str:
    """
    The id the engine derives from a producer's name (§9.2), computed here so a repo serving several
    pipelines can dispatch an ``invoke`` to the right one.

    FNV-1a over 64 bits. It is returned **as a string**, and the engine sends it as one, because a
    ``ProducerId`` is a hash past 2⁵³ and JSON has no integers: a client that read it as a JSON
    number would resolve a producer that does not exist. Python's ``int`` could hold it perfectly
    well — the string is not for Python's benefit, it is the contract, and a dispatch table keyed on
    a different type in each language is a bug waiting for the first repo with two pipelines.
    """
    mask = (1 << 64) - 1
    hash_ = 0xCBF29CE484222325
    for byte in name.encode("utf-8"):
        hash_ = (hash_ ^ byte) & mask
        hash_ = (hash_ * 0x100000001B3) & mask
    # Kept away from the small ids a human might type into a def file, exactly as the engine does.
    return str(hash_ | (1 << 32))
