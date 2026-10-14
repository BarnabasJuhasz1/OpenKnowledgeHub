"""The identity of a paper inside OpenKnowledge.

``PaperId`` is opaque on purpose: code outside the adapters passes it around and compares it,
but never does arithmetic on it or confuses it with another integer. Today it equals the
Semantic Scholar corpus ID. A mapping table can take over once a second source arrives,
without rekeying any store.
"""

from __future__ import annotations

from dataclasses import dataclass

_UINT64_MAX = 2**64 - 1


@dataclass(frozen=True, slots=True, order=True)
class PaperId:
    """One paper. Holds an unsigned 64-bit integer, the width every store uses for it."""

    value: int

    def __post_init__(self) -> None:
        """Reject anything a UInt64 column could not hold, and booleans posing as integers."""
        if type(self.value) is not int:
            raise TypeError(f"PaperId takes an int, not {type(self.value).__name__}")
        if not 0 <= self.value <= _UINT64_MAX:
            raise ValueError(f"PaperId {self.value} is outside the unsigned 64-bit range")

    def __str__(self) -> str:
        """The bare number, as it appears in URLs and logs."""
        return str(self.value)
