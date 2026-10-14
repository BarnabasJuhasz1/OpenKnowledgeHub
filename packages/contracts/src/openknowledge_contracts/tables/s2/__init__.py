"""Semantic Scholar datasets loaded in Phase 1, keyed by their name in the S2 datasets API.

Field names follow the data, not S2's documentation, which still says ``citingPaper`` where
records say ``citingcorpusid``. A field those records never showed as null is required, so the
first null fails the load instead of slipping in. A delete record carries only the key.
"""

from collections.abc import Mapping
from types import MappingProxyType

from openknowledge_contracts.datasets import DatasetName
from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.s2.abstracts import ABSTRACTS
from openknowledge_contracts.tables.s2.authors import AUTHORS
from openknowledge_contracts.tables.s2.citations import CITATIONS
from openknowledge_contracts.tables.s2.paper_ids import PAPER_IDS
from openknowledge_contracts.tables.s2.papers import PAPERS
from openknowledge_contracts.tables.s2.publication_venues import PUBLICATION_VENUES
from openknowledge_contracts.tables.s2.tldrs import TLDRS

CONTRACTS: Mapping[DatasetName, DatasetContract] = MappingProxyType(
    {
        contract.name: contract
        for contract in (
            PAPERS,
            PAPER_IDS,
            ABSTRACTS,
            TLDRS,
            AUTHORS,
            PUBLICATION_VENUES,
            CITATIONS,
        )
    }
)

__all__ = [
    "ABSTRACTS",
    "AUTHORS",
    "CITATIONS",
    "CONTRACTS",
    "PAPERS",
    "PAPER_IDS",
    "PUBLICATION_VENUES",
    "TLDRS",
]
