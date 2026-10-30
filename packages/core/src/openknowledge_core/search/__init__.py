"""Turning the current rows of the paper tables into search documents, and writing them."""

from openknowledge_core.search.commands import (
    IndexPapers,
    IndexPapersResult,
    SettleIndex,
    SettleIndexResult,
)
from openknowledge_core.search.errors import BulkRefused, SearchRejected, SearchUnavailable
from openknowledge_core.search.models import (
    AbstractRow,
    CorpusSlice,
    DeleteDocument,
    IndexDocument,
    PaperRow,
    SearchDocument,
    SourceRows,
    TldrRow,
)
from openknowledge_core.search.ports import SearchSink, SearchSource
from openknowledge_core.search.service import SearchIndexService, document_operation

__all__ = [
    "AbstractRow",
    "BulkRefused",
    "CorpusSlice",
    "DeleteDocument",
    "IndexDocument",
    "IndexPapers",
    "IndexPapersResult",
    "PaperRow",
    "SearchDocument",
    "SearchIndexService",
    "SearchRejected",
    "SearchSink",
    "SearchSource",
    "SearchUnavailable",
    "SettleIndex",
    "SettleIndexResult",
    "SourceRows",
    "TldrRow",
    "document_operation",
]
