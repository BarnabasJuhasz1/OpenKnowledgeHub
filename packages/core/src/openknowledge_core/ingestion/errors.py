"""Ingestion failures, whose messages never hold a key or a signed query string."""

# HTTP statuses that may pass on a retry, which every adapter turns into a TransientError.
TRANSIENT_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


class IngestionError(Exception):
    pass


class TransientError(IngestionError):
    """Network or service trouble that may pass, the only kind worth a retry."""


class SourceUnavailable(TransientError):
    """Semantic Scholar or its file host sent a transient status, or the connection failed."""


class StoreUnavailable(TransientError):
    """A GCS call failed in a way a retry may fix."""


class CredentialRejected(IngestionError):
    pass


class SourceRejected(IngestionError):
    """Semantic Scholar refused a request, or sent data this code cannot use."""


class LinkExpired(IngestionError):
    """A signed link no longer works and needs a fresh listing."""


class ShardNotListed(IngestionError):
    pass


class InventoryChanged(IngestionError):
    """A fresh listing names other shards than the pinned inventory does."""


class ArchiveConflict(IngestionError):
    """A document already in the bucket disagrees with this run and stays as it is."""


class StoreRejected(IngestionError):
    """A GCS call failed in a way a retry will not fix, such as refused access or no credentials."""


class ObjectChanged(IngestionError):
    """An object is missing or has another generation than the one being read."""


class IntegrityError(IngestionError):
    pass


class GzipInvalid(IntegrityError):
    pass


class IncompleteTransfer(IntegrityError):
    pass


class StoredCopyMismatch(IntegrityError):
    pass


class ReceiptInvalid(IntegrityError):
    pass
