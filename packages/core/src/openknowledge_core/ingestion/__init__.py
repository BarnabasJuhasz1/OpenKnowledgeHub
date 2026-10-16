"""Archiving Semantic Scholar datasets into the raw bucket."""

from openknowledge_core.ingestion.commands import (
    BatchDownloadResult,
    DownloadDataset,
    DownloadDatasetResult,
    DownloadShard,
    DownloadShardResult,
    LoadDataset,
    LoadDatasetResult,
    PinnedRelease,
    PinRelease,
)
from openknowledge_core.ingestion.errors import IngestionError, TransientError
from openknowledge_core.ingestion.load import LoadDatasetService
from openknowledge_core.ingestion.models import (
    JobOutcome,
    LoadOutcome,
    PinnedDataset,
    PreparedLoad,
    ShardJob,
    ShardOutcome,
)
from openknowledge_core.ingestion.ports import (
    ArtifactStore,
    ReleaseCatalog,
    ShardDownloader,
    ShardJobs,
    TableLoader,
)
from openknowledge_core.ingestion.service import DownloadDatasetService

__all__ = [
    "ArtifactStore",
    "BatchDownloadResult",
    "DownloadDataset",
    "DownloadDatasetResult",
    "DownloadDatasetService",
    "DownloadShard",
    "DownloadShardResult",
    "IngestionError",
    "JobOutcome",
    "LoadDataset",
    "LoadDatasetResult",
    "LoadDatasetService",
    "LoadOutcome",
    "PinRelease",
    "PinnedDataset",
    "PinnedRelease",
    "PreparedLoad",
    "ReleaseCatalog",
    "ShardDownloader",
    "ShardJob",
    "ShardJobs",
    "ShardOutcome",
    "TableLoader",
    "TransientError",
]
