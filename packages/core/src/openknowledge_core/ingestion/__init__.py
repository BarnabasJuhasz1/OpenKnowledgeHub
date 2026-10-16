"""Archiving Semantic Scholar datasets into the raw bucket."""

from openknowledge_core.ingestion.commands import (
    BatchDiffDownloadResult,
    BatchDownloadResult,
    DownloadDataset,
    DownloadDatasetResult,
    DownloadDiff,
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
    PinnedDiff,
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
    "BatchDiffDownloadResult",
    "BatchDownloadResult",
    "DownloadDataset",
    "DownloadDatasetResult",
    "DownloadDatasetService",
    "DownloadDiff",
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
    "PinnedDiff",
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
