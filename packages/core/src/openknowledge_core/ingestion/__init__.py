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
    LoadDiff,
    LoadDiffResult,
    PinnedRelease,
    PinRelease,
    WatchdogResult,
)
from openknowledge_core.ingestion.errors import IngestionError, TransientError
from openknowledge_core.ingestion.load import LoadDatasetService
from openknowledge_core.ingestion.models import (
    JobOutcome,
    LoadOutcome,
    PinnedDataset,
    PinnedDiff,
    PreparedDiff,
    PreparedLoad,
    ShardJob,
    ShardOutcome,
    TableFreshness,
)
from openknowledge_core.ingestion.ports import (
    AppliedReleases,
    ArtifactStore,
    ReleaseCatalog,
    ShardDownloader,
    ShardJobs,
    TableLoader,
)
from openknowledge_core.ingestion.service import DownloadDatasetService
from openknowledge_core.ingestion.watchdog import WatchdogService

__all__ = [
    "AppliedReleases",
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
    "LoadDiff",
    "LoadDiffResult",
    "LoadOutcome",
    "PinRelease",
    "PinnedDataset",
    "PinnedDiff",
    "PinnedRelease",
    "PreparedDiff",
    "PreparedLoad",
    "ReleaseCatalog",
    "ShardDownloader",
    "ShardJob",
    "ShardJobs",
    "ShardOutcome",
    "TableFreshness",
    "TableLoader",
    "TransientError",
    "WatchdogResult",
    "WatchdogService",
]
