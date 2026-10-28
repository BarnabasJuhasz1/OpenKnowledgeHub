from typing import Literal, Self

from pydantic import Field, model_validator

from openknowledge_contracts.datasets import ArchiveDocument, DatasetName, ReleaseId
from openknowledge_contracts.embedding_artifacts import EmbeddingInput
from openknowledge_contracts.manifests import ShardReceipt
from openknowledge_core.ingestion.archive_progress import HashedRef
from openknowledge_core.ingestion.commands import DownloadDataset
from openknowledge_core.ingestion.models import ShardJob


class ConvertEmbeddings(DownloadDataset):
    dataset: DatasetName = "embeddings-specter_v2"
    from_release: ReleaseId | None = None
    part: Literal["updates", "deletes"] | None = None
    completion: HashedRef | None = None
    batch_rows: int = Field(default=128, ge=1, le=1024)
    max_record_bytes: int = Field(default=1048576, ge=1, le=8388608)

    @model_validator(mode="after")
    def _part(self) -> Self:
        if self.dataset != "embeddings-specter_v2":
            raise ValueError("conversion requires embeddings-specter_v2")
        if self.from_release is not None and self.from_release >= self.release:
            raise ValueError("from_release must precede release")
        if (self.part is None) != (self.from_release is None):
            raise ValueError("from_release and part must be supplied together")
        if self.part is not None and self.completion is None:
            raise ValueError("diff conversion requires an explicit pinned completion")
        return self


class ConversionSource(ArchiveDocument):
    reference: HashedRef
    receipt: ShardReceipt


class ConversionPlan(ArchiveDocument):
    format_version: Literal[2] = 2
    command: ConvertEmbeddings
    source: EmbeddingInput
    shards: tuple[HashedRef, ...]


class ConversionJob(ShardJob):
    plan: HashedRef
