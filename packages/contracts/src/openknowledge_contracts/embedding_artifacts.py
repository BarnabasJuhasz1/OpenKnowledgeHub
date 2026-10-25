"""Raw, ordered embeddings artifacts; these do not represent resolved snapshots."""

from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, StrictInt, model_validator

from openknowledge_contracts.datasets import ArchiveDocument, ReleaseId
from openknowledge_contracts.manifests import ObjectRef, Sha256Hex, ShardReceipt


class ParquetReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, hide_input_in_errors=True)
    format_version: Literal[1] = 1
    dtype: Literal["float32"] = "float32"
    dimension: Literal[768] = 768
    operation: Literal["base-shard", "updates", "deletes"] = "base-shard"
    source: ShardReceipt
    batch_rows: int = Field(ge=1, le=1024)
    max_record_bytes: int = Field(ge=1, le=8 * 1024 * 1024)
    rows: int = Field(ge=0)
    parquet_bytes: int = Field(gt=0)
    parquet_sha256: Sha256Hex
    max_absolute_error: float = Field(ge=0, allow_inf_nan=False)
    elapsed_seconds: float = Field(ge=0, allow_inf_nan=False)


class EmbeddingInput(ArchiveDocument):
    kind: Literal["base", "updates", "deletes"]
    from_release: ReleaseId | None = None
    release: ReleaseId
    completion: ObjectRef
    completion_sha256: Sha256Hex
    manifest: ObjectRef | None
    manifest_sha256: Sha256Hex | None

    @model_validator(mode="after")
    def _identity(self) -> Self:
        if (self.manifest is None) != (self.manifest_sha256 is None):
            raise ValueError("manifest reference and hash must be supplied together")
        if self.kind == "base":
            if self.from_release is not None or self.manifest is None:
                raise ValueError("base requires a manifest and no predecessor release")
        elif self.from_release is None or self.from_release >= self.release:
            raise ValueError("diff part requires an ordered transition")
        return self


class ConvertedShard(ArchiveDocument):
    source_receipt: ObjectRef
    source_receipt_sha256: Sha256Hex
    output: ObjectRef
    receipt: ParquetReceipt


class ConversionManifest(ArchiveDocument):
    format_version: Literal[1] = 1
    semantics: Literal["raw-source-order-duplicates-unresolved"] = (
        "raw-source-order-duplicates-unresolved"
    )
    source: EmbeddingInput
    entries: tuple[ConvertedShard, ...]


class EmbeddingArtifactRow(BaseModel):
    """One raw row. The descriptor's operation distinguishes upserts and deletes."""

    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", hide_input_in_errors=True)
    corpusid: Annotated[StrictInt, Field(ge=0, le=2**64 - 1)]
    source_line: Annotated[StrictInt, Field(ge=1, le=2**64 - 1)]
    vector: Annotated[list[FiniteFloat], Field(min_length=768, max_length=768)] | None = None
