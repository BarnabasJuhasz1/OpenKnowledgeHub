from collections.abc import AsyncGenerator
from typing import Protocol

from openknowledge_contracts.embedding_artifacts import ConvertedShard, EmbeddingArtifactRow
from openknowledge_core.embedding_conversion.models import (
    ConversionJob,
    ConversionPlan,
    ConversionSource,
)
from openknowledge_core.ingestion.models import JobOutcome


class ParquetArtifacts(Protocol):
    async def convert(
        self, plan: ConversionPlan, shard: int, source: ConversionSource
    ) -> ConvertedShard: ...

    async def verify(self, artifact: ConvertedShard) -> None: ...

    async def read(self, artifact: ConvertedShard) -> AsyncGenerator[EmbeddingArtifactRow]:
        # Yield makes this an async iterator protocol, rather than a coroutine returning one.
        ...
        yield EmbeddingArtifactRow(corpusid=0, source_line=1)


class ConversionJobs(Protocol):
    async def conversion_running(self) -> tuple[str, ...]: ...

    async def submit(self, job: ConversionJob) -> str: ...

    async def wait(self, name: str) -> JobOutcome: ...
