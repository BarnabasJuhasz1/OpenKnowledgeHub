import math
import struct
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StrictInt, ValidationError

from openknowledge_core.ingestion.consumers import EmbeddingPaper
from openknowledge_core.ingestion.errors import IngestionError


def float32_vector(paper: EmbeddingPaper) -> tuple[list[float], float]:
    if paper.corpusid > 2**64 - 1:
        raise ValueError("unsupported corpusid")
    try:
        rounded = [struct.unpack("f", struct.pack("f", value))[0] for value in paper.vector]
    except OverflowError:
        raise ValueError("vector overflows float32") from None
    if not all(math.isfinite(value) for value in rounded):
        raise ValueError("vector overflows float32")
    return rounded, max(abs(a - b) for a, b in zip(paper.vector, rounded, strict=True))


class DeletePaper(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", hide_input_in_errors=True)
    corpusid: Annotated[StrictInt, Field(ge=0, le=2**64 - 1)]


def parse_delete(line: bytes) -> DeletePaper:
    try:
        return DeletePaper.model_validate_json(line)
    except ValidationError:
        raise IngestionError("invalid embedding delete; expected only a UInt64 corpusid") from None
