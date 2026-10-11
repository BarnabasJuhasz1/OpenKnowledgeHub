"""Checks on every byte of a shard as it streams: digests, counts and the gzip framing."""

from __future__ import annotations

import base64
import hashlib
import zlib

from pydantic import BaseModel, ConfigDict, NonNegativeInt, PositiveInt

from openknowledge_contracts.manifests import Md5Base64, Sha256Hex
from openknowledge_core.ingestion.errors import GzipInvalid, IncompleteTransfer

MAX_OUTPUT_STEP = 8 * 1024 * 1024


class IntegrityReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    size: NonNegativeInt
    sha256: Sha256Hex
    md5: Md5Base64
    gzip_members: PositiveInt
    records: NonNegativeInt
    uncompressed_size: NonNegativeInt


class GzipCheck:
    """``close`` fails unless every member ended, when zlib checks its CRC-32 and length."""

    def __init__(self) -> None:
        self.members = 0
        self.uncompressed_size = 0
        self._newlines = 0
        self._ends_with_newline = True
        self._start_member()

    def _start_member(self) -> None:
        self._z = zlib.decompressobj(16 + zlib.MAX_WBITS)
        self._open = False

    @property
    def records(self) -> int:
        """Lines of the decompressed text, counting a last line that has no newline."""
        return self._newlines + (0 if self._ends_with_newline else 1)

    def update(self, data: bytes) -> None:
        """Feed the next bytes, which may span any number of members."""
        while data:
            if not self._open and self.members:
                data = data.lstrip(b"\x00")  # gzip tools accept zero padding after a member
                if not data:
                    return
            self._open = True
            try:
                out = self._z.decompress(data, MAX_OUTPUT_STEP)
            except zlib.error as exc:
                raise GzipInvalid(f"the gzip stream does not decode: {exc}") from None
            if out:
                self.uncompressed_size += len(out)
                self._newlines += out.count(b"\n")
                self._ends_with_newline = out.endswith(b"\n")
            if self._z.eof:
                self.members += 1
                data = self._z.unused_data
                self._start_member()
            else:
                data = self._z.unconsumed_tail

    def close(self) -> None:
        if self.members == 0 or self._open:
            raise GzipInvalid("the gzip stream ends before its last member does")


class StreamCheck:
    def __init__(self) -> None:
        self._sha256 = hashlib.sha256()
        self._md5 = hashlib.md5(usedforsecurity=False)
        self._size = 0
        self._gzip = GzipCheck()
        self.report: IntegrityReport | None = None

    def update(self, data: bytes) -> None:
        self._gzip.update(data)
        self._sha256.update(data)
        self._md5.update(data)
        self._size += len(data)

    def finish(self, expected_size: int) -> IntegrityReport:
        self._gzip.close()
        if self._size != expected_size:
            raise IncompleteTransfer(f"received {self._size} bytes where {expected_size} were due")
        self.report = IntegrityReport(
            size=self._size,
            sha256=self._sha256.hexdigest(),
            md5=base64.b64encode(self._md5.digest()).decode(),
            gzip_members=self._gzip.members,
            records=self._gzip.records,
            uncompressed_size=self._gzip.uncompressed_size,
        )
        return self.report
