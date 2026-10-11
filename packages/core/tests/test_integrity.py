import base64
import hashlib
import zlib

import pytest

from openknowledge_core.ingestion.errors import GzipInvalid, IncompleteTransfer
from openknowledge_core.ingestion.integrity import MAX_OUTPUT_STEP, GzipCheck, StreamCheck
from tests.fakes.ports import gz


@pytest.mark.parametrize(
    "data",
    [
        gz(b"a" * 5000) + gz(b"b" * 5000),
        gz(b"a" * 5000) + b"\x00" * 13 + gz(b"b" * 5000) + b"\x00" * 7,
    ],
)
def test_gzip_check_accepts_whole_streams_in_any_chunking(data: bytes) -> None:
    for step in (1, 7, 4096, len(data)):
        check = GzipCheck()
        for start in range(0, len(data), step):
            check.update(data[start : start + step])
        check.close()
        assert check.members == 2


@pytest.mark.parametrize(
    "data",
    [
        b"",
        gz(b"abc")[:-1],
        gz(b"abc")[:5],
        b"not gzip at all",
        gz(b"abc") + b"trailing junk",
        b"\x00" + gz(b"abc"),
    ],
)
def test_gzip_check_rejects_incomplete_streams(data: bytes) -> None:
    check = GzipCheck()
    with pytest.raises(GzipInvalid):
        check.update(data)
        check.close()


def test_gzip_check_bounds_its_output_per_step() -> None:
    check = GzipCheck()
    check.update(gz(b"\x00" * (5 * MAX_OUTPUT_STEP)))  # five steps' output from one small update
    check.close()
    assert (check.members, check.uncompressed_size) == (1, 5 * MAX_OUTPUT_STEP)


def test_gzip_check_rejects_a_bad_crc() -> None:
    data = bytearray(gz(b"abc" * 100))
    data[-8] ^= 0xFF  # first byte of the CRC-32 trailer
    with pytest.raises(GzipInvalid, match="does not decode"):
        GzipCheck().update(bytes(data))
    with pytest.raises(zlib.error):  # plain zlib refuses the same bytes, so no leniency is added
        zlib.decompress(bytes(data), 16 + zlib.MAX_WBITS)


@pytest.mark.parametrize(
    ("text", "records"), [(b"a\nb\n", 2), (b"a\nb", 2), (b"", 0), (b"\n", 1), (b"x" * 9, 1)]
)
def test_records_are_lines_of_the_decompressed_text(text: bytes, records: int) -> None:
    check = GzipCheck()
    for byte in gz(text):
        check.update(bytes([byte]))
    check.close()
    assert (check.records, check.uncompressed_size) == (records, len(text))


def test_stream_check_reports_digests_and_counts() -> None:
    data = gz(b"x\n" * 1000)
    check = StreamCheck()
    for start in range(0, len(data), 100):
        check.update(data[start : start + 100])
    report = check.finish(len(data))
    assert report.sha256 == hashlib.sha256(data).hexdigest()
    assert report.md5 == base64.b64encode(hashlib.md5(data).digest()).decode()
    assert (report.size, report.gzip_members, report.records) == (len(data), 1, 1000)
    assert report.uncompressed_size == 2000
    assert check.report == report


def test_stream_check_compares_the_declared_length() -> None:
    check = StreamCheck()
    check.update(gz(b"x\n"))
    with pytest.raises(IncompleteTransfer, match="where 999 were due"):
        check.finish(999)
    assert check.report is None
