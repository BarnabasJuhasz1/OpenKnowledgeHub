"""The search source's side of ClickHouse: the latest ``papers`` and ``abstracts`` row per key.

The base query reads ``papers`` with ``FINAL``, which keeps one row per key and hides the
tombstones, since a deleted paper has no document. It reads ``abstracts`` as the diff query reads
both tables: the latest row per key by ``version``, tombstones included, because an abstract
tombstone's version is part of its document's version, and the base and diff passes must give
one table state one version. The diff query takes the keys that a release after ``from_release``
touched in either table. Both read ``version``, ``source_release`` and ``is_deleted`` next to the
fields a document needs, and pair the rows of the two tables by corpus ID in the query, whose
rows stream in rather than load whole. A part's range is cut, as ``CorpusSlice`` says, from the
smallest and largest key at the release, which ``bounds`` reads, so parts computed days apart
tile the same keys whatever was loaded in between.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import aclosing
from typing import Any

from pydantic import ValidationError

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_contracts.datasets import ReleaseId
from openknowledge_core.ingestion.errors import TableRejected
from openknowledge_core.search.models import AbstractRow, PaperRow, SourceRows

PAPER_FIELDS = (
    "externalids",
    "url",
    "title",
    "authors",
    "venue",
    "publicationvenueid",
    "year",
    "referencecount",
    "citationcount",
    "influentialcitationcount",
    "isopenaccess",
    "s2fieldsofstudy",
    "publicationtypes",
    "publicationdate",
    "journal_name",
)
_PAPER_COLUMNS = (
    "corpusid, version, source_release, is_deleted, externalids, url, title, authors, venue, "
    "publicationvenueid, year, referencecount, citationcount, influentialcitationcount, "
    "isopenaccess, arrayMap(f -> f.category, s2fieldsofstudy) AS s2fieldsofstudy, "
    "publicationtypes, publicationdate, journal.name AS journal_name"
)
_ABSTRACT_COLUMNS = "corpusid, version, source_release, is_deleted, abstract"
_RANGE = "corpusid BETWEEN {low:UInt64} AND {high:UInt64}"
_AT = "source_release <= {release:Date}"
_AFTER = "source_release > {from_release:Date}"
_PROJECTION = ", ".join(
    [
        "p.version AS paper_version",
        "p.source_release AS paper_release",
        "p.is_deleted AS paper_deleted",
        *(f"p.{name} AS {name}" for name in PAPER_FIELDS),
        "a.version AS abstract_version",
        "a.source_release AS abstract_release",
        "a.is_deleted AS abstract_deleted",
        "a.abstract AS abstract",
    ]
)
# Unmatched abstracts come back null, and named tuples as objects, whatever the server's defaults.
_SETTINGS = "SETTINGS join_use_nulls = 1, output_format_json_named_tuples_as_objects = 1"

_LATEST = "ORDER BY corpusid, version DESC\n    LIMIT 1 BY corpusid"

BOUNDS_QUERY = f"SELECT min(corpusid) AS low, max(corpusid) AS high FROM papers WHERE {_AT}"
BASE_QUERY = f"""
SELECT p.corpusid AS corpusid, {_PROJECTION}
FROM (
    SELECT {_PAPER_COLUMNS} FROM papers FINAL
    WHERE {_RANGE} AND is_deleted = 0 AND {_AT}
) AS p
LEFT JOIN (
    SELECT {_ABSTRACT_COLUMNS} FROM abstracts
    WHERE {_RANGE} AND {_AT}
    {_LATEST}
) AS a ON a.corpusid = p.corpusid
{_SETTINGS}"""
DIFF_QUERY = f"""
WITH touched AS (
    SELECT corpusid FROM papers WHERE {_RANGE} AND {_AFTER} AND {_AT}
    UNION DISTINCT
    SELECT corpusid FROM abstracts WHERE {_RANGE} AND {_AFTER} AND {_AT}
)
SELECT t.corpusid AS corpusid, {_PROJECTION}
FROM touched AS t
LEFT JOIN (
    SELECT {_PAPER_COLUMNS} FROM papers
    WHERE {_RANGE} AND {_AT} AND corpusid IN (SELECT corpusid FROM touched)
    {_LATEST}
) AS p ON p.corpusid = t.corpusid
LEFT JOIN (
    SELECT {_ABSTRACT_COLUMNS} FROM abstracts
    WHERE {_RANGE} AND {_AT} AND corpusid IN (SELECT corpusid FROM touched)
    {_LATEST}
) AS a ON a.corpusid = t.corpusid
{_SETTINGS}"""


class ClickHouseSearchSource:
    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client

    async def bounds(self, release: ReleaseId) -> tuple[int, int]:
        rows = await self._client.rows(BOUNDS_QUERY, release=release)
        return int(rows[0]["low"]), int(rows[0]["high"])

    async def base(self, release: ReleaseId, low: int, high: int) -> AsyncGenerator[SourceRows]:
        return self._rows(BASE_QUERY, low, high, release=release)

    async def diff(
        self, from_release: ReleaseId, release: ReleaseId, low: int, high: int
    ) -> AsyncGenerator[SourceRows]:
        return self._rows(DIFF_QUERY, low, high, release=release, from_release=from_release)

    async def _rows(
        self, query: str, low: int, high: int, **parameters: str
    ) -> AsyncGenerator[SourceRows]:
        """The query's rows over the range; none, and no query, for an empty part."""
        if low > high:
            return
        rows = self._client.stream(query, low=str(low), high=str(high), **parameters)
        async with aclosing(rows):
            async for row in rows:
                try:
                    yield source_rows(row)
                except ValidationError as exc:
                    fields = ", ".join(
                        ".".join(str(part) for part in error["loc"])
                        for error in exc.errors(include_input=False, include_url=False)
                    )
                    raise TableRejected(
                        f"corpusid {row.get('corpusid')}: {fields} do not fit a search row"
                    ) from None


def source_rows(row: dict[str, Any]) -> SourceRows:
    """One joined row as the queries project it, where a null version means no row."""
    paper = None
    if row["paper_version"] is not None:
        paper = PaperRow.model_validate(
            {
                **{name: row[name] for name in PAPER_FIELDS},
                "corpusid": row["corpusid"],
                "version": row["paper_version"],
                "source_release": row["paper_release"],
                "is_deleted": row["paper_deleted"],
            }
        )
    abstract = None
    if row["abstract_version"] is not None:
        abstract = AbstractRow(
            corpusid=row["corpusid"],
            version=row["abstract_version"],
            source_release=row["abstract_release"],
            is_deleted=row["abstract_deleted"],
            abstract=row["abstract"],
        )
    return SourceRows(corpusid=row["corpusid"], paper=paper, abstract=abstract)
