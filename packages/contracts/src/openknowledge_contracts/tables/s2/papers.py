"""``papers``: core metadata, one record per corpus ID.

``externalids`` is a map because S2 adds identifier kinds without notice; its ``CorpusId`` is a
string. Inside ``authors`` the key is spelled ``authorId``, unlike ``authorid`` in the authors
dataset. ``publicationdate`` stays text: some papers predate every ClickHouse date type.
"""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import (
    list_of,
    optional,
    required,
    string_map,
    struct_of,
)

PAPERS = DatasetContract(
    name="papers",
    table="papers",
    key=("corpusid",),
    source=pa.schema(
        [
            required("corpusid", pa.uint64()),
            required("externalids", string_map(nullable_values=True)),
            required("url", pa.string()),
            required("title", pa.string()),
            required(
                "authors",
                list_of(
                    struct_of(optional("authorId", pa.string()), required("name", pa.string()))
                ),
            ),
            required("venue", pa.string()),
            optional("publicationvenueid", pa.string()),
            optional("year", pa.int16()),
            required("referencecount", pa.uint32()),
            required("citationcount", pa.uint32()),
            required("influentialcitationcount", pa.uint32()),
            required("isopenaccess", pa.bool_()),
            optional(
                "s2fieldsofstudy",
                list_of(
                    struct_of(required("category", pa.string()), required("source", pa.string()))
                ),
            ),
            optional("publicationtypes", list_of(pa.string())),
            optional("publicationdate", pa.string()),
            optional(
                "journal",
                struct_of(
                    optional("name", pa.string()),
                    optional("pages", pa.string()),
                    optional("volume", pa.string()),
                ),
            ),
        ]
    ),
)
