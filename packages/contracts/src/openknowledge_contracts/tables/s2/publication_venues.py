"""``publication-venues``: journals and conferences, keyed by S2's UUID text.

Papers point here through ``publicationvenueid``. The dataset has no diffs, only full rewrites,
and those carry no deletes: a venue missing from a later release keeps its row until the table
is replaced.
"""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import list_of, optional, required

PUBLICATION_VENUES = DatasetContract(
    name="publication-venues",
    table="publication_venues",
    key=("id",),
    source=pa.schema(
        [
            required("id", pa.string()),
            required("name", pa.string()),
            optional("type", pa.string()),
            optional("issn", pa.string()),
            required("alternate_issns", list_of(pa.string())),
            required("alternate_names", list_of(pa.string())),
            optional("url", pa.string()),
            required("alternate_urls", list_of(pa.string())),
        ]
    ),
)
