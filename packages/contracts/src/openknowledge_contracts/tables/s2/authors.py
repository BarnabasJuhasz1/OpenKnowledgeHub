"""``authors``: author summaries, one record per author ID.

``authorid`` looks numeric but is text at the source and stays text. ``externalids`` was null in
every record seen; S2's API documents a DBLP name list and an ORCID in it. Until real records
fix its shape it is kept as JSON text, so no value is lost or refused meanwhile.
"""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import list_of, optional, required

AUTHORS = DatasetContract(
    name="authors",
    table="authors",
    key=("authorid",),
    source=pa.schema(
        [
            required("authorid", pa.string()),
            optional("externalids", pa.json_()),
            required("url", pa.string()),
            required("name", pa.string()),
            optional("aliases", list_of(pa.string())),
            optional("affiliations", list_of(pa.string())),
            optional("homepage", pa.string()),
            required("papercount", pa.uint32()),
            required("citationcount", pa.uint32()),
            required("hindex", pa.uint32()),
        ]
    ),
)
