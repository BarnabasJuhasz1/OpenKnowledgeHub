"""``abstracts``: abstract text and open-access details, one record per corpus ID.

Its ``externalids`` use other key names than ``papers`` (``Medline``, ``MedRxiv``) and change
their case between records, which is why they are a map with the keys as delivered.
"""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import optional, required, string_map, struct_of

ABSTRACTS = DatasetContract(
    name="abstracts",
    table="abstracts",
    key=("corpusid",),
    source=pa.schema(
        [
            required("corpusid", pa.uint64()),
            required(
                "openaccessinfo",
                struct_of(
                    optional("disclaimer", pa.string()),
                    required("externalids", string_map(nullable_values=True)),
                    optional("license", pa.string()),
                    optional("url", pa.string()),
                    optional("status", pa.string()),
                ),
            ),
            required("abstract", pa.string()),
        ]
    ),
)
