"""``tldrs``: one generated summary per corpus ID and the model that wrote it."""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import optional, required

TLDRS = DatasetContract(
    name="tldrs",
    table="tldrs",
    key=("corpusid",),
    source=pa.schema(
        [
            required("corpusid", pa.uint64()),
            required("model", pa.string()),
            optional("text", pa.string()),
        ]
    ),
)
