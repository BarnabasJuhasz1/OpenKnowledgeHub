"""``paper-ids``: SHA-based paper IDs and the corpus ID each points to.

A corpus ID has several SHAs, exactly one of them ``primary``. The SHA is S2's alias, not a
hash of any PDF, and stays text until a normalised column checks it.
"""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import required

PAPER_IDS = DatasetContract(
    name="paper-ids",
    table="paper_ids",
    key=("sha",),
    source=pa.schema(
        [
            required("sha", pa.string()),
            required("corpusid", pa.uint64()),
            required("primary", pa.bool_()),
        ]
    ),
)
