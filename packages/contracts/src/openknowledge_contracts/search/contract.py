"""A search contract: the fields of one document, and the index created to hold such documents.

A search document is a projection of a dataset's current rows, not a source record, so it has its
own fields. Each field names the type the index maps it as: ``english`` is text under the english
analyzer, which stems, and ``date`` a yyyy-MM-dd date whose malformed value leaves the field
unindexed but keeps the document. The contract's version goes into every document as
``contract_version``, so an index can say which projection built it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

type FieldType = Literal[
    "keyword", "text", "english", "integer", "long", "date", "boolean", "object"
]
type Json = dict[str, Any]

CONTRACT_VERSION = "contract_version"
# The types whose mapping is more than their name.
_MAPPINGS: dict[str, Json] = {
    "text": {"type": "text", "analyzer": "standard"},
    "english": {"type": "text", "analyzer": "english"},
    "date": {"type": "date", "format": "strict_date", "ignore_malformed": True},
}


@dataclass(frozen=True)
class SearchField:
    """One field of a document, mapped as ``type``.

    Attributes:
        name: The key in the document.
        type: The mapping type. An ``object`` field maps its ``fields`` and nothing else.
        fields: The fields of an object, in order.
        normalizer: The keyword normalizer for case-insensitive filters.
        subfields: Alternate scalar mappings of the same source value.
        dynamic: ``False`` for an object whose unlisted keys stay out of the index, as set on
            the object itself; ``None`` inherits the mapping's own setting.
    """

    name: str
    type: FieldType
    fields: tuple[SearchField, ...] = ()
    dynamic: bool | None = None
    normalizer: Literal["lowercase"] | None = None
    subfields: tuple[SearchField, ...] = ()

    def __post_init__(self) -> None:
        if (self.type == "object") != bool(self.fields):
            raise ValueError(f"{self.name}: only an object field has fields, and it needs some")
        if self.dynamic is not None and self.type != "object":
            raise ValueError(f"{self.name}: only an object field sets dynamic")
        if self.normalizer is not None and self.type != "keyword":
            raise ValueError(f"{self.name}: only a keyword field sets a normalizer")
        if self.subfields and self.type not in ("text", "english", "keyword"):
            raise ValueError(f"{self.name}: only text or keyword fields have subfields")
        if any(field.type == "object" or field.subfields for field in self.subfields):
            raise ValueError(f"{self.name}: subfields must be scalar without subfields")
        _distinct(self.name, self.subfields)
        _distinct(self.name, self.fields)


@dataclass(frozen=True)
class SearchContract:
    """One kind of search document and the index that holds its documents.

    Attributes:
        name: What the documents are, such as ``papers``.
        version: The projection's version, carried by every document.
        id_field: The keyword field whose value is also the document's id.
        fields: The document's fields, in order.
    """

    name: str
    version: int
    id_field: str
    fields: tuple[SearchField, ...]

    def __post_init__(self) -> None:
        if self.version < 1:
            raise ValueError(f"{self.name}: a contract version counts from 1")
        _distinct(self.name, self.fields)
        fields = {field.name: field for field in self.fields}
        identifier = fields.get(self.id_field)
        if identifier is None or identifier.type != "keyword":
            raise ValueError(f"{self.name}: the id field {self.id_field!r} must be a keyword field")
        version = fields.get(CONTRACT_VERSION)
        if version is None or version.type != "integer":
            raise ValueError(f"{self.name}: needs an integer {CONTRACT_VERSION} field")

    @property
    def names(self) -> frozenset[str]:
        """The top-level keys a document may have."""
        return frozenset(field.name for field in self.fields)


@dataclass(frozen=True)
class IndexBody:
    """What creating an index sends: its mapping and its settings."""

    mappings: Json
    settings: Json


def index_body(contract: SearchContract, *, shards: int) -> IndexBody:
    """The mapping of every contract field, and the settings of an index with ``shards`` shards.

    The mapping is closed: ``dynamic`` is false, so a key the contract does not name is kept in
    ``_source`` but never indexed. ``_source`` stays on, since documents are read back whole.
    The index has no replicas and compresses with zstd, as the bulk of it is stored text. It
    does not refresh on its own until settled, so a base build writes no small segment a second.
    """
    if shards < 1:
        raise ValueError(f"{contract.name}: an index needs at least one shard")
    return IndexBody(
        mappings={
            "dynamic": False,
            "_source": {"enabled": True},
            "properties": _properties(contract.fields),
        },
        settings={
            "index": {
                "number_of_shards": shards,
                "number_of_replicas": 0,
                "codec": "zstd",
                "refresh_interval": "-1",
                "analysis": {
                    "normalizer": {"lowercase": {"type": "custom", "filter": ["lowercase"]}}
                },
            },
        },
    )


def _properties(fields: tuple[SearchField, ...]) -> Json:
    properties: Json = {}
    for field in fields:
        if field.type != "object":
            properties[field.name] = dict(_MAPPINGS.get(field.type, {"type": field.type}))
            if field.normalizer is not None:
                properties[field.name]["normalizer"] = field.normalizer
            if field.subfields:
                properties[field.name]["fields"] = _properties(field.subfields)
            continue
        mapped: Json = {"type": "object"}
        if field.dynamic is not None:
            mapped["dynamic"] = field.dynamic
        mapped["properties"] = _properties(field.fields)
        properties[field.name] = mapped
    return properties


def _distinct(owner: str, fields: tuple[SearchField, ...]) -> None:
    names = [field.name for field in fields]
    if len(set(names)) < len(names):
        raise ValueError(f"{owner}: field names repeat")
