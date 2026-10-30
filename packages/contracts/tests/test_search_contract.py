from typing import Any

import pytest

from openknowledge_contracts.search import PAPERS, SearchField, index_body

# The mapping of each type that is not mapped as its own name.
MAPPED = {
    "keyword": {"type": "keyword", "ignore_above": 8191},
    "text": {"type": "text", "analyzer": "standard"},
    "english": {"type": "text", "analyzer": "english"},
    "date": {"type": "date", "format": "strict_date", "ignore_malformed": True},
}


def mapped(fields: tuple[SearchField, ...], properties: dict[str, Any]) -> None:
    assert set(properties) == {field.name for field in fields}
    for field in fields:
        if field.type == "object":
            assert properties[field.name]["type"] == "object"
            mapped(field.fields, properties[field.name]["properties"])
        else:
            expected: dict[str, Any] = dict(MAPPED.get(field.type, {"type": field.type}))
            if field.normalizer:
                expected["normalizer"] = field.normalizer
            if field.subfields:
                expected["fields"] = {"exact": {"type": "text", "analyzer": "standard"}}
            assert properties[field.name] == expected


def test_every_field_is_mapped_and_the_contract_is_at_version_2() -> None:
    assert PAPERS.version == 2
    body = index_body(PAPERS, shards=3)
    properties = body.mappings["properties"]
    mapped(PAPERS.fields, properties)
    assert (body.mappings["dynamic"], body.mappings["_source"]) == (False, {"enabled": True})
    assert properties["externalids"]["dynamic"] is False
    assert properties["title"]["fields"] == {"exact": {"type": "text", "analyzer": "standard"}}
    assert properties["tldr"] == {"type": "text", "analyzer": "english"}
    assert properties["venue"]["normalizer"] == "lowercase"
    assert properties["journal"]["properties"] == {
        "name": {"type": "keyword", "ignore_above": 8191, "normalizer": "lowercase"},
        "volume": {"type": "keyword", "ignore_above": 8191},
        "pages": {"type": "keyword", "ignore_above": 8191},
    }
    assert all(
        field["normalizer"] == "lowercase"
        for field in properties["externalids"]["properties"].values()
    )
    assert properties["s2fieldsofstudy"]["properties"]["category"]["normalizer"] == "lowercase"
    assert properties["publicationtypes"]["normalizer"] == "lowercase"
    assert all(
        properties[name] == {"type": "keyword", "ignore_above": 8191}
        for name in ("url", "publicationvenueid", "corpusid")
    )
    assert properties["authors"]["properties"]["authorId"] == MAPPED["keyword"]
    assert properties[PAPERS.id_field] == MAPPED["keyword"]
    assert properties["contract_version"] == {"type": "integer"}
    assert body.settings == {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 0,
            "codec": "zstd",
            "refresh_interval": "-1",
            "analysis": {"normalizer": {"lowercase": {"type": "custom", "filter": ["lowercase"]}}},
        }
    }


def test_invalid_field_options_are_rejected() -> None:
    with pytest.raises(ValueError, match="normalizer"):
        SearchField("invalid", "text", normalizer="lowercase")
    with pytest.raises(ValueError, match="scalar"):
        SearchField(
            "invalid", "text", subfields=(SearchField("a", "object", (SearchField("b", "text"),)),)
        )
    with pytest.raises(ValueError, match="repeat"):
        SearchField(
            "invalid", "text", subfields=(SearchField("a", "text"), SearchField("a", "text"))
        )
