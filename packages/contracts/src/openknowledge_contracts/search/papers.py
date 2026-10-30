"""``papers``: one search document per live paper, with its abstract and TLDR when present.

The document is built from the current rows of the ``papers``, ``abstracts`` and ``tldrs`` tables.
Its counts are ``long`` because the tables hold them as ``UInt32``, which an ``integer`` field, a
signed 32-bit number, cannot take at its top. ``externalids`` lists the identifier kinds the
index knows; Semantic Scholar adds kinds without notice, and those stay out of the index. The
title, abstract and TLDR are stemmed English, so "networks" finds "network", and
``publicationdate`` is a date, so it takes ranges and sorts; changing it later means
building the index again.
"""

from openknowledge_contracts.search.contract import CONTRACT_VERSION, SearchContract, SearchField

PAPERS = SearchContract(
    name="papers",
    version=2,
    id_field="corpusid",
    fields=(
        SearchField("corpusid", "keyword"),
        SearchField("title", "english", subfields=(SearchField("exact", "text"),)),
        SearchField("abstract", "english"),
        SearchField("tldr", "english"),
        SearchField(
            "authors",
            "object",
            (SearchField("name", "text"), SearchField("authorId", "keyword")),
        ),
        SearchField("venue", "keyword", normalizer="lowercase"),
        SearchField("publicationvenueid", "keyword"),
        SearchField("year", "integer"),
        SearchField("publicationdate", "date"),
        SearchField(
            "journal",
            "object",
            (
                SearchField("name", "keyword", normalizer="lowercase"),
                SearchField("volume", "keyword"),
                SearchField("pages", "keyword"),
            ),
        ),
        SearchField(
            "externalids",
            "object",
            tuple(
                SearchField(kind, "keyword", normalizer="lowercase")
                for kind in ("DOI", "ArXiv", "PubMed", "PubMedCentral", "MAG", "ACL", "DBLP")
            ),
            dynamic=False,
        ),
        SearchField(
            "s2fieldsofstudy",
            "object",
            (SearchField("category", "keyword", normalizer="lowercase"),),
        ),
        SearchField("publicationtypes", "keyword", normalizer="lowercase"),
        SearchField("referencecount", "long"),
        SearchField("citationcount", "long"),
        SearchField("influentialcitationcount", "long"),
        SearchField("isopenaccess", "boolean"),
        SearchField("url", "keyword"),
        SearchField(CONTRACT_VERSION, "integer"),
    ),
)
