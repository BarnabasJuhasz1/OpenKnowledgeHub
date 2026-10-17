import pytest

from openknowledge_contracts.tables.s2 import CONTRACTS
from openknowledge_core.ingestion.families import (
    ARCHIVE_DATASETS,
    ArchiveFamily,
    archive_family,
    require_family,
)


def test_archive_families_cover_exactly_the_supported_datasets() -> None:
    assert {
        name for name, family in ARCHIVE_DATASETS.items() if family == ArchiveFamily.CORE
    } == set(CONTRACTS)
    assert archive_family("s2orc_v2") == ArchiveFamily.FULLTEXT
    assert archive_family("embeddings-specter_v2") == ArchiveFamily.EMBEDDINGS
    for name, family in ARCHIVE_DATASETS.items():
        require_family(name, family)
        for other in ArchiveFamily:
            if other != family:
                with pytest.raises(ValueError, match="belongs to"):
                    require_family(name, other)


@pytest.mark.parametrize("dataset", ["s2orc", "embeddings-specter_v1", "unknown"])
def test_legacy_and_unknown_datasets_are_not_managed(dataset: str) -> None:
    with pytest.raises(ValueError, match="Unsupported archive dataset"):
        archive_family(dataset)
