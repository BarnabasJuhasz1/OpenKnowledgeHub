"""The Semantic Scholar client against the fake API."""

from __future__ import annotations

import httpx
import pytest

from openknowledge_adapters.semantic_scholar import FileLink, S2Error, SemanticScholarClient
from openknowledge_adapters.semantic_scholar.client import BASE_URL
from tests.conftest import KEY, FakeClock, FakeS2


def test_lists_releases_oldest_first(s2: FakeS2, s2_client: SemanticScholarClient) -> None:
    s2.releases = ["2026-10-06", "2026-09-29"]
    assert s2_client.list_releases() == ["2026-09-29", "2026-10-06"]


def test_dataset_files_take_their_name_from_the_path(
    s2: FakeS2, s2_client: SemanticScholarClient
) -> None:
    s2.add("papers", "part-0.gz", b"")
    s2.add("papers", "part-1.gz", b"")
    links = s2_client.dataset_files("2026-10-06", "papers")
    assert [link.name for link in links] == ["part-0.gz", "part-1.gz"]
    assert links[0].url.startswith("https://files.test/staging/2026-10-06/papers/part-0.gz?")


def test_diffs(s2_client: SemanticScholarClient) -> None:
    [diff] = s2_client.diffs("2026-09-29", "2026-10-06", "papers")
    assert (diff.from_release, diff.to_release) == ("2026-09-29", "2026-10-06")
    assert [link.name for link in diff.update_files] == ["update-1.gz"]
    assert [link.name for link in diff.delete_files] == ["delete-1.gz"]


def test_key_comes_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, s2: FakeS2, clock: FakeClock
) -> None:
    http = httpx.Client(base_url=BASE_URL, transport=s2.transport)
    monkeypatch.delenv("S2_API_KEY", raising=False)
    with pytest.raises(S2Error, match="S2_API_KEY"):
        SemanticScholarClient(http=http, clock=clock, sleep=clock.sleep)
    monkeypatch.setenv("S2_API_KEY", KEY)
    client = SemanticScholarClient(http=http, clock=clock, sleep=clock.sleep)
    assert client.list_releases() == s2.releases


def test_rejected_key_is_not_retried_and_never_shown(s2: FakeS2, clock: FakeClock) -> None:
    http = httpx.Client(base_url=BASE_URL, transport=s2.transport)
    client = SemanticScholarClient("wrong-key", http=http, clock=clock, sleep=clock.sleep)
    with pytest.raises(S2Error) as info:
        client.list_releases()
    assert "wrong-key" not in str(info.value)
    assert "HTTP 403" in str(info.value)
    assert len(s2.api_calls) == 1


def test_unknown_dataset_is_a_plain_error(s2_client: SemanticScholarClient) -> None:
    with pytest.raises(S2Error) as info:
        s2_client.dataset_files("2026-10-06", "nothing")
    assert str(info.value) == "GET release/2026-10-06/dataset/nothing: HTTP 404"


def test_one_request_per_second(s2_client: SemanticScholarClient, clock: FakeClock) -> None:
    s2_client.list_releases()
    s2_client.list_releases()
    s2_client.list_releases()
    assert clock.sleeps == [1.0, 1.0]


def test_retries_with_backoff(
    s2: FakeS2, s2_client: SemanticScholarClient, clock: FakeClock
) -> None:
    s2.fail_next = [429, httpx.ConnectError("reset"), 503]
    assert s2_client.list_releases() == s2.releases
    assert clock.sleeps == [1.0, 2.0, 4.0]
    assert len(s2.api_calls) == 4


def test_gives_up_after_about_two_minutes(
    s2: FakeS2, s2_client: SemanticScholarClient, clock: FakeClock
) -> None:
    s2.fail_next = [429] * 8
    with pytest.raises(S2Error, match="HTTP 429 after 8 attempts"):
        s2_client.list_releases()
    assert len(s2.api_calls) == 8
    assert clock.sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0]
    assert sum(clock.sleeps) == 127.0


def test_file_link_repr_hides_the_signed_url() -> None:
    link = FileLink.from_url("https://files.test/staging/r/d/part-0.gz?Signature=secret")
    assert link.name == "part-0.gz"
    assert "secret" not in repr(link)
    assert "secret" not in str(link)


@pytest.mark.parametrize(
    "url",
    [
        "http://files.test/staging/r/d/part-0.gz?x=1",
        "https://files.test/staging/r/d/",
        "https://files.test/staging/r/d/..",
        "https://files.test/staging/r/d/part\u200b0.gz",
        "https://files.test/staging/r/d/.hidden.gz",
        "https://files.test/staging/r/d/part 0.gz",
        "https://files.test/staging/r/d/part\n0.gz?Signature=s",
    ],
)
def test_links_must_be_https_with_a_plain_file_name(url: str) -> None:
    with pytest.raises(S2Error, match="plain file name"):
        FileLink.from_url(url)


def test_ids_are_quoted_into_the_path(s2: FakeS2, s2_client: SemanticScholarClient) -> None:
    with pytest.raises(S2Error, match="HTTP 404"):
        s2_client.dataset_files("2026-10-06/../x", "papers?fields=all")
    assert s2.api_calls == ["release/2026-10-06%2F..%2Fx/dataset/papers%3Ffields%3Dall"]


@pytest.mark.parametrize("release", ["", ".", ".."])
def test_dots_and_empty_ids_are_refused(release: str, s2_client: SemanticScholarClient) -> None:
    with pytest.raises(S2Error, match="cannot be empty or dots"):
        s2_client.dataset_files(release, "papers")


def test_a_body_that_is_not_json_is_an_error(s2: FakeS2, s2_client: SemanticScholarClient) -> None:
    s2.fail_next = [httpx.Response(200, text="<html>maintenance</html>")]
    with pytest.raises(S2Error, match="not JSON"):
        s2_client.list_releases()


def test_retry_after_is_honoured(
    s2: FakeS2, s2_client: SemanticScholarClient, clock: FakeClock
) -> None:
    s2.fail_next = [httpx.Response(429, headers={"retry-after": "7"})]
    s2_client.list_releases()
    assert clock.sleeps == [7.0]


def test_at_least_one_attempt(s2: FakeS2) -> None:
    http = httpx.Client(base_url=BASE_URL, transport=s2.transport)
    with pytest.raises(ValueError, match="attempts"):
        SemanticScholarClient(KEY, http=http, attempts=0)
