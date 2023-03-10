import pytest

from celus_nigiri.record import TitleIds


@pytest.fixture
def full_titleids():
    return TitleIds(
        DOI="https://doi.org/10.1000/182",
        ISBN="0-19-853453-1",
        Print_ISSN="1234-5678",
        Online_ISSN="0123-4567",
        Proprietary="Proprietary1",
        URI="https://example.org",
    )


@pytest.fixture
def empty_titleids():
    return TitleIds()


def test_titleids_getitem(full_titleids, empty_titleids):
    # test in empty
    for key in ["DOI", "ISBN", "Print_ISSN", "Online_ISSN", "Proprietary", "URI"]:
        with pytest.raises(KeyError):
            empty_titleids[key]

    # test with full
    for key in ["DOI", "ISBN", "Print_ISSN", "Online_ISSN", "Proprietary", "URI"]:
        full_titleids[key]

    # test with non-existing
    with pytest.raises(KeyError):
        full_titleids["unknown"]

    # Test partial
    title_ids = TitleIds(ISBN="978-3-16-148410-0")
    assert title_ids["ISBN"] == "978-3-16-148410-0"
    with pytest.raises(KeyError):
        title_ids["DOI"]


def test_titleids_setitem():
    title_ids = TitleIds()
    # Set all values
    for key in ["DOI", "ISBN", "Print_ISSN", "Online_ISSN", "Proprietary", "URI"]:
        title_ids[key] = "ID"
        assert title_ids[key] == "ID"

    with pytest.raises(KeyError):
        title_ids["unknown"] = "ID"

    # Unset all values
    for key in ["DOI", "ISBN", "Print_ISSN", "Online_ISSN", "Proprietary", "URI"]:
        title_ids[key] = None
        with pytest.raises(KeyError):
            title_ids[key]


def test_titleids_keys(full_titleids, empty_titleids):
    assert full_titleids.keys() == [
        "DOI",
        "ISBN",
        "Print_ISSN",
        "Online_ISSN",
        "Proprietary",
        "URI",
    ]
    assert empty_titleids.keys() == []

    title_ids = TitleIds(ISBN="978-3-16-148410-0")
    assert title_ids.keys() == ["ISBN"]


def test_titleids_items(full_titleids, empty_titleids):
    assert full_titleids.items() == [
        ("DOI", "https://doi.org/10.1000/182"),
        ("ISBN", "0-19-853453-1"),
        ("Print_ISSN", "1234-5678"),
        ("Online_ISSN", "0123-4567"),
        ("Proprietary", "Proprietary1"),
        ("URI", "https://example.org"),
    ]
    assert empty_titleids.items() == []

    title_ids = TitleIds(ISBN="978-3-16-148410-0")
    assert title_ids.items() == [("ISBN", "978-3-16-148410-0")]
