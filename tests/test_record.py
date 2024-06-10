import pytest

from celus_nigiri.record import Identifiers


@pytest.fixture
def full_identifiers():
    return Identifiers(
        DOI="10.1000/182",
        ISBN="0-19-853453-1",
        Print_ISSN="1234-5678",
        Online_ISSN="0123-4567",
        Proprietary="Proprietary1",
        URI="https://example.org",
    )


@pytest.fixture
def empty_identifiers():
    return Identifiers()


def test_identifiers_bool(empty_identifiers, full_identifiers):
    assert bool(empty_identifiers) is False, "empty => False"
    assert bool(full_identifiers) is True, "all set => True"
    assert bool(Identifiers(Print_ISSN="1234-5678")) is True, "one set => True"


def test_identifiers_getitem(full_identifiers, empty_identifiers):
    # test in empty
    for key in ["DOI", "ISBN", "Print_ISSN", "Online_ISSN", "Proprietary", "URI"]:
        with pytest.raises(KeyError):
            empty_identifiers[key]

    # test with full
    for key in ["DOI", "ISBN", "Print_ISSN", "Online_ISSN", "Proprietary", "URI"]:
        full_identifiers[key]

    # test with non-existing
    with pytest.raises(KeyError):
        full_identifiers["unknown"]

    # Test partial
    identifiers = Identifiers(ISBN="978-3-16-148410-0")
    assert identifiers["ISBN"] == "978-3-16-148410-0"
    with pytest.raises(KeyError):
        identifiers["DOI"]


def test_identifiers_setitem():
    identifiers = Identifiers()
    # Set all values
    for key in ["DOI", "ISBN", "Print_ISSN", "Online_ISSN", "Proprietary", "URI"]:
        identifiers[key] = "ID"
        assert identifiers[key] == "ID"

    with pytest.raises(KeyError):
        identifiers["unknown"] = "ID"

    # Unset all values
    for key in ["DOI", "ISBN", "Print_ISSN", "Online_ISSN", "Proprietary", "URI"]:
        identifiers[key] = None
        with pytest.raises(KeyError):
            identifiers[key]


def test_identifiers_keys(full_identifiers, empty_identifiers):
    assert full_identifiers.keys() == [
        "DOI",
        "ISBN",
        "Print_ISSN",
        "Online_ISSN",
        "Proprietary",
        "URI",
    ]
    assert empty_identifiers.keys() == []

    identifiers = Identifiers(ISBN="978-3-16-148410-0")
    assert identifiers.keys() == ["ISBN"]


def test_identifiers_items(full_identifiers, empty_identifiers):
    assert full_identifiers.items() == [
        ("DOI", "10.1000/182"),
        ("ISBN", "0-19-853453-1"),
        ("Print_ISSN", "1234-5678"),
        ("Online_ISSN", "0123-4567"),
        ("Proprietary", "Proprietary1"),
        ("URI", "https://example.org"),
    ]
    assert empty_identifiers.items() == []

    identifiers = Identifiers(ISBN="978-3-16-148410-0")
    assert identifiers.items() == [("ISBN", "978-3-16-148410-0")]


def test_identifiers_eq(empty_identifiers, full_identifiers):
    # other instance comparison
    assert Identifiers() == empty_identifiers
    assert (
        Identifiers(
            DOI="10.1000/182",
            ISBN="0-19-853453-1",
            Print_ISSN="1234-5678",
            Online_ISSN="0123-4567",
            Proprietary="Proprietary1",
            URI="https://example.org",
        )
        == full_identifiers
    )

    # dict comparison
    assert empty_identifiers == {}
    assert {} == empty_identifiers
    assert full_identifiers == {
        "DOI": "10.1000/182",
        "ISBN": "0-19-853453-1",
        "Print_ISSN": "1234-5678",
        "Online_ISSN": "0123-4567",
        "Proprietary": "Proprietary1",
        "URI": "https://example.org",
    }
    assert {
        "DOI": "10.1000/182",
        "ISBN": "0-19-853453-1",
        "Print_ISSN": "1234-5678",
        "Online_ISSN": "0123-4567",
        "Proprietary": "Proprietary1",
        "URI": "https://example.org",
    } == full_identifiers
