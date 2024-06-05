import typing
from dataclasses import dataclass, field
from datetime import date

IDS = [
    "DOI",
    "ISBN",
    "Print_ISSN",
    "Online_ISSN",
    "Proprietary",
    "URI",
    "authors",
    "publication_date",
]


@dataclass(init=False)
class Author:
    name: str
    ISNI: typing.Optional[str]
    ORCID: typing.Optional[str]

    __slots__ = ["name", "ISNI", "ORCID"]

    def __init__(
        self,
        name: str,
        ISNI: typing.Optional[str] = None,
        ORCID: typing.Optional[str] = None,
    ):
        self.name = name
        self.ISNI = ISNI
        self.ORCID = ORCID

    def as_csv(self):
        ids = []
        if self.ISNI is not None:
            ids.append(f"ISNI:{self.ISNI}")
        if self.ORCID is not None:
            ids.append(f"ORCID:{self.ORCID}")

        return f"{self.name} ({';'.join(ids)})"


# TODO since python3.10 there is `slot=True` attr which can really
# make this simpler (no need to override init anymore and define __slots__)
@dataclass(init=False)
class TitleIds:
    """Wrapper around title ids

    Because of dataclass it should have constant memory footprint and
    memory consumption should be lower as well when it contains at last one item
    compared to ordinary dict.

    For comaptiblity reasons it should work as a dict for some operations.
    """

    DOI: typing.Optional[str]
    ISBN: typing.Optional[str]
    Print_ISSN: typing.Optional[str]
    Online_ISSN: typing.Optional[str]
    Proprietary: typing.Optional[str]
    URI: typing.Optional[str]

    __slots__ = IDS

    def __init__(
        self,
        DOI: typing.Optional[str] = None,
        ISBN: typing.Optional[str] = None,
        Print_ISSN: typing.Optional[str] = None,
        Online_ISSN: typing.Optional[str] = None,
        Proprietary: typing.Optional[str] = None,
        URI: typing.Optional[str] = None,
    ):
        self.DOI = DOI
        self.ISBN = ISBN
        self.Print_ISSN = Print_ISSN
        self.Online_ISSN = Online_ISSN
        self.Proprietary = Proprietary
        self.URI = URI

    def __getitem__(self, item: str):
        if not isinstance(item, str):
            raise TypeError(item)

        if hasattr(self, item):
            res = getattr(self, item)
            if res is None:
                raise KeyError(item)
            else:
                return res
        else:
            raise KeyError(item)

    def __setitem__(self, key: str, value: typing.Optional[typing.Any]):
        if not hasattr(self, key):
            raise KeyError(key)
        setattr(self, key, value)

    def items(self):
        return [(k, getattr(self, k)) for k in self.keys()]

    def keys(self):
        return [e for e in IDS if getattr(self, e, None) is not None]


@dataclass
class CounterRecord:
    # The actual vaule
    value: int

    # mandatory, each record should have at least a start date
    start: typing.Optional[date] = None

    # Optional really, if the report is for whole month
    # then the start date (first day of the month) suffice
    end: typing.Optional[date] = None

    # name of the publication
    title: typing.Optional[str] = None

    # ISBN (books), ISSN (periodics), EISSN (same thing only for electronic version of publications)
    title_ids: TitleIds = field(default_factory=TitleIds)

    # name of item
    item: typing.Optional[str] = None

    # ISBN (books), ISSN (periodics), EISSN (same thing only for electronic version of publications)
    item_ids: typing.Dict[str, str] = field(default_factory=dict)

    # When was the item published
    item_publication_date: typing.Optional[str] = None

    # Who it the author of an item
    item_authors: typing.Optional[typing.List[Author]] = None

    # contains more details
    dimension_data: typing.Dict[str, str] = field(default_factory=dict)

    # What actually value of this record means
    metric: typing.Optional[str] = None

    # Entity to which are these data related
    organization: typing.Optional[str] = None

    def as_csv(self) -> typing.Tuple[str, ...]:
        def serialize_dict(mapping: typing.Optional[typing.Union[dict, TitleIds]]) -> str:
            if not mapping:
                return ""
            return "|".join(f"{k}:{mapping[k]}" for k in sorted(mapping.keys()))

        def format_date(date_obj: typing.Optional[date]):
            if not date_obj:
                return ""
            return date_obj.strftime("%Y-%m-%d")

        authors = "|".join(e.as_csv() for e in (self.item_authors or []))

        return (
            format_date(self.start),
            format_date(self.end),
            self.organization or "",
            self.title or "",
            serialize_dict(self.title_ids),
            self.item or "",
            serialize_dict(self.item_ids),
            self.item_publication_date or "",
            authors,
            serialize_dict(self.dimension_data),
            self.metric or "",
            str(self.value),
        )

    @classmethod
    def empty_generator(cls) -> typing.Generator["CounterRecord", None, None]:
        empty: typing.List["CounterRecord"] = []
        return (e for e in empty)
