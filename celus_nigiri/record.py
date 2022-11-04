import typing
from dataclasses import dataclass, field
from datetime import date


@dataclass
class CounterRecord:
    # The actual vaule
    value: int

    # mandatory, each record should have at least a start date
    start: typing.Optional[date] = None

    # Optional really, if the report is for whole month then the start date (first day of the month) suffice
    end: typing.Optional[date] = None

    # name of the publication
    title: typing.Optional[str] = None

    # ISBN (books), ISSN (periodics), EISSN (same thing only for electronic version of publications)
    title_ids: typing.Dict[str, str] = field(default_factory=dict)

    # contains more details
    dimension_data: typing.Dict[str, str] = field(default_factory=dict)

    # What actually value of this record means
    metric: typing.Optional[str] = None

    # Entity to which are these data related
    organization: typing.Optional[str] = None

    def as_csv(self) -> typing.Tuple[str, ...]:
        def serialize_dict(mapping: typing.Optional[dict]) -> str:
            if not mapping:
                return ""
            return "|".join(f"{k}:{mapping[k]}" for k in sorted(mapping.keys()))

        def format_date(date_obj: typing.Optional[date]):
            if not date_obj:
                return ""
            return date_obj.strftime("%Y-%m-%d")

        return (
            format_date(self.start),
            format_date(self.end),
            self.organization or "",
            self.title or "",
            self.metric or "",
            serialize_dict(self.dimension_data),
            serialize_dict(self.title_ids),
            str(self.value),
        )

    @classmethod
    def empty_generator(cls) -> typing.Generator['CounterRecord', None, None]:
        empty: typing.List['CounterRecord'] = []
        return (e for e in empty)
