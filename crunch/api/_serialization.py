from dataclasses import fields
from datetime import datetime
from functools import cache
from typing import TYPE_CHECKING, Any, Iterable, Mapping, TypeVar

from mashumaro.config import BaseConfig
from mashumaro.mixins.json import DataClassJSONMixin

from crunch.utils import parse_datetime

__all__ = [
    "JsonDataClassMixin",
]

_Model = TypeVar("_Model", bound="JsonDataClassMixin")


class JsonDataClassMixin(DataClassJSONMixin):

    class Config(BaseConfig):
        # compile on first use instead of at import time, keeps the cli startup fast
        lazy_compilation = True

        serialization_strategy = {
            datetime: {
                "deserialize": parse_datetime,
            },
        }

    if TYPE_CHECKING:
        # mashumaro declares `d` as a bare `Mapping`, which is partially unknown in strict mode,
        # only re-declared for the type checker, the runtime still uses mashumaro's (final) implementation
        @classmethod
        def from_dict(cls: type[_Model], d: Mapping[str, Any], **kwargs: Any) -> _Model:  # pyright: ignore[reportIncompatibleMethodOverride]
            ...

    @classmethod
    def from_dict_array(cls: type[_Model], payloads: Iterable[Mapping[str, Any]], **kwargs: Any) -> list[_Model]:
        return [
            cls.from_dict(payload, **kwargs)
            for payload in payloads
        ]

    @classmethod
    def __pre_deserialize__(cls, d: dict[Any, Any]) -> dict[Any, Any]:
        field_names = _get_field_names_by_camel_case(cls)

        return {
            field_names.get(key, key): value
            for key, value in d.items()
        }

    def __post_serialize__(self, d: dict[Any, Any]) -> dict[Any, Any]:
        return {
            _to_camel_case(key): value
            for key, value in d.items()
        }


def _to_camel_case(name: str) -> str:
    from crunch.external.inflection import camelize

    return camelize(name, False)


@cache
def _get_field_names_by_camel_case(cls: type) -> dict[str, str]:
    return {
        _to_camel_case(field.name): field.name
        for field in fields(cls)
    }
