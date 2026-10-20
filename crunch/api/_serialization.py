from dataclasses import fields
from datetime import datetime
from functools import cache
from typing import TYPE_CHECKING, Any, Iterable, Mapping, TypeVar

from crunch.utils import parse_datetime

if TYPE_CHECKING:
    from mashumaro.codecs.basic import BasicDecoder, BasicEncoder
    from mashumaro.dialect import Dialect

__all__ = [
    "JsonDataClassMixin",
]

_Model = TypeVar("_Model", bound="JsonDataClassMixin")


class JsonDataClassMixin:

    @classmethod
    def from_dict(cls: type[_Model], d: Mapping[str, Any]) -> _Model:
        return _get_decoder(cls).decode(d)

    @classmethod
    def from_dict_array(cls: type[_Model], payloads: Iterable[Mapping[str, Any]]) -> list[_Model]:
        decoder = _get_decoder(cls)

        return [
            decoder.decode(payload)
            for payload in payloads
        ]

    def to_dict(self) -> dict[str, Any]:
        return _get_encoder(type(self)).encode(self)

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


_decoders: dict[type[Any], "BasicDecoder[Any]"] = {}
_encoders: dict[type[Any], "BasicEncoder[Any]"] = {}


def _get_decoder(cls: type[_Model]) -> "BasicDecoder[_Model]":
    decoder = _decoders.get(cls)

    if decoder is None:
        from mashumaro.codecs.basic import BasicDecoder
        decoder = _decoders[cls] = BasicDecoder(cls, default_dialect=_get_dialect())

    return decoder


def _get_encoder(cls: type[_Model]) -> "BasicEncoder[_Model]":
    encoder = _encoders.get(cls)

    if encoder is None:
        from mashumaro.codecs.basic import BasicEncoder
        encoder = _encoders[cls] = BasicEncoder(cls, default_dialect=_get_dialect())

    return encoder


@cache
def _get_dialect() -> "type[Dialect]":
    from mashumaro.dialect import Dialect

    class ApiDialect(Dialect):
        serialization_strategy: dict[Any, Any] = {
            datetime: {
                "deserialize": parse_datetime,
            },
        }

    return ApiDialect


def _to_camel_case(name: str) -> str:
    from crunch.external.inflection import camelize

    return camelize(name, False)


@cache
def _get_field_names_by_camel_case(cls: type) -> dict[str, str]:
    return {
        _to_camel_case(field.name): field.name
        for field in fields(cls)
    }
