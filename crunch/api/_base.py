from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, Iterable, Iterator, Mapping, TypeVar, overload

from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._client import Client

__all__ = [
    "ApiCollection",
    "ApiEnum",
    "ApiModel",
    "Parent",
]

_ParentT = TypeVar("_ParentT")
_ModelT = TypeVar("_ModelT", bound="ApiModel")


class Parent(Generic[_ParentT]):
    """
    Typed link to the object a model belongs to, set by the collection that parsed it.

    Declared without an annotation, so it is neither a dataclass field nor (de)serialized:

        competition = Parent["Competition"]()
    """

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name
        self._attribute = f"_parent_{name}"

    @overload
    def __get__(self, instance: None, owner: type) -> "Parent[_ParentT]":
        ...

    @overload
    def __get__(self, instance: object, owner: type) -> _ParentT:
        ...

    def __get__(self, instance: object | None, owner: type) -> "Parent[_ParentT] | _ParentT":
        if instance is None:
            return self

        try:
            return vars(instance)[self._attribute]
        except KeyError:
            raise AttributeError(f"{type(instance).__name__}.{self._name} is not set") from None

    def __set__(self, instance: object, value: _ParentT) -> None:
        vars(instance)[self._attribute] = value


class ApiEnum(Enum):
    """Enum that accepts values unknown to this version of the cli, instead of failing the whole parsing."""

    @classmethod
    def _missing_(cls, value: object) -> Any:
        member = object.__new__(cls)
        member._name_ = str(value)
        member._value_ = value

        # the same unknown value always gives the same member, but it is not listed with the known ones
        cls._value2member_map_[value] = member
        return member

    def __repr__(self):
        return self.name


class ApiModel(JsonDataClassMixin):
    """Object of the API: its data are dataclass fields, its parent is a `Parent`."""

    def _update_from(self: _ModelT, other: "ApiModel") -> _ModelT:
        """For a `reload()`: take the data (and the parents) of a freshly fetched copy."""

        assert type(other) is type(self), f"cannot update a {type(self).__name__} from a {type(other).__name__}"

        vars(self).update(vars(other))
        return self


class ApiCollection(Generic[_ModelT]):
    """
    Parses and attaches models, and gives access to the API.

    Nothing else is required: each collection declares the `get`/`list`/... it supports, with its own filters.
    A paginated endpoint is exposed as `iter(...)`, returning an `Iterator` (pages are fetched while iterating),
    a single-response one as `list(...)`, returning a `list`.
    """

    model: type[_ModelT]

    def __init__(self, client: "Client"):
        self.client = client

    def _attach(self, model: _ModelT, /) -> _ModelT:
        return model

    def from_dict(self, payload: Mapping[str, Any]) -> _ModelT:
        return self._attach(self.model.from_dict(payload))

    def from_dict_array(self, payloads: Iterable[Mapping[str, Any]]) -> list[_ModelT]:
        return [
            self.from_dict(payload)
            for payload in payloads
        ]

    def from_dict_iterator(self, payloads: Iterable[Mapping[str, Any]]) -> Iterator[_ModelT]:
        for payload in payloads:
            yield self.from_dict(payload)
