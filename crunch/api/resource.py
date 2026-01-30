"""
Heavily inspired (copied) from https://github.com/docker/docker-py/blob/main/docker/models/resource.py.
"""

from types import GeneratorType
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Type, TypeVar, Union

if TYPE_CHECKING:
    from crunch.api.client import Client


# TODO: add better support for composite key resources
class Model:

    id_attribute = 'id'
    resource_identifier_attribute = 'id'

    def __init__(
        self,
        attrs: Optional[Dict[str, Any]] = None,
        client: Optional["Client"] = None,
        collection: Optional["Collection"] = None
    ):
        self._attrs: Dict[str, Any] = attrs or {}
        self._client = client
        self._collection = collection

    def __repr__(self):
        repr = f"{self.__class__.__name__}(id={self.id}"

        if self.id_attribute != self.resource_identifier_attribute:
            if isinstance(self.resource_identifier_attribute, (list, tuple)):
                repr += f", " + ", ".join([
                    f"{key}={value}"
                    for key, value in zip(self.resource_identifier_attribute, self.resource_identifier)
                ])
            else:
                repr += f", {self.resource_identifier_attribute}={self.resource_identifier}"

        return f"{repr})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(f"{self.__class__.__name__}:{self.id}")

    @property
    def id(self) -> Union[int, str]:
        return self._attrs.get(self.id_attribute)

    @property
    def resource_identifier(self):
        if isinstance(self.resource_identifier_attribute, (list, tuple)):
            return [
                getattr(self, key, None) or self._attrs.get(key)
                for key in self.resource_identifier_attribute
            ]

        return self._attrs.get(self.resource_identifier_attribute)

    def reload(
        self,
        *args, # type: ignore
        **kwargs, # type: ignore
    ):
        resource_identifier = self.resource_identifier
        if not isinstance(resource_identifier, (list, tuple)):
            resource_identifier = [resource_identifier]

        new_model = self._collection.get(
            *resource_identifier,
            *args,
            **kwargs
        )

        self._attrs = new_model._attrs
        return self

    @classmethod
    def from_dict(
        cls,
        input: dict,
        *args
    ):
        return cls(*args, attrs=input)

    @classmethod
    def from_dict_array(
        cls,
        input: List[dict],
        *args
    ):
        return [
            cls.from_dict(x, *args)
            for x in input
        ]


T = TypeVar('T', Model, Model)


class Collection:

    model: Type[T] = None

    def __init__(self, client: Optional["Client"] = None):
        self._client = client

    def __iter__(self) -> Iterator[T]:
        return iter(self.list())

    def __getitem__(self, key) -> T:
        if isinstance(key, slice):
            return self.__getslice__(key.start, key.stop, key.step)

        collection = self.list()

        if isinstance(collection, GeneratorType):
            for _ in range(key):
                next(collection)

            return next(collection)

        return collection[key]

    def __getslice__(self, start, stop, step):
        collection = self.list()

        if isinstance(collection, GeneratorType):
            if start:
                for _ in range(start):
                    next(collection)

            arguments = list(filter(bool, (start, stop, step)))
            for _ in range(*arguments):
                yield next(collection)

            return GeneratorExit

        return collection[start:stop]

    def list(self) -> List[T]:
        raise NotImplementedError

    def get(self, key) -> T:
        raise NotImplementedError

    def get_reference(
        self,
        id,
        resource_identifier=None
    ) -> T:
        id_attribute = self.model.id_attribute
        attrs = {
            id_attribute: id
        }

        resource_identifier_attribute = self.model.resource_identifier_attribute
        if (
            resource_identifier_attribute != id_attribute
            and resource_identifier is not None
        ):
            if isinstance(resource_identifier_attribute, (list, tuple)):
                attrs.update(dict(zip(resource_identifier_attribute, resource_identifier)))
            else:
                attrs[resource_identifier_attribute] = resource_identifier

        return self.prepare_model(attrs)

    def prepare_model(self, attrs, *args) -> T:
        if isinstance(attrs, self.model):
            attrs._client = self._client
            attrs._collection = self
            return attrs

        if isinstance(attrs, dict):
            return self.model(
                *args,
                attrs=attrs,
                client=self._client,
                collection=self
            )

        raise Exception(f"can't create {self.model.__name__} from {attrs}")

    def prepare_models(self, attrs_list, *args) -> List[T]:
        if isinstance(attrs_list, GeneratorType):
            return self._prepare_models_with_yield(attrs_list, args)

        return [
            self.prepare_model(attrs, *args)
            for attrs in attrs_list
        ]

    def _prepare_models_with_yield(self, attrs_list, args):
        for attrs in attrs_list:
            yield self.prepare_model(attrs, *args)

        return GeneratorExit
