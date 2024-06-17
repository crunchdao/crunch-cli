"""
Heavily inspired (copied) from https://github.com/docker/docker-py/blob/main/docker/models/resource.py.
"""

import typing


# TODO: add better support for composite key resources
class Model:

    id_attribute = 'id'
    resource_identifier_attribute = 'id'

    def __init__(
        self,
        attrs: dict = None,
        client: "Client" = None,
        collection: "Collection" = None
    ):
        self._attrs = attrs or {}
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

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(f"{self.__class__.__name__}:{self.id}")

    @property
    def id(self):
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
        *args,
        **kwargs
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
        input: typing.List[dict],
        *args
    ):
        return [
            cls.from_dict(x, *args)
            for x in input
        ]


T = typing.TypeVar('T', Model, Model)


class Collection:

    model: typing.Type[T] = None

    def __init__(self, client=None):
        self._client = client

    def __iter__(self) -> typing.Iterator[T]:
        return iter(self.list())

    def __getitem__(self, key) -> T:
        return self.list()[key]

    def __getslice__(self, from_, to):
        return self.list()[from_, to]

    def list(self) -> typing.List[T]:
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

    def prepare_models(self, attrs_list, *args) -> typing.List[T]:
        return [
            self.prepare_model(attrs, *args)
            for attrs in attrs_list
        ]
