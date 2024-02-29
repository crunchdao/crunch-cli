"""
Heavily inspired (copied) from https://github.com/docker/docker-py/blob/main/docker/models/resource.py.
"""

import typing


class Model:

    id_attribute = 'id'
    resource_identifier_attribute = 'id'

    def __init__(
        self,
        attrs: dict = None,
        client: "Client" = None,
        collection: "Collection" = None
    ):
        self.client = client
        self.collection = collection
        self.attrs = attrs or {}

    def __repr__(self):
        repr = f"{self.__class__.__name__}(id={self.id}"

        if self.id_attribute != self.resource_identifier_attribute:
            repr += f", {self.resource_identifier_attribute}={self.resource_identifier}"

        return f"{repr})"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(f"{self.__class__.__name__}:{self.id}")

    @property
    def id(self):
        return self.attrs.get(self.id_attribute)

    @property
    def resource_identifier(self):
        return self.attrs.get(self.resource_identifier_attribute)

    def reload(
        self,
        *args,
        **kwargs
    ):
        new_model = self.collection.get(self.resource_identifier, *args, **kwargs)
        self.attrs = new_model.attrs
        return self


T = typing.TypeVar('T')


class Collection:

    model: typing.Type[T] = None

    def __init__(self, client=None):
        self.client = client

    def __iter__(self) -> typing.Iterator[T]:
        return iter(self.list())

    def list(self):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def prepare_model(self, attrs, *args):
        if isinstance(attrs, self.model):
            attrs.client = self.client
            attrs.collection = self
            return attrs

        if isinstance(attrs, dict):
            return self.model(
                *args,
                attrs=attrs,
                client=self.client,
                collection=self
            )

        raise Exception(f"can't create {self.model.__name__} from {attrs}")
