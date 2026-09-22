"""
Heavily inspired (copied) from https://github.com/docker/docker-py/blob/main/docker/models/resource.py.
"""

from abc import ABC, abstractmethod
from types import GeneratorType
from typing import TYPE_CHECKING, Any, Callable, Dict, Generic, Iterable, Iterator, List, Literal, Optional, Type, TypeVar, Union, cast, overload

import requests


if TYPE_CHECKING:
    from crunch.api._auth import Auth
    from crunch.api._client import Client
    from crunch.api._pagination import PageRequest

ID = TypeVar('ID')
M = TypeVar('M', bound="Model[Any]")


class Model(Generic[ID]):

    def __init__(
        self,
        *,
        attrs: Optional[Dict[str, Any]] = None,
        client: Optional["Client"] = None,
        collection: Optional["Collection[Any]"] = None
    ):
        self._attrs: Dict[str, Any] = attrs or {}
        self._client = client
        self._collection = collection

    @property
    def _checked_client(self) -> "Client":
        if self._client is None:
            raise Exception("client unavailable")

        return self._client

    @property
    def _checked_collection(self) -> "Collection[Any]":
        if self._collection is None:
            raise Exception("collection unavailable")

        return self._collection

    def __repr__(self):
        resource_identifier = self.resource_identifier
        if resource_identifier == self.id:
            return f"{self.__class__.__name__}(id={self.id})"

        return f"{self.__class__.__name__}(id={self.id}, resource_identifier={resource_identifier})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(f"{self.__class__.__name__}:{self.id}")

    @property
    def id(self) -> ID:
        return cast(ID, self._attrs.get('id'))

    @property
    def resource_identifier(self) -> Any:
        return self.id

    def reload(
        self: "M",
        *args: Any,
        **kwargs: Any,
    ) -> "M":
        resource_identifier = self.resource_identifier
        if not isinstance(resource_identifier, (list, tuple)):
            resource_identifier = [resource_identifier]

        new_model = self._checked_collection.get(
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


T = TypeVar('T', bound=Model)


class Collection(Generic[T]):

    model: Type[T]

    def __init__(self, client: Optional["Client"] = None):
        self._client = client

    @property
    def _checked_client(self) -> "Client":
        if self._client is None:
            raise Exception("client unavailable")

        return self._client

    def list(self) -> Iterable[T]:
        raise NotImplementedError

    def __iter__(self) -> Iterator[T]:
        return iter(self.list())

    def get_reference(self, id: Optional[Any] = None, **attrs: Any) -> T:
        """
        Builds a lazy, unfetched model from just enough attrs to compute its
        `resource_identifier` (e.g. `id=123`, or `user_id=1, name="foo"` for
        a `Project`) — useful to avoid a round-trip when the caller already
        knows how to address the resource. Call `.reload()` to fetch the rest.
        """
        if id is not None:
            attrs = {"id": id, **attrs}

        return self.prepare_model(attrs)

    def prepare_model(self, attrs: Union["JsonValue", T], *args: Any) -> T:
        if isinstance(attrs, self.model):
            attrs._client = self._client  # pyright: ignore[reportPrivateUsage]
            attrs._collection = self  # pyright: ignore[reportPrivateUsage]
            return attrs

        if isinstance(attrs, dict):
            return self.model(
                *args,
                attrs=attrs,
                client=self._client,
                collection=self
            )

        raise Exception(f"can't create {self.model.__name__} from {attrs}")

    @overload
    def prepare_models(self, attrs_list: Union["JsonValue", T], *args: Any) -> List[T]:
        ...

    @overload
    def prepare_models(self, attrs_list: Iterator[Union["JsonValue", T]], *args: Any) -> Iterator[T]:
        ...

    def prepare_models(self, attrs_list: Union["JsonValue", T, Iterator[Union["JsonValue", T]]], *args: Any) -> Union[List[T], Iterator[T]]:
        if isinstance(attrs_list, GeneratorType):
            return self._prepare_models_with_yield(attrs_list, args)

        if isinstance(attrs_list, list):
            return [
                self.prepare_model(attrs, *args)
                for attrs in attrs_list
            ]

        raise Exception(f"can't create {self.model.__name__} list from {attrs_list}")

    def _prepare_models_with_yield(self, attrs_list: GeneratorType[T], args: Any):
        for attrs in attrs_list:
            yield self.prepare_model(attrs, *args)

        return GeneratorExit


JsonValue = Union[str, int, float, bool, None, dict[str, "JsonValue"], list["JsonValue"]]


class EndpointMixin(ABC):

    if TYPE_CHECKING:
        get: Callable[..., requests.Response]
        post: Callable[..., requests.Response]
        put: Callable[..., requests.Response]
        delete: Callable[..., requests.Response]
        page_size: int
        auth_: "Auth"

        def _paginated(
            self,
            requester: Callable[["PageRequest"], requests.Response],
            page_size: Optional[int] = None,
        ) -> Iterator[JsonValue]:
            ...

    @overload
    def _result(
        self,
        response: requests.Response,
        json: Literal[True],
        binary: Literal[False] = False,
    ) -> JsonValue:
        ...

    @overload
    def _result(
        self,
        response: requests.Response,
        json: Literal[False] = False,
        binary: Literal[True] = True,
    ) -> bytes:
        ...

    @overload
    def _result(
        self,
        response: requests.Response,
        json: Literal[False] = False,
        binary: Literal[False] = False,
    ) -> str:
        ...

    def _result(
        self,
        response: requests.Response,
        json: bool = False,
        binary: bool = False,
    ) -> Union[JsonValue, bytes, str]:
        ...
