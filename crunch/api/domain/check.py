import enum
import types
import typing

from ..resource import Collection, Model
from .competition import Competition


class CheckFunction(enum.Enum):

    COLUMNS_NAME = "COLUMNS_NAME"
    NANS = "NANS"
    VALUES_BETWEEN = "VALUES_BETWEEN"
    VALUES_ALLOWED = "VALUES_ALLOWED"
    MOONS = "MOONS"
    IDS = "IDS"
    CONSTANTS = "CONSTANTS"

    def __repr__(self):
        return self.name


class CheckFunctionScope(enum.Enum):

    ROOT = "ROOT"
    KEY = "KEY"

    def __repr__(self):
        return self.name


class Check(Model):

    def __init__(
        self,
        competition: Competition,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._competition = competition

    @property
    def competition(self):
        return self._competition

    @property
    def function(self):
        return CheckFunction[self._attrs["function"]]

    @property
    def scope(self):
        return CheckFunctionScope[self._attrs["scope"]]

    @property
    def order(self):
        return self._attrs["order"]

    @property
    def parameters(self) -> dict:
        return types.MappingProxyType(self._attrs.get("parameters") or {})


class CheckCollection(Collection):

    model = Check

    def __init__(
        self,
        competition: Competition,
        client=None
    ):
        super().__init__(client)

        self.competition = competition

    def __iter__(self) -> typing.Iterator[Check]:
        return super().__iter__()

    def list(
        self
    ) -> Check:
        return self.prepare_models(
            self._client.api.list_checks(
                self.competition.id
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.competition
        )


class CheckEndpointMixin:

    def list_checks(
        self,
        competition_identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/checks"
            ),
            json=True
        )
