import dataclasses
import typing

import dataclasses_json

from ..identifiers import RoundIdentifierType
from .competitions import Competition
from .resource import Collection, Model


class Round(Model):

    resource_identifier_attribute = "number"

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
    def number(self) -> int:
        return self.attrs["number"]

    @property
    def phases(self):
        from .phases import PhaseCollection

        return PhaseCollection(
            round=self,
            client=self.client
        )


class RoundCollection(Collection):

    model = Round

    def __init__(
        self,
        competition: Competition,
        client=None
    ):
        super().__init__(client)

        self.competition = competition

    def __iter__(self) -> typing.Iterator[Round]:
        return super().__iter__()

    def get(
        self,
        identifier: RoundIdentifierType
    ) -> Round:
        response = self.client.api.get_round(
            self.competition.id,
            identifier
        )

        return self.prepare_model(
            response,
            self.competition
        )

    def get_current(self):
        return self.get("@current")

    def get_last(self):
        return self.get("@last")

    def list(
        self
    ) -> typing.List[Round]:
        response = self.client.api.list_rounds(
            self.competition.id,
        )

        return [
            self.prepare_model(
                item,
                self.competition
            )
            for item in response
        ]
