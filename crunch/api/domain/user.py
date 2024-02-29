import typing

from ..identifiers import CompetitionIdentifierType
from ..resource import Collection, Model


class User(Model):

    @property
    def login(self):
        return self._attrs["login"]


class UserCollection(Collection):

    model = User

    def __iter__(self) -> typing.Iterator[User]:
        return super().__iter__()

    def get(
        self,
        id_or_login: CompetitionIdentifierType
    ) -> User:
        response = self._client.api.get_user(
            id_or_login
        )

        return self.prepare_model(response)

    def list(
        self
    ) -> typing.List[User]:
        response = self._client.api.list_users()

        return [
            self.prepare_model(item)
            for item in response
        ]


class UserEndpointMixin:

    def list_users(
        self
    ):
        return self._result(
            self.get(
                "/v2/users"
            ),
            json=True
        )

    def get_user(
        self,
        identifier
    ):
        return self._result(
            self.get(
                f"/v2/users/{identifier}"
            ),
            json=True
        )
