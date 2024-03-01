import typing

from ..identifiers import CompetitionIdentifierType
from ..resource import Collection, Model


class User(Model):

    resource_identifier_attribute = "login"

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
        return self.prepare_model(
            self._client.api.get_user(
                id_or_login
            )
        )

    def list(
        self
    ) -> typing.List[User]:
        return self.prepare_models(
            self._client.api.list_users()
        )


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
