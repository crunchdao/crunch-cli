from typing import TYPE_CHECKING, List

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._identifiers import CompetitionIdentifierType


class User(Model[int]):

    @property
    def resource_identifier(self) -> str:
        return self.login

    @property
    def login(self):
        return self._attrs["login"]


class UserCollection(Collection[User]):

    model = User

    def get(
        self,
        identifier: "CompetitionIdentifierType"
    ) -> User:
        return self.prepare_model(
            self._checked_client.api.get_user(
                identifier
            )
        )

    def get_me(self):
        return self.get("@me")

    @property
    def me(self):
        return self.get_me()

    def list(
        self
    ) -> List[User]:
        return self.prepare_models(
            self._checked_client.api.list_users()
        )


class UserEndpointMixin(EndpointMixin):

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
        identifier: "CompetitionIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v2/users/{identifier}"
            ),
            json=True
        )
