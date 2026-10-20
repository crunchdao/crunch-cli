from dataclasses import dataclass
from typing import TYPE_CHECKING

from crunch.api._base import ApiCollection, ApiModel, Parent

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._identifiers import UserIdentifierType


@dataclass
class User(ApiModel):
    id: int
    login: str

    client = Parent["Client"]()

    def reload(self) -> "User":
        return self._update_from(self.client.users.get(self.id))


class UserCollection(ApiCollection[User]):

    model = User

    def _attach(self, user: User) -> User:
        user.client = self.client
        return user

    def get(self, /, identifier: "UserIdentifierType") -> User:
        return self.from_dict(self.client.api.get_json([
            "v2", "users", identifier
        ]))

    def get_me(self) -> User:
        return self.get("@me")

    @property
    def me(self) -> User:
        return self.get_me()

    def list(self) -> list[User]:
        return self.from_dict_array(self.client.api.get_json([
            "v2", "users"
        ]))
