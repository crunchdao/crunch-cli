import os

from .. import store, constants

from .auth import Auth, ApiKeyAuth, NoneAuth
from .endpoints import EndpointClient
from .models.competitions import CompetitionCollection


class Client:

    def __init__(
        self,
        base_url: str,
        auth: Auth
    ):
        self.api = EndpointClient(base_url, auth)

    @property
    def competitions(self):
        return CompetitionCollection(client=self)

    def from_env():
        store.load_from_env()

        api_key = os.getenv(constants.API_KEY_ENV_VAR)
        if api_key:
            auth = ApiKeyAuth(api_key)
        else:
            auth = NoneAuth()

        return Client(store.api_base_url, auth)
