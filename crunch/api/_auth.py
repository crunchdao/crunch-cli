from enum import Enum


class AuthorizationScheme(Enum):

    BEARER = "Bearer"
    API_KEY = "API-Key"
    PROJECT_TOKEN = "Project-Token"
    RUN_TOKEN = "Run-Token"

    def __repr__(self):
        return self.name


class Auth:

    def __init__(
        self,
        scheme: AuthorizationScheme,
        token: str,
    ):
        self.scheme = scheme
        self._token = token

    @property
    def header_value(self) -> str:
        return f"{self.scheme.value} {self._token}"

    def __repr__(self):
        return f"Auth(scheme={self.scheme!r})"

    @staticmethod
    def bearer(token: str):
        return Auth(AuthorizationScheme.BEARER, token)

    @staticmethod
    def api_key(key: str):
        return Auth(AuthorizationScheme.API_KEY, key)

    @staticmethod
    def project_token(token: str):
        return Auth(AuthorizationScheme.PROJECT_TOKEN, token)

    @staticmethod
    def run_token(token: str):
        return Auth(AuthorizationScheme.RUN_TOKEN, token)
