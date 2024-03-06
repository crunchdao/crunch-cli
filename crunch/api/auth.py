import abc
import typing


class Auth(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def apply(
        self,
        headers: dict,
        params: dict,
        data: typing.Optional[dict],
    ):
        ...


class NoneAuth(Auth):

    def apply(
        self,
        headers: dict,
        params: dict,
        data: typing.Optional[dict],
    ):
        pass


class ApiKeyAuth(Auth):

    def __init__(
        self,
        key: str
    ):
        super().__init__()

        self._key = key

    def apply(
        self,
        headers: dict,
        params: dict,
        data: typing.Optional[dict],
    ):
        headers["Authorization"] = f"API-Key {self._key}"


class PushTokenAuth(Auth):

    def __init__(
        self,
        token: str
    ):
        super().__init__()

        self._token = token

    def apply(
        self,
        headers: dict,
        params: dict,
        data: typing.Optional[dict],
    ):
        if data is not None:
            data["pushToken"] = self._token
        else:
            params["pushToken"] = self._token


class RunTokenAuth(Auth):

    def __init__(
        self,
        token: str
    ):
        super().__init__()

        self._token = token

    def apply(
        self,
        headers: dict,
        params: dict,
        data: typing.Optional[dict],
    ):
        headers["X-Run-Token"] = self._token
