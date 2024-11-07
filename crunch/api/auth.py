import abc
import re
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

    def strip(
        self,
        error_message: str,
    ) -> str:
        return error_message


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

    def strip(
        self,
        error_message: str,
    ):
        return re.sub(
            r"pushToken=\w+",
            "pushToken=HIDDEN",
            error_message
        )


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
