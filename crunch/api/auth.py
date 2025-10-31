import re
from abc import ABC, abstractmethod
from typing import Dict, Optional


class Auth(ABC):

    @abstractmethod
    def apply(
        self,
        headers: Dict[str, str],
        params: Dict[str, str],
        data: Optional[Dict[str, str]],
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
        headers: Dict[str, str],
        params: Dict[str, str],
        data: Optional[Dict[str, str]],
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
        headers: Dict[str, str],
        params: Dict[str, str],
        data: Optional[Dict[str, str]],
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
        headers: Dict[str, str],
        params: Dict[str, str],
        data: Optional[Dict[str, str]],
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
        headers: Dict[str, str],
        params: Dict[str, str],
        data: Optional[Dict[str, str]],
    ):
        headers["X-Run-Token"] = self._token
