import requests
import typing
import dataclasses
import urllib.parse
import inflection

from ... import store
from .. import models


@dataclasses.dataclass(frozen=True)
class ClientConfiguration:

    web_base_url: str = dataclasses.field(default_factory=lambda: store.web_base_url)
    api_base_url: str = dataclasses.field(default_factory=lambda: store.api_base_url)
    debug: bool = dataclasses.field(default_factory=lambda: store.debug)


class Client:

    def __init__(
        self,
        configuration: typing.Optional[ClientConfiguration] = None
    ):
        self._configuration = configuration or ClientConfiguration()

        self._session = requests.Session()

    def _request(self, method, endpoint, *args, **kwargs):
        response = super().request(
            method,
            urllib.parse.urljoin(self._configuration.api_base_url, endpoint),
            *args,
            **kwargs
        )

        status_code = response.status_code
        if status_code // 100 != 2:
            raise self._convert_error(response)

        return response

    def _convert_error(
        self,
        response: requests.Response
    ):
        try:
            error = response.json()
        except:
            return ValueError(f"unexpected error: {response.text}")
        else:
            code = error.pop("code", "")
            message = error.pop("message", "")

            error_class = self._find_error_class(code, message)
            error = error_class(message)

            for key, value in error.items():
                key = inflection.underscore(key)
                setattr(error, key, value)

            return error

    def _find_error_class(
        self,
        code: str
    ):
        if code:
            base_class_name = inflection.camelize(code)

            for suffix in ["Exception", "Error"]:
                class_name = base_class_name + suffix

                clazz = getattr(models, class_name, None)
                if clazz is not None:
                    return clazz

        return models.ApiException
