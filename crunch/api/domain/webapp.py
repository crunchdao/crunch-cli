from dataclasses import dataclass
from typing import TYPE_CHECKING

from dataclasses_json import LetterCase, Undefined, dataclass_json

if TYPE_CHECKING:
    from crunch.api.client import Client


@dataclass_json(
    letter_case=LetterCase.CAMEL,  # type: ignore
    undefined=Undefined.EXCLUDE,
)
@dataclass(frozen=True)
class Configuration:

    phala: "ConfigurationPhala"


@dataclass_json(
    letter_case=LetterCase.CAMEL,  # type: ignore
    undefined=Undefined.EXCLUDE,
)
@dataclass(frozen=True)
class ConfigurationPhala:

    key_url: str


class WebappNamespace:

    def __init__(
        self,
        *,
        client: "Client",
    ):
        self._client = client

    @property
    def configuration(self) -> Configuration:
        return self._client.api.get_configuration()


class WebappEndpointMixin:

    def get_configuration(
        self
    ) -> Configuration:
        content = self._result(
            self.get(
                "/v1/configuration"
            ),
            json=True
        )

        return Configuration.from_dict(content)
