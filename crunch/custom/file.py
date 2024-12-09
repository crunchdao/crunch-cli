import dataclasses
import functools
import os
import typing

import dataclasses_json
import requests


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass(frozen=True)
class File:

    path: str
    uri: str
    size: int

    @property
    def name(self):
        return os.path.basename(self.path)

    @property
    def directory(self):
        return os.path.dirname(self.path)

    @functools.cached_property
    def text(self):
        if not self.uri:
            return None

        if self.uri.startswith("http:") or self.uri.startswith("https:"):
            return requests.get(self.uri).text

        with open(self.uri) as fd:
            return fd.read()

    @classmethod
    def from_dict_array(
        cls,
        input: typing.List[dict],
    ):
        return [
            cls.from_dict(x)
            for x in input
        ]
