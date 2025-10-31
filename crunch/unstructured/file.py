import os
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Dict, List

import requests
from dataclasses_json import LetterCase, Undefined, dataclass_json


@dataclass_json(
    letter_case=LetterCase.CAMEL,  # type: ignore
    undefined=Undefined.EXCLUDE,
)
@dataclass(frozen=True)
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

    @cached_property
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
        input: List[Dict[str, Any]],
    ) -> List['File']:
        return [
            cls.from_dict(x)  # type: ignore
            for x in input
        ]
