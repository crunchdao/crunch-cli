import dataclasses
import typing

T = typing.TypeVar("T")


@dataclasses.dataclass(frozen=True)
class PageRequest:

    number: int = 0
    size: typing.Optional[int] = None

    def next(self):
        return PageRequest(self.number + 1, self.size)
