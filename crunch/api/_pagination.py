from dataclasses import dataclass


@dataclass(frozen=True)
class PageRequest:

    number: int = 0
    size: int | None = None

    def next(self):
        return PageRequest(self.number + 1, self.size)
