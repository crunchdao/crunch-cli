from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PageRequest:

    number: int = 0
    size: Optional[int] = None

    def next(self):
        return PageRequest(self.number + 1, self.size)
