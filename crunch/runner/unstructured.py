from abc import ABC, abstractmethod
from types import ModuleType
from typing import Any, Callable, Literal, Optional

from crunch.runner.types import KwargsLike


class RunnerContext(ABC):

    @property
    @abstractmethod
    def force_first_train(self) -> bool:
        pass  # pragma: no cover

    @property
    @abstractmethod
    def is_local(self) -> bool:
        pass  # pragma: no cover

    @property
    @abstractmethod
    def is_determinism_check_enabled(self) -> bool:
        pass  # pragma: no cover

    @abstractmethod
    def report_determinism(self, deterministic: bool) -> None:
        pass  # pragma: no cover

    @abstractmethod
    def log(
        self,
        message: str,
        *,
        important: bool = False,
        error: bool = False,
    ) -> Literal[True]:
        pass  # pragma: no cover

    @abstractmethod
    def execute(
        self,
        *,
        command: str,
        parameters: Optional[KwargsLike] = None,
    ) -> None:
        pass  # pragma: no cover


class RunnerExecutorContext(ABC):

    @property
    @abstractmethod
    def is_local(self) -> bool:
        pass  # pragma: no cover

    @abstractmethod
    def trip_data_fuse(self) -> None:
        pass  # pragma: no cover


class UserModule(ABC):

    def get_function(
        self,
        name: str,
    ) -> Callable[..., Any]:
        function = getattr(self.module, name, None)

        if not callable(function):
            raise ValueError(f"no `{name}` function found")

        return function

    @property
    @abstractmethod
    def module(self) -> ModuleType:
        pass  # pragma: no cover
