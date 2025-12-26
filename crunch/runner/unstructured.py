from abc import ABC, abstractmethod
from types import ModuleType
from typing import Any, Callable, Literal, Optional

from crunch.runner.types import KwargsLike


class RunnerContext(ABC):

    @property
    @abstractmethod
    def train_frequency(self) -> int:
        pass  # pragma: no cover

    @property
    @abstractmethod
    def force_first_train(self) -> bool:
        pass  # pragma: no cover

    @property
    @abstractmethod
    def is_local(self) -> bool:
        """
        Whether the runner is running in a local (or Colab) environment.
        """

    @property
    @abstractmethod
    def is_submission_phase(self) -> bool:
        """
        Whether the runner is running for the Submission Phase.
        """
    
    @property
    def is_first_time(self) -> bool:
        if self.is_submission_phase:
            return True
        
        # 1 is for the submission phase
        return self.chain_height <= 2

    @property
    @abstractmethod
    def chain_height(self) -> int:
        """
        How many consecutive runs are there with the same parameters?
        """

    @property
    @abstractmethod
    def has_model(self) -> bool:
        """
        Whether this submission originally had a model.
        """

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
