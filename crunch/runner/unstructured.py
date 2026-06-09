from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from types import ModuleType
from typing import Any, Callable, List, Literal, Optional

from crunch.runner.types import KwargsLike


class RunnerContext(ABC):

    @property
    @abstractmethod
    def started_at(self) -> datetime:
        pass  # pragma: no cover

    @property
    @abstractmethod
    def timeout(self) -> Optional[timedelta]:
        pass  # pragma: no cover

    @property
    def remaining_duration_before_timeout(self) -> Optional[timedelta]:
        timeout = self.timeout
        if timeout is None:
            return None

        elapsed_time = datetime.now() - self.started_at
        return timeout - elapsed_time

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
        trace: bool = True,
        span_hidden_parameters: Optional[List[str]] = None,
        span_attributes: Optional[KwargsLike] = None,
        install_data_fuse: bool = True,
    ) -> None:
        """
        Execute a command.

        Parameters:
            command (str): The command to be executed (e.g., "train", "predict", etc.).
            parameters (Optional[KwargsLike]): The parameters of the command.
            trace (bool): Whether to trace the command execution in a span automatically.
            span_hidden_parameters (Optional[List[str]]): The parameters to hide from the span (e.g., contain useless information).
            span_attributes (Optional[KwargsLike]): Additional attributes to merge with the span's attributes.
            install_data_fuse (bool): Whether to temporarily allow data access, which is only restricted after the data fuse is tripped. If disabled, no data access is ever permitted and triggering the fuse has no effect.

        Returns:
            None: This function does not return anything.

        Raises:
            ValueError: If the command fails, the user's code error is re-raised.
        """


class RunnerExecutorContext(ABC):

    @property
    @abstractmethod
    def is_local(self) -> bool:
        pass  # pragma: no cover

    @abstractmethod
    def trip_data_fuse(self) -> None:
        pass  # pragma: no cover


_sentinel = object()

class UserModule(ABC):

    def get_function(
        self,
        name: str,
    ) -> Callable[..., Any]:
        function = getattr(self.module, name, None)

        if not callable(function):
            raise ValueError(f"no `{name}` function found")

        return function

    def get_value(
        self,
        name: str,
        *,
        default: Optional[Any] = _sentinel,
    ) -> Optional[Any]:
        value = getattr(self.module, name, default)

        if value is _sentinel and default is _sentinel:
            raise ValueError(f"no `{name}` value found")

        return value

    @property
    @abstractmethod
    def module(self) -> ModuleType:
        pass  # pragma: no cover
