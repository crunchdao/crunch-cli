import abc
import types
import typing

from . import collector

if typing.TYPE_CHECKING:
    import pandas


class RunnerContext(abc.ABC):

    @property
    @abc.abstractmethod
    def force_first_train(self) -> bool:
        ...

    @property
    @abc.abstractmethod
    def is_local(self) -> bool:
        ...

    @property
    @abc.abstractmethod
    def is_determinism_check_enabled(self) -> bool:
        ...

    @abc.abstractmethod
    def report_determinism(self, deterministic: bool) -> None:
        ...

    @abc.abstractmethod
    def log(self, message: str, important=False, error=False) -> True:
        ...

    @abc.abstractmethod
    def execute(
        self,
        command: str,
        parameters: dict = None,
        return_prediction: typing.Union[bool, collector.PredictionCollector] = False
    ) -> "pandas.DataFrame":
        ...


class RunnerExecutorContext(abc.ABC):

    @property
    @abc.abstractmethod
    def is_local(self) -> bool:
        ...

    @abc.abstractmethod
    def trip_data_fuse(self) -> None:
        ...


class UserModule(abc.ABC):

    def get_function(self, name: str) -> typing.Callable:
        function = getattr(self.module, name, None)

        if not callable(function):
            raise ValueError(f"no `{name}` function found")

        return function

    @property
    @abc.abstractmethod
    def module(self) -> types.ModuleType:
        ...
