import abc
import types
import typing

from . import collector


class RunnerContext(abc.ABC):

    @property
    @abc.abstractmethod
    def is_local(self):
        ...

    @abc.abstractmethod
    def log(self, message: str, important=False, error=False):
        ...

    @abc.abstractmethod
    def execute(
        self,
        command: str,
        parameters: dict = None,
        return_prediction: typing.Union[bool, collector.PredictionCollector] = False
    ):
        ...


class RunnerExecutorContext(abc.ABC):

    @property
    @abc.abstractmethod
    def is_local(self):
        ...

    @abc.abstractmethod
    def trip_data_fuse(self):
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
