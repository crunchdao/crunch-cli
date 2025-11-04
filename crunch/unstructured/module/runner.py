from logging import Logger
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

from crunch.unstructured.code_loader import CodeLoader, ModuleWrapper, NoCodeFoundError
from crunch.unstructured.execute import call_function

if TYPE_CHECKING:
    from crunch.runner.cloud import RunnerContext
    from crunch.runner.unstructured import RunnerExecutorContext, UserModule

__all__ = [
    "RunnerModule",
]


class RunnerModule(ModuleWrapper):

    def get_load_data_function(
        self,
        *,
        ensure: bool = True,
    ) -> Callable[..., Any]:
        return self._get_function(
            name="load_data",
            ensure=ensure,
        )

    def load_data(
        self,
        *,
        data_directory_path: str,
        logger: Logger,
        print: Optional[Callable[[Any], None]] = None,
    ) -> Any:
        return call_function(
            self.get_load_data_function(ensure=True),
            {
                "data_directory_path": data_directory_path,
                "logger": logger,
            },
            print=print,
        )

    def get_run_function(
        self,
        *,
        ensure: bool = True,
    ):
        return self._get_function(
            name="run",
            ensure=ensure,
        )

    def run(
        self,
        *,
        context: "RunnerContext",
        data_directory_path: str,
        model_directory_path: str,
        prediction_directory_path: str,
        print: Optional[Callable[[Any], None]] = None,
    ) -> Any:
        return call_function(
            self.get_run_function(ensure=True),
            {
                "context": context,
                "data_directory_path": data_directory_path,
                "model_directory_path": model_directory_path,
                "prediction_directory_path": prediction_directory_path,
            },
            print=print,
        )

    def get_execute_function(
        self,
        *,
        ensure: bool = True,
    ):
        return self._get_function(
            name="execute",
            ensure=ensure,
        )

    def execute(
        self,
        *,
        context: "RunnerExecutorContext",
        module: "UserModule",
        data_directory_path: str,
        model_directory_path: str,
        prediction_directory_path: str,
        print: Optional[Callable[[Any], None]] = None,
    ) -> Dict[str, Callable[..., Any]]:
        return call_function(
            self.get_execute_function(ensure=True),
            {
                "context": context,
                "module": module,
                "data_directory_path": data_directory_path,
                "model_directory_path": model_directory_path,
                "prediction_directory_path": prediction_directory_path,
            },
            print=print,
        )

    @staticmethod
    def load(loader: CodeLoader):
        try:
            module = loader.load()
            return RunnerModule(module)
        except NoCodeFoundError:
            return None
