import logging
import typing

from .. import code_loader, execute


class RunnerModule(code_loader.ModuleWrapper):

    def get_load_data_function(self, ensure=True):
        return self._get_function("load_data", ensure)

    def load_data(
        self,
        data_directory_path: str,
        logger: logging.Logger,
    ) -> typing.Any:
        return execute.call_function(
            self.get_load_data_function(ensure=True),
            {
                "data_directory_path": data_directory_path,
                "logger": logger,
            },
        )

    def get_run_function(self, ensure=True):
        return self._get_function("run", ensure)

    def run(
        self,
        context: "RunnerContext",
        data_directory_path: str,
        model_directory_path: str,
        limit_traceback=True,
    ) -> typing.Any:
        return execute.call_function(
            self.get_run_function(ensure=True),
            {
                "context": context,
                "data_directory_path": data_directory_path,
                "model_directory_path": model_directory_path,
            },
            limit_traceback=limit_traceback,
        )

    def get_execute_function(self, ensure=True):
        return self._get_function("execute", ensure)

    def execute(
        self,
        context: "RunnerExecutorContext",
        module: "UserModule",
        data_directory_path: str,
        model_directory_path: str,
    ) -> typing.Any:
        return execute.call_function(
            self.get_execute_function(ensure=True),
            {
                "context": context,
                "module": module,
                "data_directory_path": data_directory_path,
                "model_directory_path": model_directory_path,
            },
        )

    @staticmethod
    def load(loader: code_loader.CodeLoader):
        try:
            module = loader.load()
            return RunnerModule(module)
        except code_loader.NoCodeFoundError:
            return None
