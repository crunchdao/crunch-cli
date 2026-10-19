import ast
import json
import os
import queue
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Literal, Optional, Tuple, overload

import psutil

if TYPE_CHECKING:
    from jupyter_client import BlockingKernelClient  # pyright: ignore[reportPrivateImportUsage]

IPyNb = Dict[str, Any]


class IPyNbNotAvailableError(Exception):
    pass


class NotebookSecretNotAccessibleError(Exception):
    pass


class NotebookEnvironment(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def display_name_and_gif(self, have_runs: bool) -> Tuple[str, str]:
        pass

    @abstractmethod
    def extract_ipynb(self) -> IPyNb:
        pass

    @abstractmethod
    def get_secret(self, name: str) -> str:
        pass

    @staticmethod
    @overload
    def detect(none_if_not_found: Literal[False] = False) -> "NotebookEnvironment":
        ...

    @staticmethod
    @overload
    def detect(none_if_not_found: Literal[True]) -> Optional["NotebookEnvironment"]:
        ...

    @staticmethod
    def detect(none_if_not_found: bool = False) -> Optional["NotebookEnvironment"]:
        if Kaggle.ENVVAR in os.environ:
            return Kaggle()

        if Colab.ENVVAR in os.environ:
            return Colab()

        if _Debugging.ENVVAR in os.environ:
            return _Debugging()

        if none_if_not_found:
            return None

        return Generic()


def _colab_gif(have_runs: bool) -> str:
    if not have_runs:
        return "download-and-submit-notebook-deployment.gif"

    return "download-and-submit-notebook.gif"


class KernelNotReachableError(Exception):
    pass


class KernelExpressionError(Exception):

    def __init__(self, exception_name: str, exception_value: str):
        super().__init__(f"{exception_name}: {exception_value}")
        self.exception_name = exception_name
        self.exception_value = exception_value


class _Jupyter(NotebookEnvironment):

    CONTROL_REPLY_TIMEOUT_SECONDS = 60

    @staticmethod
    def _extract_connection_file_path(arguments: List[str]) -> Optional[str]:
        for index, argument in enumerate(arguments):
            if argument == "-f" and index + 1 < len(arguments):
                return arguments[index + 1]

            if argument.startswith(("-f=", "--f=")):
                return argument.split("=", 1)[1]

        return None

    @classmethod
    def _find_connection_file_path(cls) -> Optional[str]:
        current_process = psutil.Process()

        for process in [current_process, *current_process.parents()]:
            try:
                connection_file_path = cls._extract_connection_file_path(process.cmdline())
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue

            if connection_file_path is not None:
                return connection_file_path

        return None

    @contextmanager  # pyright: ignore[reportDeprecated]
    def _connect_to_kernel(self) -> Iterator["BlockingKernelClient"]:
        from jupyter_client import BlockingKernelClient  # pyright: ignore[reportPrivateImportUsage]

        connection_file_path = self._find_connection_file_path()
        if connection_file_path is None:
            raise KernelNotReachableError("no Jupyter kernel found in the parent processes")

        kernel_client = BlockingKernelClient(connection_file=connection_file_path)
        kernel_client.load_connection_file()
        kernel_client.start_channels(shell=False, iopub=False, stdin=False, hb=False, control=True)

        try:
            yield kernel_client
        finally:
            kernel_client.stop_channels()

    def _execute_on_control_channel(
        self,
        code: str,
        user_expressions: Optional[Dict[str, str]] = None,
        silent: bool = True,
    ) -> Dict[str, Any]:
        with self._connect_to_kernel() as kernel_client:
            request = kernel_client.session.msg("execute_request", {
                "code": code,
                "silent": silent,
                "store_history": False,
                "user_expressions": user_expressions or {},
                "allow_stdin": False,
                "stop_on_error": False,
            })
            kernel_client.control_channel.send(request)

            while True:
                try:
                    reply = kernel_client.get_control_msg(timeout=self.CONTROL_REPLY_TIMEOUT_SECONDS)
                except queue.Empty:
                    raise KernelNotReachableError(f"no reply from the kernel after {self.CONTROL_REPLY_TIMEOUT_SECONDS} seconds")

                if reply["parent_header"].get("msg_id") == request["header"]["msg_id"]:
                    return reply["content"]

    def _evaluate_in_kernel(self, expression: str) -> Any:
        reply_content = self._execute_on_control_channel("", user_expressions={"result": expression})

        result = reply_content["user_expressions"]["result"]
        if result["status"] == "error":
            raise KernelExpressionError(result["ename"], result["evalue"])

        return ast.literal_eval(result["data"]["text/plain"])

    def _show_error_in_current_cell(self, exception_name: str, exception_value: str) -> bool:
        code = f"get_ipython()._showtraceback(type({exception_name!r}, (Exception,), {{}}), {exception_value!r}, [])"

        try:
            self._execute_on_control_channel(code, silent=False)
            return True
        except Exception:
            return False


class Colab(_Jupyter):

    ENVVAR = "COLAB_GPU"

    @property
    def name(self) -> str:
        return "colab"

    def display_name_and_gif(self, have_runs: bool) -> Tuple[str, str]:
        return "Colab", _colab_gif(have_runs)

    def extract_ipynb(self) -> IPyNb:
        try:
            from google.colab import _message  # type: ignore
        except ImportError as error:
            raise IPyNbNotAvailableError(f"could not import google.colab._message: {error}")

        try:
            response = _message.blocking_request("get_ipynb", request="", timeout_sec=5)  # type: ignore
        except Exception as error:
            raise IPyNbNotAvailableError(f"failed to request ipynb: {error}")

        if response is None:
            raise IPyNbNotAvailableError(f"google.colab._message.blocking_request did not answered")

        error = response.get("error")  # type: ignore
        if error is not None:
            raise IPyNbNotAvailableError(f"{error.get('type')}: {error.get('description')}")  # type: ignore

        ipynb = response.get("ipynb")  # type: ignore
        if ipynb is None:
            raise IPyNbNotAvailableError(f"missing ipynb, available keys are: {list(response.keys())}")  # type: ignore

        if ipynb.get("cells") is None:  # type: ignore
            raise IPyNbNotAvailableError(f"missing cells, available keys are: {list(ipynb.keys())}")  # type: ignore

        return ipynb  # type: ignore

    def get_secret(self, name: str) -> str:
        try:
            from google.colab import userdata as _  # type: ignore
        except ImportError as error:
            raise NotebookSecretNotAccessibleError(f"could not import google.colab.userdata: {error}")

        expression = f"__import__('google.colab.userdata', fromlist=['get']).get({name!r})"
        try:
            return self._evaluate_in_kernel(expression)
        except KernelNotReachableError as error:
            raise NotebookSecretNotAccessibleError(f"failed to access secret `{name}`: {error}") from error
        except KernelExpressionError as error:
            if error.exception_name == "NotebookAccessError":
                raise NotebookSecretNotAccessibleError(f"access to secret `{name}` not allowed: give access in Secrets (key icon on the left) and re-run this cell") from error

            if error.exception_name == "SecretNotFoundError":
                raise NotebookSecretNotAccessibleError(f"secret `{name}` not found: add it in Secrets (key icon on the left) and re-run this cell") from error

            if error.exception_name == "TimeoutException":
                # Colab's `!` loop reads the same stdin socket and may swallow the reply, even when access is granted
                raise NotebookSecretNotAccessibleError(f"no answer received for secret `{name}`: re-run this cell") from error

            raise NotebookSecretNotAccessibleError(f"failed to access secret `{name}`: {error}") from error


class Kaggle(_Jupyter):

    ENVVAR = "KAGGLE_KERNEL_RUN_TYPE"

    @property
    def name(self) -> str:
        return "kaggle"

    def display_name_and_gif(self, have_runs: bool) -> Tuple[str, str]:
        # TODO Record separate GIFs for deployments from Kaggle
        return "Kaggle", "download-and-submit-notebook-on-kaggle.gif"

    def extract_ipynb(self) -> IPyNb:
        try:
            from kaggle_session import UserSessionClient  # type: ignore
        except ImportError as error:
            raise IPyNbNotAvailableError(f"could not import kaggle_session.UserSessionClient: {error}")

        try:
            client = UserSessionClient()  # type: ignore
            response = client.get_exportable_ipynb()  # type: ignore
        except Exception as error:
            raise IPyNbNotAvailableError(f"failed to extract Kaggle notebook: {error}")

        source = response.get("sourceNullable")  # type: ignore
        if source is None:
            raise IPyNbNotAvailableError(f"missing sourceNullable, available keys are: {list(response.keys())}")  # type: ignore

        try:
            ipynb = json.loads(source)  # type: ignore
        except json.JSONDecodeError as error:
            raise IPyNbNotAvailableError(f"failed to parse sourceNullable as JSON: {error}")

        if ipynb.get("cells") is None:  # type: ignore
            raise IPyNbNotAvailableError(f"missing cells, available keys are: {list(ipynb.keys())}")  # type: ignore

        return ipynb  # type: ignore

    def get_secret(self, name: str) -> str:
        try:
            from kaggle_secrets import UserSecretsClient  # type: ignore
        except ImportError as error:
            raise NotebookSecretNotAccessibleError(f"could not import kaggle_secrets.UserSecretsClient: {error}")

        try:
            client = UserSecretsClient()  # type: ignore
            return client.get_secret(name)  # type: ignore
        except KernelNotReachableError as error:
            raise NotebookSecretNotAccessibleError(f"failed to access secret `{name}`: {error}") from error
        except Exception as error:
            # Kaggle's frontend opens the "attach secret" modal when an error output in the cell matches its message
            self._show_error_in_current_cell("BackendError", str(error))

            raise NotebookSecretNotAccessibleError(f"missing secret `{name}`: attach it via Add-ons > Secrets, then re-run this cell") from error


class Generic(NotebookEnvironment):

    @property
    def name(self) -> str:
        return "generic"

    def display_name_and_gif(self, have_runs: bool) -> Tuple[str, str]:
        return "your current environment", _colab_gif(have_runs)

    def extract_ipynb(self) -> IPyNb:
        raise IPyNbNotAvailableError(f"unknown notebook environment")

    def get_secret(self, name: str) -> str:
        value = os.environ.get(name)

        if value is None:
            raise NotebookSecretNotAccessibleError(f"envvar `{name}` is not set")

        return value


class _Debugging(NotebookEnvironment):

    ENVVAR = "CRUNCH_NOTEBOOK_ENVIRONMENT_DEBUGGING"

    @property
    def name(self) -> str:
        return "debugging"

    def display_name_and_gif(self, have_runs: bool) -> Tuple[str, str]:
        return "your debugging environment", _colab_gif(have_runs)

    def extract_ipynb(self) -> IPyNb:
        path = (os.getenv("CRUNCH_NOTEBOOK_ENVIRONMENT_DEBUGGING_NOTEBOOK") or input("Path to the notebook file: ")).strip()
        if not path:
            raise IPyNbNotAvailableError(f"no notebook file path provided")

        if not os.path.exists(path):
            raise IPyNbNotAvailableError(f"notebook file `{path}` does not exist")

        with open(path, "r", encoding="utf-8") as fd:
            source = fd.read()

        try:
            ipynb = json.loads(source)  # type: ignore
        except json.JSONDecodeError as error:
            raise IPyNbNotAvailableError(f"failed to parse sourceNullable as JSON: {error}")

        if ipynb.get("cells") is None:  # type: ignore
            raise IPyNbNotAvailableError(f"missing cells, available keys are: {list(ipynb.keys())}")  # type: ignore

        return ipynb  # type: ignore

    def get_secret(self, name: str) -> str:
        value = (os.getenv(f"CRUNCH_NOTEBOOK_ENVIRONMENT_DEBUGGING_SECRET_{name}") or input(f"{name}: ")).strip()

        if not value:
            raise NotebookSecretNotAccessibleError(f"value `{name}` is not set")

        return value
