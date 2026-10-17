import json
import os
from abc import ABC, abstractmethod
from glob import glob
from typing import Any, Dict, Tuple

IPyNb = Dict[str, Any]


class IPyNbNotAvailableError(Exception):
    pass


class NotebookSecretNotAccessibleError(Exception):
    pass


class NotebookEnvironment(ABC):

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
    def detect() -> "NotebookEnvironment":
        if Kaggle.ENVVAR in os.environ:
            return Kaggle()

        if Colab.ENVVAR in os.environ:
            return Colab()

        if _Debugging.ENVVAR in os.environ:
            return _Debugging()

        return Generic()


def _colab_gif(have_runs: bool) -> str:
    if not have_runs:
        return "download-and-submit-notebook-deployment.gif"

    return "download-and-submit-notebook.gif"


class Colab(NotebookEnvironment):

    ENVVAR = "COLAB_GPU"

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
            from google.colab import userdata  # type: ignore
        except ImportError as error:
            raise NotebookSecretNotAccessibleError(f"could not import google.colab.userdata: {error}")

        try:
            return userdata.get(name)  # type: ignore
        except Exception as error:
            if error.__class__.__name__ == "NotebookAccessError":
                raise NotebookSecretNotAccessibleError(f"access to secret `{name}` not allowed: give access in Secrets (key icon on the left) and re-run this cell") from error

            if error.__class__.__name__ == "SecretNotFoundError":
                raise NotebookSecretNotAccessibleError(f"secret `{name}` not found: add it in Secrets (key icon on the left) and re-run this cell") from error

            raise NotebookSecretNotAccessibleError(f"failed to access secret `{name}`: {error}") from error


class Kaggle(NotebookEnvironment):

    ENVVAR = "KAGGLE_KERNEL_RUN_TYPE"

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
        except Exception as error:
            self._raise_in_current_cell(str(error))

            raise NotebookSecretNotAccessibleError(f"missing secret `{name}`: attach it via Add-ons > Secrets, then re-run this cell") from error

    def _raise_in_current_cell(self, error_message: str) -> bool:
        connection_file_path = max(glob("/root/.local/share/jupyter/runtime/kernel-*.json"), key=os.path.getmtime, default=None)
        if not connection_file_path:
            return False

        from jupyter_client import BlockingKernelClient  # pyright: ignore[reportPrivateImportUsage]
        kernel_client = BlockingKernelClient(connection_file=connection_file_path)
        kernel_client.load_connection_file()
        kernel_client.start_channels(shell=False, iopub=False, stdin=False, hb=False, control=True)
        try:
            request = kernel_client.session.msg("execute_request", {
                "code": (
                    "import kaggle_web_client\n"
                    f"get_ipython()._showtraceback(kaggle_web_client.BackendError, kaggle_web_client.BackendError({error_message!r}), [])\n"
                ),
                "silent": False,
                "store_history": False,
                "user_expressions": {},
                "allow_stdin": False,
                "stop_on_error": False,
            })
            kernel_client.control_channel.send(request)
            kernel_client.get_control_msg(timeout=10)  # safe: the control thread isn't blocked by the cell

            return True
        finally:
            kernel_client.stop_channels()


class Generic(NotebookEnvironment):

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

    def display_name_and_gif(self, have_runs: bool) -> Tuple[str, str]:
        return "your debugging environment", _colab_gif(have_runs)

    def extract_ipynb(self) -> IPyNb:
        path = input("Path to the notebook file: ").strip()
        if not path:
            raise IPyNbNotAvailableError(f"no notebook file path provided")

        if not os.path.exists(path):
            raise IPyNbNotAvailableError(f"notebook file `{path}` does not exist")

        with open(path, "r", encoding="utf-8") as fd:
            return fd.read()

    def get_secret(self, name: str) -> str:
        value = input(f"{name}: ").strip()

        import traceback
        traceback.print_stack()

        if not value:
            raise NotebookSecretNotAccessibleError(f"value `{name}` is not set")

        return value
