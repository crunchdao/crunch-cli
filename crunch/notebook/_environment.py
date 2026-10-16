import json
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple

IPYNB = Dict[str, Any]


class NotebookEnvironment(ABC):

    @abstractmethod
    def display_name_and_gif(self, have_runs: bool) -> Tuple[str, str]:
        pass

    @abstractmethod
    def extract_ipynb(self) -> IPYNB:
        pass

    @staticmethod
    def detect() -> "NotebookEnvironment":
        if Kaggle.ENVVAR in os.environ:
            return Kaggle()

        if Colab.ENVVAR in os.environ:
            return Colab()

        return Generic()


class IPyNbNotAvailableError(Exception):
    pass


def _colab_gif(have_runs: bool) -> str:
    if not have_runs:
        return "download-and-submit-notebook-deployment.gif"

    return "download-and-submit-notebook.gif"


class Colab(NotebookEnvironment):

    ENVVAR = "COLAB_GPU"

    def display_name_and_gif(self, have_runs: bool) -> Tuple[str, str]:
        return "Colab", _colab_gif(have_runs)

    def extract_ipynb(self) -> IPYNB:
        try:
            from google.colab import _message  # pyright: ignore[reportUnknownVariableType, reportMissingImports]
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


class Kaggle(NotebookEnvironment):

    ENVVAR = "KAGGLE_KERNEL_RUN_TYPE"

    def display_name_and_gif(self, have_runs: bool) -> Tuple[str, str]:
        # TODO Record separate GIFs for deployments from Kaggle
        return "Kaggle", "download-and-submit-notebook-on-kaggle.gif"

    def extract_ipynb(self) -> IPYNB:
        try:
            from kaggle_session import UserSessionClient  # pyright: ignore[reportUnknownVariableType, reportMissingImports]
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


class Generic(NotebookEnvironment):

    def display_name_and_gif(self, have_runs: bool) -> Tuple[str, str]:
        return "your current environment", _colab_gif(have_runs)

    def extract_ipynb(self) -> IPYNB:
        raise IPyNbNotAvailableError(f"unknown notebook environment")
