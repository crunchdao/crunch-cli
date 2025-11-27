from typing import Any, Callable, List, Optional

from crunch.api import SubmissionType
from crunch.scoring import ScoredMetricDetail as ScoredMetricDetail
from crunch.unstructured.code_loader import CodeLoader, ModuleWrapper, NoCodeFoundError
from crunch.unstructured.execute import call_function
from crunch.unstructured.file import File

__all__ = [
    "SubmissionModule",
]


class SubmissionModule(ModuleWrapper):

    def get_check_function(
        self,
        *,
        ensure: bool = True
    ) -> Callable[..., None]:
        return self._get_function(
            name="check",
            ensure=ensure,
        )

    def check(
        self,
        *,
        submission_type: SubmissionType,
        submission_files: List[File],
        model_files: List[File],
        print: Optional[Callable[[Any], None]] = None,
    ):
        call_function(
            self.get_check_function(ensure=True),
            kwargs={
                "submission_type": submission_type,
                "submission_files": submission_files,
                "model_files": model_files,
            },
            print=print,
        )

    @staticmethod
    def load(loader: CodeLoader):
        try:
            module = loader.load()
            return SubmissionModule(module)
        except NoCodeFoundError:
            return None
