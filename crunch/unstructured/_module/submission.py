from typing import TYPE_CHECKING, Any, Callable, List, Optional

from crunch.api import SubmissionType
from crunch.unstructured._code_loader import CodeLoader, ModuleWrapper, NoCodeFoundError
from crunch.unstructured._execute import call_function
from crunch.unstructured._file import File
from crunch.unstructured.utils import find_file_by_path

if TYPE_CHECKING:
    from crunch_convert.requirements_txt import NamedRequirement, RequirementLanguage

__all__ = [
    "SubmissionModule",
]


def _load_requirements(
    language: "RequirementLanguage",
    submission_files: List[File],
) -> Optional[List["NamedRequirement"]]:
    from crunch_convert.requirements_txt import RequirementParseError, parse_from_file

    requirements_file = find_file_by_path(submission_files, language.txt_file_name)
    if not requirements_file:
        return None

    content = requirements_file.text
    if content is None:
        return None

    try:
        return parse_from_file(
            language=language,
            file_content=content,
        )
    except RequirementParseError:
        return None


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
        from crunch_convert.requirements_txt import RequirementLanguage

        call_function(
            self.get_check_function(ensure=True),
            kwargs={
                "submission_type": submission_type,
                "submission_files": submission_files,
                "model_files": model_files,
            },
            lazy_kwargs={
                "python_requirements": lambda: _load_requirements(
                    language=RequirementLanguage.PYTHON,
                    submission_files=submission_files,
                ),
                "r_requirements": lambda: _load_requirements(
                    language=RequirementLanguage.R,
                    submission_files=submission_files,
                ),
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
