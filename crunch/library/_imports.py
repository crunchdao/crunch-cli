import logging
import os
import sys
from dataclasses import dataclass
from typing import Iterable, Mapping

import requests
from crunch_convert import RequirementLanguage
from crunch_convert.requirements_txt import CachedWhitelist, CrunchHubWhitelist, MultipleLibraryAliasCandidateException, RequirementParseError, Whitelist

import crunch.store as store
from crunch.api import WebUrlFormatter
from crunch.constants import REQUIREMENTS_TXT
from crunch.library._recorder import ImportLocation
from crunch.library._requirements import extract_from_requirements


# the runner installs its own crunch-cli
_RUNNER_MODULE_NAME = "crunch"


@dataclass(frozen=True)
class ImportProblem:
    alias: str
    location: ImportLocation
    names: list[str]
    """Empty when the alias is not whitelisted, several when it is ambiguous."""

    @property
    def is_whitelisted(self) -> bool:
        return len(self.names) != 0

    @property
    def is_imported_under_its_name(self) -> bool:
        """
        As `import catboost` for `catboost`, unlike `import sklearn` for `scikit-learn`.
        """

        return len(self.names) == 1 and self.names[0].lower() == self.alias.lower()


def find_import_problems(
    *,
    imports: Mapping[str, ImportLocation],
    requirement_names: Iterable[str],
    whitelist: Whitelist,
) -> list[ImportProblem]:
    """
    The whitelist gives the library of each import, since the import name is not always the library name: `sklearn` is from `scikit-learn`.
    """

    listed = {
        name.lower()
        for name in requirement_names
    }

    problems: list[ImportProblem] = []
    for alias, location in imports.items():
        # if "crunch-cli" itself, a stdlib or already in the requirements.txt file, skip it
        if alias == _RUNNER_MODULE_NAME or alias in sys.stdlib_module_names or alias.lower() in listed:
            continue

        try:
            library = whitelist.find_library(
                language=RequirementLanguage.PYTHON,
                alias=alias,
            )
        except MultipleLibraryAliasCandidateException as error:
            names = sorted(error.names)
        else:
            if library is not None and library.standard:
                continue

            names = [library.name] if library is not None else []

        if any(name.lower() in listed for name in names):
            continue

        problems.append(ImportProblem(alias, location, names))

    return problems


def scan_imports(
    *,
    imports: Mapping[str, ImportLocation],
    project_directory_path: str,
    competition_name: str,
    logger: logging.Logger,
    whitelist: Whitelist | None = None,
):
    try:
        requirement_names = extract_from_requirements(
            file_path=os.path.join(project_directory_path, REQUIREMENTS_TXT),
        )
    except RequirementParseError as error:
        logger.warning(f"{REQUIREMENTS_TXT}: {error}, the imports are not checked")
        return

    if whitelist is None:
        whitelist = CachedWhitelist(CrunchHubWhitelist(api_base_url=store.api_base_url))

    try:
        problems = find_import_problems(
            imports=imports,
            requirement_names=requirement_names,
            whitelist=whitelist,
        )
    except requests.RequestException as error:
        logger.warning(f"whitelist could not be reached, the imports are not checked: {error}")
        return

    web_url_formatter = WebUrlFormatter.from_env()

    for problem in problems:
        place = problem.location.format(project_directory_path)

        if not problem.is_whitelisted:
            logger.error(f"forbidden library: `{problem.alias}` is imported by {place} but is not whitelisted, the cloud run might fail")

            url = web_url_formatter.whitelisted_libraries(
                competition_name,
                request_alias=problem.alias,
                request_language=RequirementLanguage.PYTHON,
            )
            logger.error(f"  -> request to whitelist: {url}")
            continue

        imported_as = "" if problem.is_imported_under_its_name else f" as `{problem.alias}`"
        logger.warning(f"missing requirement: {' or '.join(problem.names)} is imported{imported_as} by {place} but is not in {REQUIREMENTS_TXT}, the cloud run might fail")
