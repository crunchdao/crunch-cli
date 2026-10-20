import logging
import os
from collections import defaultdict
from types import ModuleType
from typing import Any, NamedTuple, overload

import click
from crunch_convert import RequirementLanguage
from crunch_convert.notebook import BadCellHandling, extract_from_cells
from crunch_convert.requirements_txt import CachedWhitelist, CrunchHubWhitelist, MultipleLibraryAliasCandidateException, RequirementParseError, Whitelist, parse_from_file

import crunch.store as store
from crunch.api import WebUrlFormatter
from crunch.constants import REQUIREMENTS_R_TXT, REQUIREMENTS_TXT
from crunch.repository import Repository

__all__ = [
    "extract_from_requirements",
    "extract_from_notebook_modules",
    "find_problematic",
    "scan",
]


def extract_from_requirements(
    *,
    language: RequirementLanguage = RequirementLanguage.PYTHON,
    file_path: str,
) -> set[str]:
    """
    Extracts package names from a requirements file.
    Does nothing if the file does not exist.
    """

    if not os.path.exists(file_path):
        return set()

    with open(file_path, "r") as fd:
        file_content = fd.read()

    try:
        requirements = parse_from_file(
            language=language,
            file_content=file_content,
        )
    except RequirementParseError as error:
        print(f"{language.txt_file_name}: {error}")
        raise click.Abort()

    return {
        requirement.name
        for requirement in requirements
        if requirement.language == language
    }


def extract_from_notebook_modules(
    *,
    module: ModuleType
) -> dict[RequirementLanguage, set[str]]:
    """
    Extracts package names from a Jupyter notebook module.
    """

    cells = getattr(module, "In", [])
    cells: list[dict[str, Any]] = [
        {
            "metadata": {},
            "cell_type": "code",
            "source": cell if isinstance(cell, list) else str(cell).split("\n")
        }
        for cell in cells
    ]

    flatten = extract_from_cells(
        cells,
        print=None,
        validate=False,
        bad_cell_handling=BadCellHandling.IGNORE,
    )

    packages: dict[RequirementLanguage, set[str]] = defaultdict(set)
    for requirement in flatten.requirements:
        packages[requirement.language].add(requirement.alias)

    return packages


_AliasCollision = NamedTuple(
    "_AliasCollision",
    [
        ("alias", str),
        ("names", set[str]),
    ],
)

_Problems = NamedTuple(
    "_Problems",
    [
        ("forbidden", set[str]),
        ("alias_collisions", list[_AliasCollision]),
    ],
)


def find_problematic(
    *,
    packages: dict[RequirementLanguage, set[str]],
    is_alias: bool,
    whitelist: Whitelist | None = None,
):
    """
    Finds forbidden libraries by querying the API.
    """

    if whitelist is None:
        whitelist = CrunchHubWhitelist(
            api_base_url=store.api_base_url,
        )
    if not isinstance(whitelist, CachedWhitelist):
        whitelist = CachedWhitelist(whitelist)

    return {
        language: _find_problematic(
            language=language,
            packages=names_or_aliases,
            is_alias=is_alias,
            whitelist=whitelist,
        )
        for language, names_or_aliases in packages.items()
    }


def _find_problematic(
    *,
    language: RequirementLanguage,
    packages: set[str],
    is_alias: bool,
    whitelist: Whitelist,
):
    forbidden: set[str] = set()
    alias_collisions: list[_AliasCollision] = []

    for package in packages:
        if is_alias:
            try:
                library = whitelist.find_library(
                    language=language,
                    alias=package,
                )
            except MultipleLibraryAliasCandidateException as error:
                alias_collisions.append(_AliasCollision(error.alias, error.names))
                continue

        else:
            library = whitelist.find_library(
                language=language,
                name=package,
            )

        is_forbidden = (
            library is None or
            (library.standard and not is_alias)
        )

        if is_forbidden:
            forbidden.add(package)

    return _Problems(
        forbidden=forbidden,
        alias_collisions=alias_collisions,
    )


@overload
def scan(
    *,
    module: ModuleType,
    logger: logging.Logger | None = None,
) -> None:
    """
    Scans a module for forbidden libraries.
    """


@overload
def scan(
    *,
    requirements_file: str | None = None,
    requirements_r_file: str | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """
    Scans requirements files for forbidden libraries.
    If no files are provided, uses the default ones from constants.
    If the files do not exist, they are ignored.
    """


def scan(
    module: ModuleType | None = None,
    requirements_file: str | None = None,
    requirements_r_file: str | None = None,
    logger: logging.Logger | None = None,
):
    if module is not None:
        packages = extract_from_notebook_modules(
            module=module,
        )

        problems_by_language = find_problematic(
            packages=packages,
            is_alias=True,
        )

        _log_problems(
            problems_by_language=problems_by_language,
            logger=logger,
            is_alias=True,
        )

    else:
        if requirements_file is None:
            requirements_file = REQUIREMENTS_TXT

        if requirements_r_file is None:
            requirements_r_file = REQUIREMENTS_R_TXT

        packages = {
            RequirementLanguage.PYTHON: extract_from_requirements(
                language=RequirementLanguage.PYTHON,
                file_path=requirements_file,
            ),
            RequirementLanguage.R: extract_from_requirements(
                language=RequirementLanguage.R,
                file_path=requirements_r_file,
            ),
        }

        problems_by_language = find_problematic(
            packages=packages,
            is_alias=False,
        )

        _log_problems(
            problems_by_language=problems_by_language,
            logger=logger,
            is_alias=False,
        )


def _log_problems(
    *,
    problems_by_language: dict[RequirementLanguage, _Problems],
    logger: logging.Logger | None,
    is_alias: bool,
):
    if logger is None:
        logger = logging.getLogger()

    if not len(problems_by_language):
        logger.warning("no forbidden library found")
        return

    repository = Repository.try_open()
    competition_name = repository.get_project().competition_name if repository is not None else None
    web_url_formatter = WebUrlFormatter.from_env()

    show_warning = False

    for language, problems in problems_by_language.items():
        forbidden, alias_collisions = problems

        for package in forbidden:
            logger.error(f"forbidden library: {package} ({language.name.lower()})")

            if competition_name:
                url = web_url_formatter.whitelisted_libraries(
                    competition_name,
                    request_name=None if is_alias else package,
                    request_alias=package if is_alias else None,
                    request_language=language.name,
                )
                logger.error(f"  -> request to whitelist: {url}")

            show_warning = True

        for alias, names in alias_collisions:
            logger.error(f"alias collision: {alias} ({language.name.lower()}), specify which one you want to fix it:")

            for name in names:
                logger.error(f"  -> import {alias}  # {name} @latest")

            show_warning = True

    if show_warning:
        logger.error(f"note: detection is based on the previous execution of the cell; if you have already corrected the issue(s), please restart the kernel and run all cells again")
