import logging
import os
from collections import defaultdict
from types import ModuleType
from typing import Any, Dict, List, Optional, Set, overload

from crunch_convert import RequirementLanguage
from crunch_convert.notebook import BadCellHandling, extract_from_cells
from crunch_convert.requirements_txt import CachedWhitelist, CrunchHubWhitelist, Whitelist, parse_from_file

from crunch.api import Client
from crunch.constants import REQUIREMENTS_R_TXT, REQUIREMENTS_TXT
from crunch.utils import try_get_competition_name

__all__ = [
    "extract_from_requirements",
    "extract_from_notebook_modules",
    "find_forbidden",
    "scan",
]


def extract_from_requirements(
    *,
    language: RequirementLanguage = RequirementLanguage.PYTHON,
    file_path: str,
) -> Set[str]:
    """
    Extracts package names from a requirements file.
    Does nothing if the file does not exist.
    """

    if not os.path.exists(file_path):
        return set()

    with open(file_path, 'r') as fd:
        requirements = parse_from_file(
            language=language,
            file_content=fd.read(),
        )

        return {
            requirement.name
            for requirement in requirements
            if requirement.language == language
        }


def extract_from_notebook_modules(
    *,
    module: ModuleType
) -> Dict[RequirementLanguage, Set[str]]:
    """
    Extracts package names from a Jupyter notebook module.
    """

    cells = getattr(module, "In", [])
    cells: List[Dict[str, Any]] = [
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

    packages: Dict[RequirementLanguage, Set[str]] = defaultdict(set)
    for requirement in flatten.requirements:
        packages[requirement.language].add(requirement.alias)

    return packages


def find_forbidden(
    *,
    packages: Dict[RequirementLanguage, Set[str]],
    is_alias: bool,
    whitelist: Optional[Whitelist] = None,
):
    """
    Finds forbidden libraries by querying the API.
    """

    if whitelist is None:
        whitelist = CrunchHubWhitelist()
    if not isinstance(whitelist, CachedWhitelist):
        whitelist = CachedWhitelist(whitelist)

    return {
        language: _find_forbidden(
            language=language,
            packages=names_or_aliases,
            is_alias=is_alias,
            whitelist=whitelist,
        )
        for language, names_or_aliases in packages.items()
    }


def _find_forbidden(
    *,
    language: RequirementLanguage,
    packages: Set[str],
    is_alias: bool,
    whitelist: Whitelist,
):
    forbidden: Set[str] = set()

    for package in packages:
        if is_alias:
            library = whitelist.find_library(
                language=language,
                alias=package,
            )
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

    return forbidden


@overload
def scan(
    *,
    module: ModuleType,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Scans a module for forbidden libraries.
    """


@overload
def scan(
    *,
    requirements_file: Optional[str] = None,
    requirements_r_file: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Scans requirements files for forbidden libraries.
    If no files are provided, uses the default ones from constants.
    If the files do not exist, they are ignored.
    """


def scan(
    module: Optional[ModuleType] = None,
    requirements_file: Optional[str] = None,
    requirements_r_file: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
):
    if module is not None:
        packages = extract_from_notebook_modules(
            module=module,
        )

        forbidden = find_forbidden(
            packages=packages,
            is_alias=True,
        )

        _log_forbidden(
            forbidden=forbidden,
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

        forbidden = find_forbidden(
            packages=packages,
            is_alias=False,
        )

        _log_forbidden(
            forbidden=forbidden,
            logger=logger,
            is_alias=False,
        )


def _log_forbidden(
    *,
    forbidden: Dict[RequirementLanguage, Set[str]],
    logger: Optional[logging.Logger],
    is_alias: bool,
):
    if logger is None:
        logger = logging.getLogger()

    if not len(forbidden):
        logger.warning('no forbidden library found')
        return

    competition_name = try_get_competition_name()
    client = Client.from_env()
    query_param = 'requestAlias' if is_alias else 'requestName'

    for language, packages in forbidden.items():
        for package in packages:
            logger.error(f'forbidden library: {package} ({language.name.lower()})')

            if competition_name:
                url = client.format_web_url(f'/competitions/{competition_name}/resources/whitelisted-libraries?{query_param}={package}&requestLanguage={language.name}')
                logger.error(f'request to whitelist: {url}')
