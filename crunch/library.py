import logging
import os
from collections import defaultdict
from types import ModuleType
from typing import Dict, Optional, Set, cast, overload

import requirements

from . import api, constants, utils
from .api import Language
from .convert import extract_cells

__all__ = [
    "extract_from_requirements",
    "extract_from_notebook_modules",
    "find_forbidden",
    "scan",
]


def extract_from_requirements(
    file_path: str
) -> Set[str]:
    """
    Extracts package names from a requirements file.
    Does nothing if the file does not exist.
    """

    if not os.path.exists(file_path):
        return set()

    with open(file_path, 'r') as fd:
        return {
            cast(str, requirement.name)
            for requirement in requirements.parse(fd)
        }


def extract_from_notebook_modules(
    module: ModuleType
):
    """
    Extracts package names from a Jupyter notebook module.
    """

    cells = getattr(module, "In", [])
    cells = [
        {
            "metadata": {},
            "cell_type": "code",
            "source": cell if isinstance(cell, list) else cell.split("\n")
        }
        for cell in cells
    ]

    (
        _source_code,
        _embed_files,
        requirements,
    ) = extract_cells(
        cells,
        print=None,
        validate=False,
    )

    packages = defaultdict(set)
    for requirement in requirements:
        packages[requirement.language].add(requirement.alias)

    return packages


def find_forbidden(
    packages: Dict[Language, Set[str]],
    allow_standard: bool
):
    """
    Finds forbidden libraries by querying the API.
    """

    client = api.Client.from_env()

    whitelist: Dict[Language, Set[str]] = defaultdict(set)

    for language in packages.keys():
        libraries = client.libraries.list(
            standard=(
                None
                if allow_standard
                else False
            ),
            language=language,
        )

        for library in libraries:
            whitelisted = whitelist[language]

            whitelisted.add(library.name)
            whitelisted.update(library.aliases)

    return {
        language: package_set - whitelist[language]
        for language, package_set in packages.items()
    }


@overload
def scan(
    *,
    module: ModuleType,
    logger: logging.Logger = logging.getLogger()
):
    """
    Scans a module for forbidden libraries.
    """


@overload
def scan(
    *,
    requirements_file: Optional[str] = None,
    requirements_r_file: Optional[str] = None,
    logger: logging.Logger = logging.getLogger()
):
    """
    Scans requirements files for forbidden libraries.
    If no files are provided, uses the default ones from constants.
    If the files do not exist, they are ignored.
    """


def scan(
    module: Optional[ModuleType] = None,
    requirements_file: Optional[str] = None,
    requirements_r_file: Optional[str] = None,
    logger: Optional[logging.Logger] = None
):
    if module is not None:
        packages = extract_from_notebook_modules(module)
        forbidden = find_forbidden(packages, True)
        _log_forbidden(
            forbidden=forbidden,
            logger=logger,
            is_alias=True
        )

    else:
        if requirements_file is None:
            requirements_file = constants.REQUIREMENTS_TXT

        if requirements_r_file is None:
            requirements_r_file = constants.REQUIREMENTS_R_TXT

        packages = {
            Language.PYTHON: extract_from_requirements(requirements_file),
            Language.R: extract_from_requirements(requirements_r_file),
        }

        forbidden = find_forbidden(packages, False)
        _log_forbidden(
            forbidden=forbidden,
            logger=logger,
            is_alias=False
        )


def _log_forbidden(
    *,
    forbidden: Dict[Language, Set[str]],
    logger: Optional[logging.Logger],
    is_alias: bool,
):
    if logger is None:
        logger = logging.getLogger()

    if not len(forbidden):
        logger.warning('no forbidden library found')
        return

    competition_name = utils.try_get_competition_name()
    client = api.Client.from_env()
    query_param = 'requestAlias' if is_alias else 'requestName'

    show_language = len(forbidden) > 1

    for language, packages in forbidden.items():
        for package in packages:
            line = f'forbidden library: {package}'
            if show_language:
                line += f' ({language.name.lower()})'

            logger.error(line)

            if competition_name:
                url = client.format_web_url(f'/competitions/{competition_name}/resources/whitelisted-libraries?{query_param}={package}&requestLanguage={language.name}')
                logger.error(f'request to whitelist: {url}')
