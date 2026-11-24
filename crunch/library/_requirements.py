import os
from collections import defaultdict
from types import ModuleType
from typing import Any

from crunch_convert import RequirementLanguage
from crunch_convert.notebook import BadCellHandling, extract_from_cells
from crunch_convert.requirements_txt import parse_from_file


def extract_from_requirements(
    *,
    language: RequirementLanguage = RequirementLanguage.PYTHON,
    file_path: str,
) -> set[str]:
    """
    Extracts package names from a requirements file.
    Does nothing if the file does not exist.

    Raises:
        RequirementParseError: If the file is invalid.
    """

    if not os.path.exists(file_path):
        return set()

    with open(file_path, "r") as fd:
        file_content = fd.read()

    requirements = parse_from_file(
        language=language,
        file_content=file_content,
    )

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
