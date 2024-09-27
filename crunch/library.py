import ast
import logging
import typing

import requirements

from . import api, utils

DOT = "."


def strip_packages(
    name: str
):
    if name.startswith(DOT):
        return None  # just in case, but should not happen

    if DOT not in name:
        return name

    index = name.index(DOT)
    return name[:index]


def _convert_import(
    node: ast.AST
):
    packages = set()

    if isinstance(node, ast.Import):
        for alias in node.names:
            name = strip_packages(alias.name)
            if name:
                packages.add(name)
    elif isinstance(node, ast.ImportFrom):
        name = strip_packages(node.module)
        if name:
            packages.add(name)

    return packages


def extract_from_requirements(
    file_path: str
):
    with open(file_path, 'r') as fd:
        return {
            requirement.name
            for requirement in requirements.parse(fd)
        }


def extract_from_code_cells(
    cells: typing.List[typing.List[str]]
):
    packages = set()

    for index, lines in enumerate(cells):
        try:
            source = utils.strip_python_special_lines(lines)

            tree = ast.parse(source)

            for node in tree.body:
                packages.update(_convert_import(node))
        except Exception as exception:
            print(f"ignoring cell #{index + 1}: {str(exception)}")

    return packages


def extract_from_notebook_modules(
    module: typing.Any
):
    cells = getattr(module, "In", [])
    cells_and_lines = [
        cell if isinstance(cell, list) else cell.split("\n")
        for cell in cells
    ]

    return extract_from_code_cells(cells_and_lines)


def find_forbidden(
    packages: typing.Set[str],
    allow_standard: bool
):
    client = api.Client.from_env()

    libraries = client.libraries.list(
        include=api.LibraryListInclude.ALL if allow_standard else api.LibraryListInclude.THIRD_PARTY
    )

    whitelist = set()
    for library in libraries:
        whitelist.add(library.name)
        whitelist.update(library.aliases)

    return packages - whitelist


def scan(
    module: typing.Any = None,
    requirements_file: str = None
):
    forbidden = set()

    if module:
        packages = extract_from_notebook_modules(module)
        forbidden = find_forbidden(packages, True)
    elif requirements_file:
        packages = extract_from_requirements(requirements_file)
        forbidden = find_forbidden(packages, False)

    for package in forbidden:
        logging.error('forbidden library: %s', package)

    if not len(forbidden):
        logging.warning('no forbidden library found')
