import os
import json
import ast
import astor
import re
import click
import typing

from .. import constants


def convert_cells(cells: typing.List[typing.Any]):
    module = ast.Module()
    module.body = []

    for index, cell in enumerate(cells):
        cell_id = cell["metadata"].get("id") or f"cell_{index}"

        def log(message):
            print(f"convert {cell_id} {message}")

        cell_type = cell["cell_type"]
        if cell_type != "code":
            log(f"skip since not code: {cell_type}")
            continue

        source = "\n".join(
            line
            for line in cell["source"]
            if not re.match(r"^\s*?(!|%|#)", line)
        )

        if not len(source):
            log(f"skip since empty (without !bash, %magic and #comment)")
            continue

        valid = 0
        tree = ast.parse(source)

        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef)):
                valid += 1
                module.body.append(node)

        if valid == 0:
            log(f"skip since no valid node")
        else:
            log(f"used {valid}/{len(tree.body)} node(s)")

    return astor.to_source(module)


def convert_cells_to_file(
    cells: typing.List[typing.Any],
    python_file_path: str,
    override: bool = False,
):
    source = convert_cells(cells)

    if python_file_path != constants.CONVERTED_MAIN_PY and not override and os.path.exists(python_file_path):
        override = click.prompt(
            f"file {python_file_path} already exists, override?",
            type=bool,
            default=False,
            prompt_suffix=" "
        )

        if not override:
            raise click.Abort()

    with open(python_file_path, "w") as fd:
        fd.write(source)


def convert(
    notebook_file_path: str,
    python_file_path: str,
    override: bool = False,
):
    with open(notebook_file_path) as fd:
        notebook = json.load(fd)

    convert_cells_to_file(
        notebook.get("cells", []),
        python_file_path,
        override
    )
