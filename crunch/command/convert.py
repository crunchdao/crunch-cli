import json
import os
import textwrap
import typing

import click


def convert_cells_to_file(
    cells: typing.List[typing.Any],
    python_file_path: str,
    override: bool = False,
):
    from ..convert import extract_cells

    (
        source_code,
        # TODO Include them in the submission, prompt for replace?
        _embed_files,
        _requirements,
    ) = extract_cells(cells, print)

    if not override and os.path.exists(python_file_path):
        override = click.prompt(
            f"file {python_file_path} already exists, override?",
            type=bool,
            default=False,
            prompt_suffix=" "
        )

        if not override:
            raise click.Abort()

    with open(python_file_path, "w") as fd:
        fd.write(source_code)


def convert(
    notebook_file_path: str,
    python_file_path: str,
    override: bool = False,
):
    from ..convert import (ConverterError, InconsistantLibraryVersionError,
                           NotebookCellParseError)

    try:
        with open(notebook_file_path) as fd:
            notebook = json.load(fd)

        convert_cells_to_file(
            notebook.get("cells", []),
            python_file_path,
            override
        )
    except ConverterError as error:
        print(f"convert failed: {error}")

        if isinstance(error, NotebookCellParseError):
            print(f"  cell: {error.cell_id} ({error.cell_index})")
            print(f"  source:")
            _print_indented(error.cell_source)
            print(f"  parser error:")
            _print_indented(error.parser_error)

        elif isinstance(error, InconsistantLibraryVersionError):
            print(f"  package name: {error.package_name}")
            print(f"  first version: {error.old}")
            print(f"  other version: {error.new}")

        raise click.Abort()


def _print_indented(text):
    indented = textwrap.indent(text, "   | ", lambda x: True)

    if indented.endswith("\n"):
        indented = indented[:-1]

    print(indented)
