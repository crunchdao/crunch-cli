import json
import os
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
    from ..convert import ConverterError

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
        raise click.Abort()
