import os
from typing import Any, List, Sequence

import click

from crunch.api import Quickstarter, QuickstarterNotFoundException
from crunch.command._common import get_project
from crunch.utils import ascii_table, download


def quickstarter_list():
    project = get_project()
    quickstarters = project.competition.quickstarters.list()

    rows: List[Sequence[Any]] = []
    for quickstarter in quickstarters:
        rows.append(
            (
                quickstarter.name,
                quickstarter.title,
                _to_type(quickstarter),
                quickstarter.language.name,
            )
        )

    ascii_table(
        headers=["Name", "Title", "Type", "Language"],
        values=rows,
    )


def quickstarter_show(
    quickstarter_name: str,
):
    quickstarter = _get_quickstarter(quickstarter_name)

    print("Quickstarter Details:")
    print(f"  Name: {quickstarter.name}")
    print(f"  Title: {quickstarter.title}")
    print(f"  Type: {_to_type(quickstarter)}")
    print(f"  Language: {quickstarter.language.name}")
    print(f"  Authors:")
    for author in quickstarter.authors:
        print(f"    - {author.name}", f"({author.link})" if author.link else "")
    print(f"  Files:")
    for file in quickstarter.files:
        print(f"    - {file.name}")

    print("")
    print("Tips:")
    print(f"  To apply/download locally, use `crunch quickstarter apply {quickstarter.name}`")


def quickstarter_apply(
    quickstarter_name: str,
    overwrite: bool = False,
):
    quickstarter = _get_quickstarter(quickstarter_name)

    files = quickstarter.files

    for file in files:
        if os.path.exists(file.name) and not overwrite:
            print(f"apply: file already exists: {file.name}")
            print("apply: `--overwrite` to overwrite them.")
            raise click.Abort()

    for file in files:
        path = os.path.join(".", file.name)  # useful?
        download(file.url, path)

    if quickstarter.notebook:
        print("")
        print("Tips:")
        print(f"  This quickstarter is a notebook, to convert to a main.py, you can do `crunch convert {files[0].name}`")
        print(f"  Only useful for people that want to work with Python files. The documentation will be excluded.")


def _get_quickstarter(name: str):
    try:
        return get_project().competition.quickstarters.get(name)
    except QuickstarterNotFoundException:
        print(f"quickstarter: not found: {name}")
        raise click.Abort()


def _to_type(quickstarter: Quickstarter):
    if quickstarter.notebook:
        return "Notebook"

    return "Code"
