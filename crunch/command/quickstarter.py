import os
from typing import Any, List, Sequence

import click

from crunch.api import Quickstarter, QuickstarterNotFoundException
from crunch.command._common import get_project
from crunch.utils import ascii_table, download


def quickstarter_list(
    *,
    show_tips: bool = False,
):
    project = get_project()
    quickstarters = project.competition.quickstarters.list()

    rows: List[Sequence[Any]] = []
    for quickstarter in quickstarters:
        rows.append(
            (
                quickstarter.name,
                repr(quickstarter.title),
                _to_type(quickstarter),
                quickstarter.language.name.lower(),
            )
        )

    print("quickstarters:")
    ascii_table(
        headers=["name", "title", "type", "language"],
        values=rows,
    )

    if show_tips:
        print()
        print("tips:")
        print(f"  - To show details, use `crunch quickstarter show <name>`.")
        print(f"  - To apply/download locally, use `crunch quickstarter apply <name>`.")


def quickstarter_show(
    *,
    quickstarter_name: str,
    show_tips: bool = False,
):
    quickstarter = _get_quickstarter(quickstarter_name)

    print("quickstarter:")
    print(f"  name: {quickstarter.name}")
    print(f"  title: {quickstarter.title!r}")
    print(f"  type: {_to_type(quickstarter)}")
    print(f"  language: {quickstarter.language.name.lower()}")
    print(f"  authors:")
    for author in quickstarter.authors:
        print(f"    - {author.name!r}", f"({author.link})" if author.link else "")
    print(f"  files:")
    for file in quickstarter.files:
        print(f"    - {file.name}")

    if show_tips:
        print()
        print("tips:")
        print(f"  - To apply/download locally, use `crunch quickstarter apply {quickstarter.name}`.")


def quickstarter_apply(
    *,
    quickstarter_name: str,
    overwrite: bool = False,
    show_tips: bool = False,
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

    if show_tips and quickstarter.notebook:
        print()
        print("tips:")
        print(f"  - This quickstarter is a notebook, to convert to a main.py, you can do `crunch convert {files[0].name}`, but it only useful for people that want to work with python files as the documentation will be excluded.")


def _get_quickstarter(name: str):
    try:
        return get_project().competition.quickstarters.get(name)
    except QuickstarterNotFoundException:
        print(f"quickstarter: not found: {name}")
        raise click.Abort()


def _to_type(quickstarter: Quickstarter):
    if quickstarter.notebook:
        return "notebook"

    return "code"
