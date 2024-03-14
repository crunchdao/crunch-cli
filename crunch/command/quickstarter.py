import os
import typing

import click

from .. import api, utils


def _select(
    client: api.Client,
    competition_name: str,
    quickstarter_name: typing.Optional[str],
    show_notebook_quickstarters: bool,
) -> typing.Optional[api.Quickstarter]:
    import inquirer

    competition = client.competitions.get(competition_name)

    quickstarters = [
        *client.quickstarters(competition.format).list(),
        *competition.quickstarters.list()
    ]

    if quickstarter_name is not None:
        for quickstarter in quickstarters:
            if quickstarter_name in (quickstarter.name, quickstarter.title):
                return quickstarter

            print(f"{competition_name}: no quickstarter named `{quickstarter_name}`")
            raise click.Abort()

    if not show_notebook_quickstarters:
        quickstarters = list(filter(lambda x: not x.notebook, quickstarters))

    quickstarters_length = len(quickstarters)
    if quickstarters_length == 0:
        return None
    elif quickstarters_length == 1:
        return quickstarters[0]

    mapping = {}
    for quickstarter in quickstarters:
        key = f"{quickstarter.title} ({quickstarter.id})"

        if show_notebook_quickstarters:
            type = "Notebook" if quickstarter.notebook else "Code"
            key = f"[{type}] {key}"

        mapping[key] = quickstarter

    questions = [
        inquirer.List(
            'quickstarter',
            message="What quickstarter to use?",
            choices=mapping.keys(),
        ),
    ]

    answers = inquirer.prompt(questions, raise_keyboard_interrupt=True)
    return mapping[answers["quickstarter"]]


def quickstarter(
    name: typing.Optional[str],
    show_notebook: bool,
    overwrite: bool,
):
    client, project = api.Client.from_project()
    competition = project.competition

    quickstarter = _select(
        client,
        competition.name,
        name,
        show_notebook
    )

    if quickstarter is None:
        print("no quickstarter available, leaving directory empty")
        return

    files = quickstarter.files
    conflicts = [
        file.name
        for file in files
        if os.path.exists(file.name)
    ]

    if len(conflicts) and not overwrite:
        print("")
        print("---")
        print("Conflicting files that will be overwritten:")
        for name in conflicts:
            print(f"- {name}")

        print("")
        if not click.confirm("continue?"):
            raise click.Abort()

    from_ = f"competitions/{quickstarter.competition.name}" if quickstarter.competition else "generic"
    print(f"quickstarter {quickstarter.name} from {from_}")

    for file in files:
        path = os.path.join(".", file.name)  # useful?
        utils.download(file.url, path)
