import os
import shutil
import typing

import click
import inquirer

from .. import api, constants, utils


def _dot_crunchdao(directory: str):
    return os.path.join(directory, constants.DOT_CRUNCHDAO_DIRECTORY)


def _delete_tree_if_exists(path: str):
    if os.path.exists(path):
        print(f"delete {path}")
        shutil.rmtree(path)


def _check_if_already_exists(directory: str, force: bool):
    if not os.path.exists(directory):
        return False

    if force:
        return True
    elif len(os.listdir(directory)):
        print(f"{directory}: directory not empty (use --force to override)")
        raise click.Abort()


def _select_quickstarter(
    client: api.Client,
    competition_name: str,
    quickstarter_name: typing.Optional[str],
    show_notebook_quickstarters: bool,
) -> typing.Optional[api.Quickstarter]:
    quickstarters = [
        *client.quickstarters.list(),
        *client.competitions.get(competition_name).quickstarters.list()
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


def _setup_quickstarter(
    directory: str,
    client: api.Client,
    competition_name: str,
    quickstarter_name: typing.Optional[str],
    show_notebook_quickstarters: bool,
):
    quickstarter = _select_quickstarter(
        client,
        competition_name,
        quickstarter_name,
        show_notebook_quickstarters
    )

    if quickstarter is None:
        print("No quickstarter available, leaving directory empty.")
        return

    for file in quickstarter.files:
        path = os.path.join(directory, file.name)
        utils.download(file.url, path)


def _setup_submission(urls: dict):
    for path, url in urls.items():
        utils.download(url, path)


def setup(
    clone_token: str,
    submission_number: str,
    competition_name: str,
    directory: str,
    model_directory: str,
    force: bool,
    no_model: bool,
    quickstarter_name: typing.Optional[str],
    show_notebook_quickstarters: bool,
):
    should_delete = _check_if_already_exists(directory, force)

    client = api.Client.from_env()

    project_token = client.project_tokens.upgrade(clone_token)

    dot_crunchdao_path = _dot_crunchdao(directory)
    if should_delete:
        _delete_tree_if_exists(dot_crunchdao_path)

    os.makedirs(dot_crunchdao_path, exist_ok=True)

    plain = project_token.plain
    user_id = project_token.project.user_id

    project_info = utils.ProjectInfo(
        competition_name,
        user_id
    )

    utils.write_project_info(project_info, directory)
    utils.write_token(plain, directory)

    os.chdir(directory)
    client, project = api.Client.from_project()

    try:
        raise api.NeverSubmittedException("x")
        urls = project.clone(
            submission_number=submission_number,
            include_model=not no_model,
        )
    except api.NeverSubmittedException:
        _setup_quickstarter(
            ".",  # already os.chdir
            client,
            competition_name,
            quickstarter_name,
            show_notebook_quickstarters
        )
    else:
        _setup_submission(urls)

    os.makedirs(model_directory, exist_ok=True)
