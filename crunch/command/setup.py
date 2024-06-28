import typing

from .. import api, command, utils


def setup(
    clone_token: str,
    submission_number: str,
    competition_name: str,
    project_name: str,
    directory: str,
    model_directory: str,
    force: bool,
    no_model: bool,
    quickstarter_name: typing.Optional[str],
    show_notebook_quickstarters: bool,
):
    command.init(
        clone_token,
        competition_name,
        project_name,
        directory,
        model_directory,
        force
    )
    _, project = api.Client.from_project()

    try:
        urls = project.clone(
            submission_number=submission_number,
            include_model=not no_model,
        )

        for path, url in urls.items():
            utils.download(url, path)
    except api.NeverSubmittedException:
        command.quickstarter(
            quickstarter_name,
            show_notebook_quickstarters,
            True,
        )
