import typing

from .. import api, command, utils


def setup(
    clone_token: str,
    submission_number: str,
    directory: str,
    model_directory: str,
    force: bool,
    no_model: bool,
    show_quickstarters: bool,
    quickstarter_name: typing.Optional[str],
    show_notebook_quickstarters: bool,
    data_size_variant: api.SizeVariant,
):
    command.init(
        clone_token,
        directory,
        model_directory,
        force,
        data_size_variant,
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
        if show_quickstarters:
            command.quickstarter(
                quickstarter_name,
                show_notebook_quickstarters,
                True,
            )
        else:
            print(f"you appear to have never submitted code before")


def setup_notebook(
    clone_token: str,
    submission_number: str,
    directory: str,
    model_directory: str,
    no_model: bool,
    data_size_variant: api.SizeVariant,
):
    setup(
        clone_token,
        submission_number,
        directory,
        model_directory,
        True,
        no_model,
        False,
        None,
        False,
        data_size_variant,
    )
