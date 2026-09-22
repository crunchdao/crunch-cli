from typing import Any, Literal, Optional, Union

import click

from .. import api, command, utils

SetupSubmissionNumber = Union[int, Literal["latest", "scratch"]]


class SetupSubmissionNumberClickType(click.ParamType):  # pyright: ignore[reportMissingTypeArgument]
    name = "number"

    def convert(self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]):
        if "latest" == value:
            return "latest"

        if "scratch" == value:
            return "scratch"

        if isinstance(value, int) or value.isdigit():
            return int(value)

        self.fail(
            f"'{value}' is not a valid integer.",
            param,
            ctx
        )


def setup(
    clone_token: str,
    submission_number: SetupSubmissionNumber,
    directory: str,
    model_directory: str,
    force: bool,
    no_model: bool,
    show_quickstarters: bool,
    quickstarter_name: Optional[str],
    show_notebook_quickstarters: bool,
    data_size_variant: api.SizeVariant,
):
    command.init(
        clone_token=clone_token,
        directory=directory,
        model_directory=model_directory,
        force=force,
        data_size_variant=data_size_variant,
    )

    _, project = api.Client.from_project()

    if submission_number == "scratch":
        print(f"you decided to start from scratch, previous submission will not be downloaded")
        return

    try:
        urls = project.clone(
            submission_number=(
                None
                if submission_number == "latest"
                else submission_number
            ),
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

    except api.EncryptedSubmissionException:
        print(f"you appear to have submitted an encrypted submission")


def setup_notebook(
    clone_token: str,
    submission_number: SetupSubmissionNumber,
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
