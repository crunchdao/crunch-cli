from typing import Any, Literal, TypeAlias

import click

import crunch.api as api
import crunch.command as command
from crunch.repository import Repository
from crunch.utils import download

SetupSubmissionNumberType: TypeAlias = int | Literal["latest", "scratch"]


class SetupSubmissionNumberClickType(click.ParamType):  # pyright: ignore[reportMissingTypeArgument]
    name = "number"

    def get_metavar(self, param: click.Parameter, ctx: click.Context | None = None) -> str:  # pyright: ignore[reportIncompatibleMethodOverride]
        return "[latest|scratch|<number>]"

    def convert(self, value: Any, param: click.Parameter | None, ctx: click.Context | None) -> SetupSubmissionNumberType:
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
    *,
    clone_token: str,
    competition_name: str,
    project_name: str | None,
    submission_number: SetupSubmissionNumberType,
    directory: str,
    model_directory: str,
    force: bool,
    no_model: bool,
    data_size_variant: api.SizeVariant,
) -> Repository:
    repository = command.init(
        clone_token=clone_token,
        competition_name=competition_name,
        project_name=project_name,
        directory=directory,
        model_directory=model_directory,
        force=force,
        data_size_variant=data_size_variant,
    )

    _, project = repository.create_client()

    if submission_number == "scratch":
        print(f"setup: you decided to start from scratch, previous submission will not be downloaded")
        return repository

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
            download(url, path)

    except api.NeverSubmittedException:
        print(f"setup: you appear to have never submitted code before")

    except api.EncryptedSubmissionException:
        print(f"setup: you appear to have submitted an encrypted submission")

    return repository
