from typing import TYPE_CHECKING, Any, Sequence

import click

from crunch import store
from crunch.api import NeverSubmittedException, RuntimeOptionStatus, SubmissionNotFoundException
from crunch.command._common import get_project, get_submission
from crunch.repository import Repository
from crunch.utils import ascii_table

if TYPE_CHECKING:
    from crunch.api import SubmissionIdentifierType
    from crunch.api._domain.runtime import RuntimeDefinition


def runtime_list(
    repository: Repository,
    *,
    submission_identifier: "SubmissionIdentifierType",
):
    project = get_project(repository)

    try:
        submission = project.submissions.get(submission_identifier)
    except NeverSubmittedException:
        submission = None
    except SubmissionNotFoundException:
        print(f"submission: not found: {submission_identifier}")
        raise click.Abort()

    if submission is not None:
        has_one_requestable = False

        rows: list[Sequence[Any]] = []
        for option in submission.runtime_options.iter():
            rows.append(
                (
                    *_to_runtime_definition_columns(option.definition),
                    f"{option.status.name}",
                )
            )

            if option.status == RuntimeOptionStatus.REQUESTABLE:
                has_one_requestable = True

        print("runtimes:")
        ascii_table(
            headers=["name", "display name", "ram", "cpu", "gpu", "status"],
            values=rows
        )

        if store.show_tips and has_one_requestable:
            print()
            print("tips:")
            print("  - Request a runtime option with `crunch runtime request <name> --justification <reason>`.")

    else:
        rows = list(map(
            _to_runtime_definition_columns,
            project.competition.runtime_definitions.iter(),
        ))

        ascii_table(
            headers=["name", "display name", "ram", "cpu", "gpu"],
            values=rows
        )

        if store.show_tips:
            print()
            print("tips:")
            print("  - You will only know eligibility for runtime options after submitting once.")


def _to_runtime_definition_columns(definition: "RuntimeDefinition") -> Sequence[Any]:
    gpu = definition.specification.gpu

    return (
        definition.name,
        definition.display_name,
        f"{definition.specification.memory.size} GB",
        f"{definition.specification.cpu.core_count} cores",
        f"{gpu.model} ({gpu.driver})" if gpu.model is not None else "(none)",
    )


def runtime_request(
    repository: Repository,
    *,
    runtime_option_name: str,
    submission_identifier: "SubmissionIdentifierType",
    justification: str,
):
    project = get_project(repository)

    submission = get_submission(project, submission_identifier)

    option = next(
        (
            option
            for option in submission.runtime_options.iter()
            if option.definition.name == runtime_option_name
        ),
        None
    )

    if option is None:
        print(f"runtime: not found: {runtime_option_name}")

        if store.show_tips:
            print()
            print("tips:")
            print("  - List the available runtimes with `crunch runtime list`.")

        raise click.Abort()

    if option.status == RuntimeOptionStatus.AVAILABLE:
        print(f"runtime: {runtime_option_name}: already available, no request needed")
        return

    if option.status == RuntimeOptionStatus.REQUESTED:
        print(f"runtime: {runtime_option_name}: already requested, pending review")
        return

    if option.status == RuntimeOptionStatus.BLOCKED:
        print(f"runtime: {runtime_option_name}: blocked, further requests are not allowed")
        raise click.Abort()

    runtime_access_request = submission.runtime_access_requests.create(
        definition_name=runtime_option_name,
        justification=justification,
    )

    print(f"request: created #{runtime_access_request.id} for {runtime_option_name} on submission #{submission.number} ({runtime_access_request.verdict.name.lower()})")

    if store.show_tips:
        print()
        print("tips:")
        print("  - Runtime requests are reviewed by the Crunch team, the user will receive the outcome via email.")
        print("  - Check the status with `crunch runtime list`.")
