from typing import TYPE_CHECKING, Any, List, Sequence

import click

from crunch.api import NeverSubmittedException, RuntimeOptionStatus, SubmissionNotFoundException
from crunch.command._common import get_project
from crunch.utils import ascii_table

if TYPE_CHECKING:
    from crunch.api import SubmissionIdentifierType
    from crunch.api._domain.runtime import RuntimeDefinition


def runtime_list(
    *,
    submission_identifier: "SubmissionIdentifierType",
    show_tips: bool = False,
):
    project = get_project()

    try:
        submission = project.submissions.get(submission_identifier)
    except NeverSubmittedException:
        submission = None
    except SubmissionNotFoundException:
        print(f"submission: not found: {submission_identifier}")
        raise click.Abort()

    if submission is not None:
        has_one_requestable = False

        rows: List[Sequence[Any]] = []
        for option in submission.runtime_options:
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

        if show_tips and has_one_requestable:
            print()
            print("tips:")
            print("  - Request a runtime option with `crunch runtime request <name> --justification <reason>`.")

    else:
        rows = list(map(
            _to_runtime_definition_columns,
            project.competition.runtime_definitions.list(),
        ))

        ascii_table(
            headers=["name", "display name", "ram", "cpu", "gpu"],
            values=rows
        )

        if show_tips:
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
    *,
    runtime_option_name: str,
    submission_identifier: "SubmissionIdentifierType",
    justification: str,
    show_tips: bool = False,
):
    raise NotImplementedError()
