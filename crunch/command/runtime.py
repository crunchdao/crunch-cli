from typing import TYPE_CHECKING, Any, List, Sequence

from crunch.api import RuntimeOptionStatus
from crunch.command._common import get_project
from crunch.utils import ascii_table

if TYPE_CHECKING:
    from crunch.api import SubmissionIdentifierType


def runtime_list(
    submission_identifier: "SubmissionIdentifierType",
    show_tips: bool = False,
):
    project = get_project()

    submission = project.submissions.get(submission_identifier)

    has_one_requestable = False

    rows: List[Sequence[Any]] = []
    for option in submission.runtime_options:
        definition = option.definition
        gpu = definition.specification.gpu

        rows.append(
            (
                definition.name,
                definition.display_name,
                f"{definition.specification.memory.size} GB",
                f"{definition.specification.cpu.core_count} cores",
                f"{gpu.model} ({gpu.driver})" if gpu.model is not None else "(none)",
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


def runtime_request(
    runtime_option_name: str,
    submission_identifier: "SubmissionIdentifierType",
    justification: str,
    show_tips: bool = False,
):
    raise NotImplementedError()
