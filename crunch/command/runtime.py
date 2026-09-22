from typing import Any, List, Optional, Sequence

from crunch.command._common import get_project
from crunch.utils import ascii_table


def runtime_list(
    submission_number: Optional[int],
):
    project = get_project()

    # TODO Add @last, and ensure at least one submission
    submission = project.submissions.get(submission_number) if submission_number is not None else project.submissions.list()[-1]

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

        if option.status.name == "REQUESTABLE":
            has_one_requestable = True

    ascii_table(
        headers=["Name", "Display Name", "RAM", "CPU", "GPU", "Status"],
        values=rows
    )

    if has_one_requestable:
        print("")
        print("Tips:")
        print("  Request a runtime option with `crunch runtime request <name>`")


def runtime_request(
    runtime_option_name: str,
    submission_number: Optional[int],
    justification: str,
):
    raise NotImplementedError()
