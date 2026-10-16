import os
import sys
from typing import TYPE_CHECKING, Optional

import crunch.tester as tester
from crunch.api import CompetitionFormat, RoundIdentifierType
from crunch.constants import DEFAULT_USER_CODE_MODULE_NAME
from crunch.unstructured import RunnerModule, deduce_code_loader

if TYPE_CHECKING:
    from types import ModuleType

    from crunch.repository import Repository


def load_user_code(
    main_file_path: str,
    module_name: str = DEFAULT_USER_CODE_MODULE_NAME,
) -> "ModuleType":
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, main_file_path)
    module = importlib.util.module_from_spec(spec)  # pyright: ignore[reportArgumentType]

    sys.path.insert(0, os.getcwd())

    getpid = os.getpid  # avoid swap
    initial_pid = getpid()

    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)  # pyright: ignore[reportOptionalMemberAccess]
    except:
        sys.modules.pop(module_name, None)
        raise

    if getpid() != initial_pid:
        raise RuntimeError("fork detected while loading user code")

    return module


def test(
    repository: "Repository",
    main_file_path: str,
    model_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    round_number: RoundIdentifierType,
    has_gpu: bool,
    no_determinism_check: Optional[bool],
):
    _, project = repository.create_client()
    competition = project.competition.reload()

    runner_module = None
    if competition.format == CompetitionFormat.UNSTRUCTURED:
        loader = deduce_code_loader(competition_name=competition.name, file_name="runner")
        runner_module = RunnerModule.load(loader)

    module = load_user_code(main_file_path)

    tester.run(
        repository,
        module,
        runner_module,
        model_directory_path,
        force_first_train,
        train_frequency,
        round_number,
        competition,
        has_gpu,
        no_determinism_check,
        trace_exporter=None,
    )
