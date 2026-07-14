import importlib.util
import os
import sys
from types import ModuleType
from typing import TYPE_CHECKING, Optional
from crunch.api import Client, CompetitionFormat

if TYPE_CHECKING:
    from crunch.api import RoundIdentifierType

from .. import constants, tester, unstructured


def load_user_code(
    *,
    main_file_path: str,
    module_name: str = constants.DEFAULT_USER_CODE_MODULE_NAME,
) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, main_file_path)
    module = importlib.util.module_from_spec(spec)

    sys.path.insert(0, os.getcwd())

    getpid = os.getpid  # avoid swap
    initial_pid = getpid()

    spec.loader.exec_module(module)

    if getpid() != initial_pid:
        raise RuntimeError("fork detected while loading user code")

    sys.modules[module_name] = module

    return module


def test(
    *,
    main_file_path: str,
    model_directory_path: str,
    prediction_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    round_number: "RoundIdentifierType",
    has_gpu: bool,
    no_determinism_check: Optional[bool],
    verbose: bool = False,
):
    _, project = Client.from_project()
    competition = project.competition.reload()

    runner_module = None
    if competition.format == CompetitionFormat.UNSTRUCTURED:
        loader = unstructured.deduce_code_loader(competition_name=competition.name, file_name="runner")
        runner_module = unstructured.RunnerModule.load(loader)

    module = load_user_code(
        main_file_path=main_file_path,
    )

    tester.run(
        user_module=module,
        runner_module=runner_module,
        model_directory_path=model_directory_path,
        prediction_directory_path=prediction_directory_path,
        force_first_train=force_first_train,
        train_frequency=train_frequency,
        round_number=round_number,
        competition=competition,
        has_gpu=has_gpu,
        no_determinism_check=no_determinism_check,
        verbose=verbose,
        trace_exporter=None,
    )
