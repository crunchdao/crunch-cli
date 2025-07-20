import importlib.util
import os
import sys
import types
import typing

from .. import api, constants, tester, unstructured, utils


def load_user_code(
    main_file_path: str,
    module_name=constants.DEFAULT_USER_CODE_MODULE_NAME,
) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, main_file_path)
    module = importlib.util.module_from_spec(spec)

    sys.path.insert(0, os.getcwd())
    
    with utils.limit_traceback():
        spec.loader.exec_module(module)

    sys.modules[module_name] = module

    return module


def test(
    main_file_path: str,
    model_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    round_number: str,
    has_gpu: bool,
    checks: bool,
    no_determinism_check: typing.Optional[bool],
):
    _, project = api.Client.from_project()
    competition = project.competition.reload()

    runner_module = None
    if competition.format == api.CompetitionFormat.UNSTRUCTURED:
        loader = unstructured.deduce_code_loader(competition.name, "runner")
        runner_module = unstructured.RunnerModule.load(loader)

    module = load_user_code(main_file_path)

    prediction = tester.run(
        module,
        runner_module,
        model_directory_path,
        force_first_train,
        train_frequency,
        round_number,
        competition,
        has_gpu,
        checks,
        no_determinism_check,
    )

    if prediction is not None:
        logger = tester.logger
        logger.warning('prediction=\n%s', prediction)
        logger.warning('')
        logger.warning('local test succesfully run!')
        logger.warning('')

    return prediction
