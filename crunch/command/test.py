import importlib
import logging
import os
import sys

from .. import tester, utils, api


def test(
    main_file_path: str,
    model_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    round_number: str,
    has_gpu: bool,
    checks: bool,
):
    _, project = api.Client.from_project()
    competition = project.competition.reload()

    spec = importlib.util.spec_from_file_location("user_code", main_file_path)
    module = importlib.util.module_from_spec(spec)

    sys.path.insert(0, os.getcwd())
    spec.loader.exec_module(module)

    prediction = tester.run(
        module,
        model_directory_path,
        force_first_train,
        train_frequency,
        round_number,
        competition.format,
        has_gpu,
        checks,
    )

    if prediction is not None:
        logging.warn('prediction=\n%s', prediction)
        logging.warn('')
        logging.warn('local test succesfully run!')
        logging.warn('')

    return prediction
