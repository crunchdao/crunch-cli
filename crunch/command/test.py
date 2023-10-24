import importlib
import logging
import os
import sys

from .. import tester, utils


def test(
    session: utils.CustomSession,
    main_file_path: str,
    model_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    round_number: str,
    has_gpu: bool,
):
    spec = importlib.util.spec_from_file_location("user_code", main_file_path)
    module = importlib.util.module_from_spec(spec)

    sys.path.insert(0, os.getcwd())
    spec.loader.exec_module(module)

    prediction = tester.run(
        module,
        session,
        model_directory_path,
        force_first_train,
        train_frequency,
        round_number,
        has_gpu,
    )

    logging.warn('prediction=\n%s', prediction)
    return prediction
