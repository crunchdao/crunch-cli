import os
import importlib
import sys
import logging
import coloredlogs
import typing
import pandas

from .. import utils, ensure, constants
from .download import download


def test(
    session: utils.CustomSession,
    main_file_path: str,
    model_directory_path: str,
):
    coloredlogs.install(
        level=logging.INFO,
        fmt='%(asctime)s %(message)s',
        datefmt='%H:%M:%S',
    )

    logging.info('running local test')
    logging.warn("internet access isn't restricted, no check will be done")
    logging.info("")

    spec = importlib.util.spec_from_file_location("user_code", main_file_path)
    module = importlib.util.module_from_spec(spec)

    sys.path.insert(0, os.getcwd())
    spec.loader.exec_module(module)

    data_process_handler = ensure.is_function(module, "data_process")
    train_handler = ensure.is_function(module, "train")
    infer_handler = ensure.is_function(module, "infer")

    (
        embargo,
        moon_column_name,
        x_train_path,
        y_train_path,
        x_test_path
    ) = download(session)

    x_train = utils.read(x_train_path)
    y_train = utils.read(y_train_path)
    x_test = utils.read(x_test_path)

    moons = x_test[moon_column_name].unique()
    moons.sort()

    os.makedirs(model_directory_path, exist_ok=True)

    predictions: typing.List[pandas.DataFrame] = []

    for index, moon in enumerate(moons):
        logging.warn('---')
        logging.warn('moon: %s (%s/%s)', moon, index + 1, len(moons))

        x_train_loop = x_train[x_train[moon_column_name] < moon - embargo]
        y_train_loop = y_train[y_train[moon_column_name] < moon - embargo]
        x_test_loop = x_test[x_test[moon_column_name] == moon]
    
        logging.warn('handler: data_process(%s, %s, %s)', x_train_path, y_train_path, x_test_path)
        result = data_process_handler(x_train_loop, y_train_loop, x_test_loop)
        x_train_loop, y_train_loop, x_test_loop = ensure.return_data_process(result)

        logging.warn('handler: train(%s, %s, %s)', x_train_path, y_train_path, model_directory_path)
        train_handler(x_train_loop, y_train_loop, model_directory_path)

        logging.warn('handler: infer(%s, %s)', model_directory_path, x_test_path)
        prediction = infer_handler(model_directory_path, x_test_loop)
        prediction = ensure.return_infer(prediction)

        predictions.append(prediction)
    
    prediction = pandas.concat(predictions)
    prediction_path = os.path.join(constants.DOT_DATA_DIRECTORY, "prediction.csv")
    utils.write(prediction, prediction_path)

    logging.warn('prediction_path=%s', prediction_path)
    logging.warn('prediction=\n%s', prediction)
