import os
import importlib
import sys
import logging
import coloredlogs

from .. import utils, ensure, constants
from .download import download


def test(
    session: utils.CustomSession,
    main_file_path: str,
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

    x_train_path, y_train_path, x_test_path = download(session)

    x_train = utils.read(x_train_path)
    y_train = utils.read(y_train_path)
    x_test = utils.read(x_test_path)

    logging.warn('handler: data_process(%s, %s, %s)', x_train_path, y_train_path, x_test_path)
    result = data_process_handler(x_train, y_train, x_test)
    x_train, y_train, x_test = ensure.return_data_process(result)

    logging.warn('handler: train(%s, %s)', x_train_path, y_train_path)
    model = train_handler(x_train, y_train)
    model = ensure.return_train(model)
    
    model_path = os.path.join(constants.DOT_DATA_DIRECTORY, f"model.{utils.guess_extension(model)}")
    utils.write(model, model_path)

    logging.warn('model_path=%s', model_path)
    logging.warn('model=%s', model)

    logging.warn('handler: infer(%s, %s)', model_path, x_test_path)
    prediction = infer_handler(model, x_test)
    prediction = ensure.return_infer(prediction)
    
    prediction_path = os.path.join(constants.DOT_DATA_DIRECTORY, "prediction.csv")
    utils.write(prediction, prediction_path)

    logging.warn('prediction_path=%s', prediction_path)
    logging.warn('prediction=%s', prediction)
