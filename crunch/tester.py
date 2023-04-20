import os
import logging
import typing
import pandas
import requests
import psutil
import coloredlogs
import time

from . import utils, constants, ensure
from . import command

def _get_process_memory() -> int:
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss

def format_bytes(bytes: int):
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    suffix_index = 0

    while bytes >= 1024 and suffix_index < 8:
        bytes /= 1024
        suffix_index += 1

    return f"{bytes:,.2f} {suffixes[suffix_index]}"

def run(
    module: typing.Any,
    session: requests.Session,
    model_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
):
    coloredlogs.install(
        level=logging.INFO,
        fmt='%(asctime)s %(message)s',
        datefmt='%H:%M:%S',
    )

    logging.info('running local test')
    logging.warn("internet access isn't restricted, no check will be done")
    logging.info("")

    memory_before = _get_process_memory()
    start = time.time()

    data_process_handler = ensure.is_function(module, "data_process")
    train_handler = ensure.is_function(module, "train")
    infer_handler = ensure.is_function(module, "infer")

    (
        embargo,
        moon_column_name,
        x_train_path,
        y_train_path,
        x_test_path
    ) = command.download(session)

    x_train = utils.read(x_train_path)
    y_train = utils.read(y_train_path)
    x_test = utils.read(x_test_path)

    moons = x_test[moon_column_name].unique()
    moons.sort()

    for dataframe in [x_train, y_train, x_test]:
        dataframe.set_index(moon_column_name, drop=True, inplace=True)

    os.makedirs(model_directory_path, exist_ok=True)

    predictions: typing.List[pandas.DataFrame] = []

    for index, moon in enumerate(moons):
        train = (force_first_train and index == 0) or (train_frequency != 0 and moon % train_frequency == 0)

        logging.warn('---')
        logging.warn('loop: moon=%s train=%s (%s/%s)', moon, train, index + 1, len(moons))

        x_train_loop = x_train[x_train.index < moon - embargo].reset_index()
        y_train_loop = y_train[y_train.index < moon - embargo].reset_index()
        x_test_loop = x_test[x_test.index == moon].reset_index()

        logging.warn('handler: data_process(%s, %s, %s)', x_train_path, y_train_path, x_test_path)
        result = data_process_handler(x_train_loop, y_train_loop, x_test_loop)
        x_train_loop, y_train_loop, x_test_loop = ensure.return_data_process(result)

        if train:
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
    
    logging.warn('duration: time=%s', time.strftime("%H:%M:%S", time.gmtime(time.time() - start)))

    memory_after = _get_process_memory()
    logging.warn('memory: before="%s" after="%s" consumed="%s"', format_bytes(memory_before), format_bytes(memory_after), format_bytes(memory_after - memory_before))

    logging.warn('local test succesfully run!')

    return prediction
