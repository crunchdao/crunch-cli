import os
import tempfile
import importlib
import sys
import logging
import pandas
import coloredlogs
import click

from .. import utils


def ensure_function(module, name: str):
    if not hasattr(module, name):
        logging.error("no `%s` function found", name)
        raise click.Abort()

    return getattr(module, name)


def ensure_tuple(input):
    if not isinstance(input, tuple):
        logging.error("result is not a tuple")
        raise click.Abort()

    if len(input) != 3:
        logging.error("result tuple must be of length 3")
        raise click.Abort()


def ensure_dataframe(input, name: str):
    if not isinstance(input, pandas.DataFrame):
        logging.error(f"`%s` must be a dataframe", name)
        raise click.Abort()


def read(path: str) -> pandas.DataFrame:
    if path.endswith(".parquet"):
        return pandas.read_parquet(path)
    return pandas.read_csv(path)


def write(dataframe: pandas.DataFrame, path: str) -> None:
    if path.endswith(".parquet"):
        dataframe.to_parquet(path)
    else:
        dataframe.to_csv(path)


def test(
    main_file: str
):
    coloredlogs.install(
        level=logging.DEBUG,
        fmt='%(asctime)s %(message)s',
        datefmt='%H:%M:%S',
    )

    utils.change_root()

    logging.info('running local test')
    logging.warn("internet access isn't restricted, no check will be done")
    logging.info("")

    tmp = tempfile.TemporaryDirectory(prefix="test-")
    logging.info('tmp=%s', tmp.name)

    x_train_path = os.path.join(tmp.name, "x_train.csv")
    y_train_path = os.path.join(tmp.name, "y_train.csv")
    x_test_path = os.path.join(tmp.name, "x_test.csv")
    model_path = os.path.join(tmp.name, "model.csv")
    prediction_path = os.path.join(tmp.name, "prediction.csv")

    spec = importlib.util.spec_from_file_location("user_code", main_file)
    module = importlib.util.module_from_spec(spec)

    sys.path.insert(0, os.getcwd())
    spec.loader.exec_module(module)

    data_process_handler = ensure_function(module, "data_process")
    train_handler = ensure_function(module, "train")
    infer_handler = ensure_function(module, "infer")

    if True:
        dummy = pandas.DataFrame()
        for path in [x_train_path, y_train_path, x_test_path]:
            dummy.to_csv(path)

    x_train = read(x_train_path)
    y_train = read(y_train_path)
    x_test = read(x_test_path)

    logging.warn('handler: data_process(%s, %s, %s)', x_train, y_train, x_test)
    result = data_process_handler(x_train, y_train, x_test)
    ensure_tuple(result)

    x_train, y_train, x_test = result
    ensure_dataframe(x_train, "x_train")
    ensure_dataframe(y_train, "y_train")
    ensure_dataframe(x_test, "x_test")

    logging.warn('handler: train(%s, %s)', x_train, y_train)
    model = train_handler(x_train, y_train)
    ensure_dataframe(model, "model")
    write(model, model_path)

    logging.warn('model_path=%s', model_path)
    logging.warn('model=%s', model)

    logging.warn('handler: infer(%s, %s)', model_path, x_test)
    prediction = infer_handler(model, x_test)
    ensure_dataframe(prediction, "prediction")
    write(prediction, prediction_path)

    logging.warn('prediction_path=%s', prediction_path)
    logging.warn('prediction=%s', prediction)
