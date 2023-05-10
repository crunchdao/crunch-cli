"""
This is a basic example of what you need to do to participate to the tournament.
The code will not have access to the internet (or any socket related operation) so don't try to get access to external resources.
"""

import os
import typing

import joblib
import pandas as pd
import sklearn

import xgboost as xgb
from scipy import stats
from sklearn.feature_selection import VarianceThreshold
from sklearn.model_selection import train_test_split


def scorer(y_test: pd.DataFrame, y_pred: pd.DataFrame) -> None:
    score = (stats.spearmanr(y_test, y_pred)*100)[0]
    print(f"In sample spearman correlation {score}")


def train(x_train: pd.DataFrame, y_train: pd.DataFrame, model_directory_path: str) -> None:
    """
    Do your model training here.
    At each retrain this function will save an updated version of the model under the model_directiory_path.
    Make sure to use the correct operator to read and/or write your model.

    Args:
        x_train, y_train: the data post user processing and user feature engeneering done in the data_process function.
        model_directory_path: the path to the directory to the directory in wich we will saving your updated model

    Returns:
        None
    """

    # spliting training and test set
    print("spliting...")
    X_train, X_test, y_train, y_test = train_test_split(
        x_train,
        y_train,
        test_size=0.2,
        shuffle=False
    )

    # very shallow xgboost regressor
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        max_depth=4,
        learning_rate=0.01,
        n_estimators=2,
        n_jobs=-1,
        colsample_bytree=0.5
    )

    # training the model
    print("fiting...")
    model.fit(X_train.iloc[:, 2:], y_train.iloc[:, 2:])

    # testing model's Spearman score
    pred = model.predict(X_test.iloc[:, 2:])
    scorer(y_test.iloc[:, 2:], pred)

    # make sure that the train function correctly save the trained model in the model_directory_path
    joblib.dump(model, os.path.join(model_directory_path, "model.joblib"))


def infer(model_directory_path: str, x_test: pd.DataFrame) -> pd.DataFrame:
    """
    Do your inference here.
    This function will load the model saved at the previous iteration and use it to produce your inference on the current date.
    It is mandatory to send your inferences with the ids so the system can match it correctly.

    Args:
        model_directory_path: the path to the directory to the directory in wich we will be saving your updated model.
        x_test: the independant  variables of the current date passed to your model.

    Returns:
        A dataframe with the inferences of your model for the current date, including the ids columns.
    """

    # loading the model saved by the train function at previous iteration
    model = joblib.load(os.path.join(model_directory_path, "model.joblib"))

    # creating the predicted label dataframe without omiting to keep the ids and data
    predicted = x_test[["date", "id"]].copy()
    predicted["value"] = model.predict(x_test.iloc[:, 2:])

    return predicted
