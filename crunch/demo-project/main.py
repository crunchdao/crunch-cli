"""
This is a basic example of what you need to do to participate to the tournament.
The code will not have access to the internet (or any socket related operation).
"""

# Imports
import xgboost as xgb
import pandas as pd
import typing
import joblib
from pathlib import Path


def train(
    X_train: pd.DataFrame,
    y_train: pd.DataFrame,
    model_directory_path: str = "resources"
) -> None:
    """
    Do your model training here.
    At each retrain this function will have to save an updated version of
    the model under the model_directiory_path, as in the example below.
    Note: You can use other serialization methods than joblib.dump(), as
    long as it matches what reads the model in infer().

    Args:
        X_train, y_train: the data to train the model.
        model_directory_path: the path to save your updated model

    Returns:
        None
    """

    # basic xgboost regressor
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        max_depth=4,
        learning_rate=0.1,
        n_estimators=2,
        n_jobs=-1,
        colsample_bytree=0.05
    )

    # training the model
    print("training...")
    model.fit(X_train.iloc[:, 2:], y_train.iloc[:, 2:])

    # make sure that the train function correctly save the trained model
    # in the model_directory_path
    model_pathname = Path(model_directory_path) / "model.joblib"
    print(f"Saving model in {model_pathname}")
    joblib.dump(model, model_pathname)


def infer(
    X_test: pd.DataFrame,
    model_directory_path: str = "resources"
) -> pd.DataFrame:
    """
    Do your inference here.
    This function will load the model saved at the previous iteration and use
    it to produce your inference on the current date.
    It is mandatory to send your inferences with the ids so the system
    can match it correctly.

    Args:
        model_directory_path: the path to the directory to the directory in wich we will be saving your updated model.
        X_test: the independant  variables of the current date passed to your model.

    Returns:
        A dataframe (date, id, value) with the inferences of your model for the current date.
    """

    # loading the model saved by the train function at previous iteration
    model = joblib.load(Path(model_directory_path) / "model.joblib")

    # creating the predicted label dataframe with correct dates and ids
    y_test_predicted = X_test[["date", "id"]].copy()
    y_test_predicted["value"] = model.predict(X_test.iloc[:, 2:])

    return y_test_predicted
