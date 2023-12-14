import logging

import pandas

from .. import utils
from . import functions

CheckError = functions.CheckError

_FUNCTIONS = {
    "COLUMNS_NAME": functions.columns_name,
    "NANS": functions.nans,
    "VALUES_BETWEEN": functions.values_between,
    "VALUES_ALLOWED": functions.values_allowed,
    "MOONS": functions.moons,
    "IDS": functions.ids_at_moon,
    "CONSTANTS": functions.constants_at_moon,
}


def _run_checks(
    checks: list,
    scope: str,
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    id_column_name: str,
    moon_column_name: str,
    prediction_column_name: str,
    moon: int,
):
    checks.sort(key=lambda x: x['order'])
    
    for check in checks:
        if check['scope'] != scope:
            continue

        function_name = check["function"]
        function = _FUNCTIONS.get(function_name)
        if function is None:
            logging.error(f"missing function - name={function_name}")
            continue

        parameters = check.get("parameters", {})
        logging.warn(
            f"check prediction - call={function.__name__}({parameters}) moon={moon}")

        try:
            utils.smart_call(function, {
                "prediction": prediction,
                "example_prediction": example_prediction,
                "id_column_name": id_column_name,
                "moon_column_name": moon_column_name,
                "prediction_column_name": prediction_column_name,
                "moon": moon,
                **parameters,
            })
        except CheckError:
            raise
        except Exception as exception:
            raise CheckError(
                "failed to check"
            ) from exception


def run_via_api(
    session: utils.CustomSession,
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    id_column_name: str,
    moon_column_name: str,
    prediction_column_name: str,
):
    project_info = utils.read_project_info()
    checks = session.get(
        f"/v1/competitions/{project_info.competition_name}/checks"
    ).json()

    return run(
        checks,
        prediction,
        example_prediction,
        id_column_name,
        moon_column_name,
        prediction_column_name,
    )


def run(
    checks: list,
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    id_column_name: str,
    moon_column_name: str,
    prediction_column_name: str,
):
    _run_checks(
        checks, "ROOT",
        prediction,
        example_prediction,
        id_column_name,
        moon_column_name,
        prediction_column_name,
        None
    )

    for moon in example_prediction[moon_column_name].unique():
        prediction_at_moon = prediction[prediction[moon_column_name] == moon]
        example_prediction_at_moon = example_prediction[example_prediction[moon_column_name] == moon]

        _run_checks(
            checks, "MOON",
            prediction_at_moon,
            example_prediction_at_moon,
            id_column_name,
            moon_column_name,
            prediction_column_name,
            moon,
        )
