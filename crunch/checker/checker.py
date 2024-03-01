import logging
import typing

import pandas

from .. import utils, api
from . import functions

CheckError = functions.CheckError


def _run_checks(
    checks: typing.List[api.Check],
    scope: api.CheckFunctionScope,
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    id_column_name: str,
    moon_column_name: str,
    prediction_column_name: str,
    moon: int,
):
    checks.sort(key=lambda x: x.order)
    
    for check in checks:
        if check.scope != scope:
            continue

        function_name = check.function
        function = functions.REGISTRY.get(function_name)
        if function is None:
            logging.error(f"missing function - name={function_name.name}")
            continue

        parameters = check.parameters
        logging.warn(f"check prediction - call={function.__name__}({parameters}) moon={moon}")

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
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    id_column_name: str,
    moon_column_name: str,
    prediction_column_name: str,
):
    _, project = api.Client.from_project()
    competition = project.competition
    checks = competition.checks.list()

    return run(
        checks,
        prediction,
        example_prediction,
        id_column_name,
        moon_column_name,
        prediction_column_name,
    )


def run(
    checks: typing.List[api.Check],
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    id_column_name: str,
    moon_column_name: str,
    prediction_column_name: str,
):
    _run_checks(
        checks,
        api.CheckFunctionScope.ROOT,
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
            checks,
            api.CheckFunctionScope.MOON,
            prediction_at_moon,
            example_prediction_at_moon,
            id_column_name,
            moon_column_name,
            prediction_column_name,
            moon,
        )
