import logging
import typing

import pandas

from .. import api, utils
from . import functions

CheckError = functions.CheckError

def _filter_checks(
    checks: typing.List[api.Check],
    scope: api.CheckFunctionScope
):
    return [
        check
        for check in checks
        if check.scope == scope
    ]


def _run_checks(
    checks: typing.List[api.Check],
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
    competition_format: api.CompetitionFormat,
    moon: int,
    logger: logging.Logger,
):
    checks.sort(key=lambda x: x.order)

    for check in checks:
        function_name = check.function
        function = functions.REGISTRY.get(function_name)
        if function is None:
            logger.error(f"missing function - name={function_name.name}")
            continue

        parameters = check.parameters
        logger.info(f"check prediction - call={function.__name__}({parameters}) moon={moon}")

        try:
            utils.smart_call(function, {
                "prediction": prediction,
                "example_prediction": example_prediction,
                "id_column_name": column_names.id,
                "moon_column_name": column_names.moon,
                "prediction_column_name": column_names.prediction,
                "column_names": column_names,
                "competition_format": competition_format,
                **parameters,
            })
        except CheckError as error:
            if moon is None:
                raise

            raise CheckError(f"{error} on {column_names.moon}={moon}") from error
        except Exception as exception:
            raise CheckError(
                "failed to check"
            ) from exception


def run_via_api(
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
    logger: logging.Logger,
):
    _, project = api.Client.from_project()
    competition = project.competition.reload()
    checks = competition.checks.list()

    return run(
        checks,
        prediction,
        example_prediction,
        column_names,
        competition.format,
        logger,
    )


def run(
    checks: typing.List[api.Check],
    prediction: pandas.DataFrame,
    example_prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
    competition_format: api.CompetitionFormat,
    logger: logging.Logger,
):
    if not len(checks):
        return

    _run_checks(
        _filter_checks(checks, api.CheckFunctionScope.ROOT),
        prediction,
        example_prediction,
        column_names,
        competition_format,
        None,
        logger,
    )

    moon_checks = _filter_checks(checks, api.CheckFunctionScope.KEY)
    if not len(moon_checks):
        return

    moons = prediction[column_names.moon].unique()
    for moon in moons:
        prediction_at_moon = prediction[prediction[column_names.moon] == moon]
        example_prediction_at_moon = example_prediction[example_prediction[column_names.moon] == moon]

        _run_checks(
            moon_checks,
            prediction_at_moon,
            example_prediction_at_moon,
            column_names,
            competition_format,
            moon,
            logger,
        )
