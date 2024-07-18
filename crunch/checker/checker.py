import logging
import typing

import pandas

from .. import api, utils
from . import functions

CheckError = functions.CheckError


def _change_message_and_raise(error: CheckError, suffix: str):
    if not suffix:
        raise error

    raise CheckError(f"{error} on{suffix}") from error


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
        function_descriptor = functions.REGISTRY.get(function_name)
        if function_descriptor is None:
            logger.error(f"missing function - name={function_name.name}")
            continue

        def do_call(prediction_column_name: str):
            parameters = check.parameters

            if True:
                suffix = ""

                if moon is not None:
                    suffix = f" moon={moon}"

                if prediction_column_name is not None:
                    suffix = f" column=`{prediction_column_name}`"

                logger.info(f"check prediction - call={function_descriptor.name}({parameters}){suffix}")

            try:
                utils.smart_call(
                    function_descriptor.callable,
                    {
                        "prediction": prediction,
                        "example_prediction": example_prediction,
                        "id_column_name": column_names.id,
                        "moon_column_name": column_names.moon,
                        "prediction_column_name": prediction_column_name,
                        "column_names": column_names,
                        "competition_format": competition_format,
                    },
                    parameters
                )
            except CheckError as error:
                _change_message_and_raise(error, suffix)
            except Exception as exception:
                raise CheckError("failed to check") from exception

        if function_descriptor.column_based:
            for prediction_column_name in column_names.outputs:

                do_call(prediction_column_name)
        else:
            do_call(None)


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
