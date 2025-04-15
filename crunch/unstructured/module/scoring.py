import typing

import pandas

from ... import api, scoring
from .. import code_loader, execute, utils


class ScoringModule:
    """
    Duck typing class that represent a `scoring.py` usable for a custom checker and scorer.
    """

    check: typing.Callable
    score: typing.Callable

    @staticmethod
    def load(loader: code_loader.CodeLoader):
        module = loader.load()

        assert hasattr(module, "check"), "`check` function is missing"
        assert hasattr(module, "score"), "`score` function is missing"

        return typing.cast(ScoringModule, module)


def check(
    scoring: ScoringModule,
    phase_type: api.PhaseType,
    metrics: typing.List[api.Metric],
    prediction: pandas.DataFrame,
    data_directory_path: str,
    logger=print,
):
    _call(
        scoring.check,
        phase_type,
        metrics,
        prediction,
        data_directory_path,
        logger,
    )


def score(
    scoring_module: ScoringModule,
    phase_type: api.PhaseType,
    metrics: typing.List[api.Metric],
    prediction: pandas.DataFrame,
    data_directory_path: str,
    logger=print,
) -> typing.Dict[int, scoring.ScoredMetric]:
    metric_ids = {
        metric.id
        for metric in metrics
    }

    results = _call(
        scoring_module.score,
        phase_type,
        metrics,
        prediction,
        data_directory_path,
        logger,
    )

    if not isinstance(results, dict):
        raise ValueError(f"return results must be a dict, got: {results.__class__}")

    for metric_id, scored_metric in results.items():
        if metric_id not in metric_ids:
            raise ValueError(f"metric id {metric_id} does not exists")

        if not isinstance(scored_metric, scoring.ScoredMetric):
            raise ValueError(f"results[{metric_id}] must be a ScoredMetric, got: {scored_metric.__class__}")

        value = scored_metric.value
        if not isinstance(value, float):
            raise ValueError(f"results[{metric_id}].value must be a float, got: {value.__class__}")

    return results


def _call(
    function: typing.Callable,
    phase_type: api.PhaseType,
    metrics: typing.List[api.Metric],
    prediction: pandas.DataFrame,
    data_directory_path: str,
    print=print,
):
    target_and_metrics = utils.group_metrics_by_target(metrics)

    target_names = list({
        target.name
        for target, _ in target_and_metrics
    })

    return execute.call_function(
        function,
        {
            "phase_type": phase_type,
            "prediction": prediction,
            "data_directory_path": data_directory_path,
            "target_names": target_names,
            "target_and_metrics": target_and_metrics,
        },
        print,
    )
