import numpy
import pandas
import sklearn.metrics

from .. import api


def spearman(
    group: pandas.DataFrame,
    target_column_name: str,
    prediction_column_name: str,
) -> float:
    score = group[prediction_column_name].corr(
        group[target_column_name],
        method="spearman"
    )

    return score


def fbeta_factory(beta: int):
    def _score(
        group: pandas.DataFrame,
        target_column_name: str,
        prediction_column_name: str,
    ) -> float:
        prediction = group[prediction_column_name]

        threshold = prediction.median()
        prediction = (prediction > threshold).astype(int)

        return sklearn.metrics.fbeta_score(
            group[target_column_name],
            prediction,
            beta=beta
        )

    return _score


def recall(
    group: pandas.DataFrame,
    target_column_name: str,
    prediction_column_name: str,
) -> float:
    prediction = group[prediction_column_name]

    threshold = prediction.median()
    prediction = (prediction > threshold).astype(int)

    return sklearn.metrics.recall_score(
        group[target_column_name],
        prediction
    )


def precision(
    group: pandas.DataFrame,
    target_column_name: str,
    prediction_column_name: str,
) -> float:
    prediction = group[prediction_column_name]

    threshold = prediction.median()
    prediction = (prediction > threshold).astype(int)

    return sklearn.metrics.precision_score(
        group[target_column_name],
        prediction
    )


def dot_product(
    group: pandas.DataFrame,
    target_column_name: str,
    prediction_column_name: str,
) -> float:
    prediction = group[prediction_column_name]
    target = group[target_column_name]

    return numpy.dot(
        prediction,
        target,
    )


REGISTRY = {
    api.ScorerFunction.SPEARMAN: spearman,
    api.ScorerFunction.F1: fbeta_factory(beta=1),
    api.ScorerFunction.RECALL: recall,
    api.ScorerFunction.PRECISION: precision,
    api.ScorerFunction.DOT_PRODUCT: dot_product,
}
