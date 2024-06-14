import numpy
import pandas

from .. import api


def balanced_accuracy(
    group: pandas.DataFrame,
    target_column_name: str,
    prediction_column_name: str,
) -> float:
    import sklearn.metrics
    
    target = group[target_column_name]
    prediction = group[prediction_column_name]

    return sklearn.metrics.balanced_accuracy_score(
        target,
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


def fbeta_factory(beta: int):
    def _score(
        group: pandas.DataFrame,
        target_column_name: str,
        prediction_column_name: str,
    ) -> float:
        import sklearn.metrics

        prediction = group[prediction_column_name]

        threshold = prediction.median()
        prediction = (prediction > threshold).astype(int)

        return sklearn.metrics.fbeta_score(
            group[target_column_name],
            prediction,
            beta=beta
        )

    return _score


def precision(
    group: pandas.DataFrame,
    target_column_name: str,
    prediction_column_name: str,
) -> float:
    import sklearn.metrics

    prediction = group[prediction_column_name]

    threshold = prediction.median()
    prediction = (prediction > threshold).astype(int)

    return sklearn.metrics.precision_score(
        group[target_column_name],
        prediction
    )


def random(
    group: pandas.DataFrame,
    target_column_name: str,
    prediction_column_name: str,
) -> float:
    return numpy.random.random_sample()


def recall(
    group: pandas.DataFrame,
    target_column_name: str,
    prediction_column_name: str,
) -> float:
    import sklearn.metrics

    prediction = group[prediction_column_name]

    threshold = prediction.median()
    prediction = (prediction > threshold).astype(int)

    return sklearn.metrics.recall_score(
        group[target_column_name],
        prediction
    )


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


REGISTRY = {
    api.ScorerFunction.BALANCED_ACCURACY: balanced_accuracy,
    api.ScorerFunction.DOT_PRODUCT: dot_product,
    api.ScorerFunction.F1: fbeta_factory(beta=1),
    api.ScorerFunction.PRECISION: precision,
    api.ScorerFunction.RANDOM: random,
    api.ScorerFunction.RECALL: recall,
    api.ScorerFunction.SPEARMAN: spearman,
}
