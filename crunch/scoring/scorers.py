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


def meta__execution_time(
    group: pandas.DataFrame,
    target_column_name: None,
    prediction_column_name: str,
):
    return group[prediction_column_name].mean()


def custom__mid_one__profit_and_loss_with_transaction_cost(
    group: pandas.DataFrame,
    target_column_name: str,
    prediction_column_name: str,
):
    EPSILON = 0.0025

    profit_and_loss = group[target_column_name] * numpy.sign(group[prediction_column_name])
    transactions_cost = (group[prediction_column_name] != 0).sum() * EPSILON

    return profit_and_loss - transactions_cost


REGISTRY = {
    api.ScorerFunction.BALANCED_ACCURACY: balanced_accuracy,
    api.ScorerFunction.DOT_PRODUCT: dot_product,
    api.ScorerFunction.F1: fbeta_factory(beta=1),
    api.ScorerFunction.PRECISION: precision,
    api.ScorerFunction.RANDOM: random,
    api.ScorerFunction.RECALL: recall,
    api.ScorerFunction.SPEARMAN: spearman,

    api.ScorerFunction.META__EXECUTION_TIME: meta__execution_time,

    api.ScorerFunction.CUSTOM__MID_ONE__PROFIT_AND_LOSS_WITH_TRANSACTION_COST: custom__mid_one__profit_and_loss_with_transaction_cost,
}
