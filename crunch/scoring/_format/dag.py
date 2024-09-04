import logging
import typing

import networkx
import numpy
import pandas
from tqdm.auto import tqdm

from ... import api, store

# TODO too specific
FROM_COLUMN_NAME = "from"
TO_COLUMN_NAME = "to"

MAPPING = numpy.full(16, -1)
MAPPING[0b0000] = 1  # 'Independent'
MAPPING[0b0001] = 2  # 'Cause of X'
MAPPING[0b0010] = 3  # 'Cause of Y'
MAPPING[0b0011] = 4  # 'Confounder'
MAPPING[0b0100] = 5  # 'Consequence of Y'
MAPPING[0b1000] = 6  # 'Consequence of X'
MAPPING[0b1010] = 7  # 'Mediator'
MAPPING[0b1100] = 8  # 'Collider'


class BadGraphError(ValueError):
    pass


def get_labels(
    key: typing.Union[str, int],
    pivoted: pandas.DataFrame,

    # do not change, local lookups are faster than globals
    mapping=MAPPING,
):
    """
    Classify the nodes of g as "collider", "confounder", etc., wrt the edge X→Y

    For each node i, we look at the role of i wrt X→Y, ignoring all other nodes.
    There are 8 possible cases:
    - Cause of X
    - Consequence of X
    - Confounder
    - Collider
    - Mediator
    - Independent
    - Cause of Y
    - Consequence of Y

    Caveat:
    - The notions of "confounder", "collider", etc. only make sense for small, textbook graphs.

    Input:  g: nx.DiGraph object, with an edge X→Y
    Output: list of tuple, with the edges as keys (excluding 'X' and 'Y'): (dataset_id, node, value)
    """

    nodes = pivoted.columns.to_list()
    graph = networkx.from_pandas_adjacency(pivoted, create_using=networkx.DiGraph)

    if 'X' not in nodes:
        raise BadGraphError(f"X not in nodes for dataset `{key}`")

    if 'Y' not in nodes:
        raise BadGraphError(f"Y not in nodes for dataset `{key}`")

    if ('X', 'Y') not in graph.edges:
        raise BadGraphError(f"X and/or Y not in edges for dataset `{key}`")

    if not networkx.is_directed_acyclic_graph(graph):
        raise BadGraphError(f"not a directed acyclic graph for dataset `{key}`")

    A = pivoted.values

    x_index = nodes.index('X')
    y_index = nodes.index('Y')

    return [
        (
            key,
            node,
            mapping[
                (A[x_index, index] << 3) |
                (A[y_index, index] << 2) |
                (A[index, y_index] << 1) |
                (A[index, x_index])
            ]
        )
        for index, node in enumerate(nodes)
        if node not in "XY"
    ]


def _process_prediction(
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
):
    # TODO support multiple target
    prediction_column_name = column_names.first_target.output

    prediction = prediction[[
        column_names.id,
        prediction_column_name
    ]].copy()

    # TODO too specific
    id_column = prediction[column_names.id].str.split('_')
    prediction[column_names.id] = id_column.str[:-2].str.join('_')
    prediction[FROM_COLUMN_NAME] = id_column.str[-2]
    prediction[TO_COLUMN_NAME] = id_column.str[-1]

    groups = prediction.groupby(column_names.id)
    if store.debug:
        groups = tqdm(groups)

    labelss: typing.List[tuple] = []
    for key, group in groups:
        pivoted = group.pivot(
            index=FROM_COLUMN_NAME,
            columns=TO_COLUMN_NAME,
            values=prediction_column_name
        )

        labels = get_labels(key, pivoted)
        labelss.extend(labels)

    return pandas.DataFrame(
        labelss,
        columns=[
            column_names.moon,
            column_names.id,
            prediction_column_name
        ]
    )


def _process_y(
    y_test: typing.Dict[typing.Union[str, int], pandas.DataFrame],
    column_names: api.ColumnNames,
):
    # TODO support multiple target
    target_column_name = column_names.first_target.input

    groups = y_test.items()
    if store.debug:
        groups = tqdm(groups)

    labelss: typing.List[tuple] = []
    for key, y in groups:
        labels = get_labels(key, y)
        labelss.extend(labels)

    return pandas.DataFrame(
        labelss,
        columns=[
            column_names.moon,
            column_names.id,
            target_column_name
        ]
    )


def merge(
    y_test: typing.Dict[typing.Union[str, int], pandas.DataFrame],
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
):
    prediction = _process_prediction(
        prediction,
        column_names,
    )

    y_test = _process_y(
        y_test,
        column_names,
    )

    return pandas.merge(
        y_test,
        prediction,
        on=[
            column_names.moon,
            column_names.id
        ]
    )
