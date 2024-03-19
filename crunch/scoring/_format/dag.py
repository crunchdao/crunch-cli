import collections
import typing

import networkx
import pandas

from ... import api

MAPPING = collections.defaultdict(str)
MAPPING[(0, 0, 0, 1)] = 'Cause of X'
MAPPING[(1, 0, 0, 0)] = 'Consequence of X'
MAPPING[(0, 0, 1, 1)] = 'Confounder'
MAPPING[(1, 1, 0, 0)] = 'Collider'
MAPPING[(1, 0, 1, 0)] = 'Mediator'
MAPPING[(0, 0, 0, 0)] = 'Independent'
MAPPING[(0, 0, 1, 0)] = 'Cause of Y'
MAPPING[(0, 1, 0, 0)] = 'Consequence of Y'


# TODO too specific
FROM_COLUMN_NAME = "from"
TO_COLUMN_NAME = "to"
PARENT_COLUMN_NAME = "parent"


def get_labels(graph: networkx.Graph):
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
    Output: dictionary, with the edges as keys (excluding 'X' and 'Y'), and

    Example:

        g = random_DAG()
        g = shuffle_nodes(g)
        g = highlight_edge(g)
        get_labels(g)
    """

    assert 'X' in graph.nodes
    assert 'Y' in graph.nodes
    assert ('X', 'Y') in graph.edges
    assert networkx.is_directed_acyclic_graph(graph)

    A = networkx.adjacency_matrix(graph).todense()
    A = pandas.DataFrame(A, index=graph.nodes, columns=graph.nodes)

    result = {}
    for node in graph.nodes:
        if node in "XY":
            continue

        B = A.loc[('X', 'Y', node), :].loc[:, ('X', 'Y', node)]
        result[node] = MAPPING[tuple(B.values[[0, 1, 2, 2], [2, 2, 1, 0]])]

    return result


def process_prediction(
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
):
    prediction = prediction[[
        column_names.id,
        column_names.prediction
    ]].copy()

    # TODO too specific
    id_column = prediction[column_names.id].str.split('_')
    prediction[column_names.id] = id_column.str[:-2].str.join('_')
    prediction[FROM_COLUMN_NAME] = id_column.str[-2]
    prediction[TO_COLUMN_NAME] = id_column.str[-1]

    dataframes: typing.List[pandas.DataFrame] = []
    for key, group in prediction.groupby(column_names.id):
        group = group.pivot(
            index=FROM_COLUMN_NAME,
            columns=TO_COLUMN_NAME,
            values=column_names.prediction
        )

        graph = networkx.from_pandas_adjacency(group, create_using=networkx.DiGraph)
        labels = get_labels(graph)

        dataframe = pandas.Series(labels)\
            .sort_index()\
            .to_frame(column_names.prediction)\
            .reset_index(names=column_names.id)

        dataframe[column_names.moon] = key
        dataframes.append(dataframe)

    return pandas.concat(dataframes)


def process_y(
    y_test: typing.Dict[str, pandas.DataFrame],
    column_names: api.ColumnNames,
):
    dataframes: typing.List[pandas.DataFrame] = []
    for key, y in y_test.items():
        edges = y.set_index(PARENT_COLUMN_NAME).unstack().reset_index()
        edges.columns = [TO_COLUMN_NAME, FROM_COLUMN_NAME, column_names.target]

        group = edges.pivot(
            index=FROM_COLUMN_NAME,
            columns=TO_COLUMN_NAME,
            values=column_names.target
        )

        graph = networkx.from_pandas_adjacency(group, create_using=networkx.DiGraph)
        labels = get_labels(graph)

        dataframe = pandas.Series(labels)\
            .sort_index()\
            .to_frame(column_names.target)\
            .reset_index(names=column_names.id)

        dataframe[column_names.moon] = key
        dataframes.append(dataframe)

    return pandas.concat(dataframes)


def merge(
    y_test: typing.Dict[str, pandas.DataFrame],
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
):
    prediction = process_prediction(
        prediction,
        column_names,
    )

    y_test = process_y(
        y_test,
        column_names,
    )

    # print(prediction)
    # print(y_test)

    return pandas.merge(
        y_test,
        prediction,
        on=[
            column_names.moon,
            column_names.id
        ]
    )
