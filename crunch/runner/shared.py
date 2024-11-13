import pandas

from .. import api, container, utils


def split_streams(
    x_train: pandas.DataFrame,
    column_names: api.ColumnNames
):
    stream_names = set(column_names.target_names)
    side_column_name = column_names.side

    streams = []
    for stream_name, group in x_train.groupby(column_names.id):
        if stream_name not in stream_names:
            continue

        parts = utils.split_at_nans(group, side_column_name)
        for index, part in enumerate(parts):
            stream = container.CallableIterable.from_dataframe(
                part,
                side_column_name,
                container.StreamMessage
            )

            # stream.identifier = f"{stream_name}/{index}"

            streams.append(stream)

    return streams
