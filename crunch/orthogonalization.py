import tempfile
import typing

import pandas

from . import api


def orthogonalize(
    dataframe: pandas.DataFrame,
    prediction_name: typing.Optional[str] = None,
    round_number="@current",
):
    _, project = api.Client.from_project()

    with tempfile.NamedTemporaryFile() as tmp:
        dataframe.to_parquet(tmp.name, index=False)

        round = project.competition.rounds.get(round_number)
        phase = round.phases.get_submission()

        with open(tmp.name, "rb") as fd:
            files = [
                ("file", ('submission.parquet', fd, "application/vnd.apache.parquet"))
            ]

            prediction, scores = phase.orthogonalize(
                name=prediction_name or "null",
                files=files
            )

    return prediction, scores
