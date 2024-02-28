import pandas
import tempfile
import typing

from . import utils, store, api

def orthogonalize(
    dataframe: pandas.DataFrame,
    prediction_name: typing.Optional[str] = None,
    _session: typing.Optional[utils.CustomSession] = None,
):
    project_info = utils.read_project_info()
    push_token = utils.read_token()

    if _session is None:
        store.load_from_env()
        _session = store.session
    
    with tempfile.NamedTemporaryFile() as tmp:
        dataframe.to_parquet(tmp.name, index=False)

        with open(tmp.name, "rb") as fd:
            response = _session.post(
                f"/v1/competitions/{project_info.competition_name}/rounds/@current/phases/submission/orthogonalization",
                data={
                    "name": prediction_name,
                    "pushToken": push_token,
                    "tag": "USER_ORTHOGONALIZE",
                },
                files=[
                    ("file", ('submission.parquet', fd, "application/vnd.apache.parquet"))
                ]
            )

    result = response.json()

    prediction = api.Prediction.from_dict(result["prediction"])
    scores = [
        api.Score.from_dict(score)
        for score in result["scores"]
    ]

    return prediction, scores
