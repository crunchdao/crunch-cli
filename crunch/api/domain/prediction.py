import typing

from .project import Project
from ..resource import Collection, Model


class Prediction(Model):

    def __init__(
        self,
        project: Project,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._project = project

    @property
    def project(self):
        return self._project

    @property
    def name(self):
        return self.attrs["name"]

    @property
    def scores(self):
        from .score import ScoreCollection

        return ScoreCollection(
            prediction=self,
            client=self.client
        )

    @property
    def score_summary(self) -> "ScoreSummary":
        from .score import ScoreSummary

        response = self.client.api.get_score_summary(
            self._project.competition.id,
            self._project.user_id,
            self.id
        )

        return ScoreSummary.from_dict(response)


class PredictionCollection(Collection):

    model = Prediction

    def __init__(
        self,
        project: Project,
        client=None
    ):
        super().__init__(client)

        self.project = project

    def __iter__(self) -> typing.Iterator[Prediction]:
        return super().__iter__()

    def get(
        self,
        id: int
    ) -> Prediction:
        response = self.client.api.get_prediction(
            self.project.competition.id,
            self.project.user_id,
            id
        )

        return self.prepare_model(
            response,
            self.project
        )

    def list(
        self
    ) -> typing.List[Prediction]:
        response = self.client.api.list_predictions(
            self.project.competition.id,
            self.project.user_id,
        )

        return [
            self.prepare_model(
                item,
                self.project
            )
            for item in response
        ]


class PredictionEndpointMixin:

    def list_predictions(
        self,
        competition_identifier,
        user_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/predictions"
            ),
            json=True
        )

    def get_prediction(
        self,
        competition_identifier,
        user_identifier,
        prediction_id
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/predictions/{prediction_id}"
            ),
            json=True
        )
