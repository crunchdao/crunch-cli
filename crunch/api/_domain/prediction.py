from typing import TYPE_CHECKING, Any

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.metric import Unit
    from crunch.api._domain.project import Project
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class Prediction(Model[int]):

    def __init__(
        self,
        project: "Project",
        attrs: "Attrs | None" = None,
        client: "Client | None" = None,
        collection: "PredictionCollection | None" = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._project = project

    @property
    def project(self):
        return self._project

    @property
    def name(self):
        return self._attrs["name"]

    @property
    def valid(self) -> bool | None:
        return self._attrs["valid"]

    @property
    def success(self) -> bool | None:
        return self._attrs["success"]

    @property
    def error_message(self) -> str | None:
        return self._attrs["error"]

    @property
    def managed(self) -> bool:
        return self._attrs["managed"]

    @property
    def deterministic(self) -> bool:
        return self._attrs["deterministic"]

    @property
    def primary_mean(self) -> float | None:
        return self._attrs.get("primaryMean")

    @property
    def primary_unit(self) -> "Unit | None":
        from crunch.api._domain.metric import Unit

        unit_attrs = self._attrs.get("primaryUnit")
        if unit_attrs is None:
            return None

        return Unit.from_dict(unit_attrs)  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType, reportAttributeAccessIssue]

    @property
    def scores(self):
        from crunch.api._domain.score import ScoreCollection

        return ScoreCollection(
            prediction=self,
            client=self._client
        )


class PredictionCollection(Collection[Prediction]):

    model = Prediction

    def __init__(
        self,
        project: "Project",
        client: "Client | None" = None
    ):
        super().__init__(client)

        self.project = project

    def get(
        self,
        id: int
    ) -> Prediction:
        return self.prepare_model(
            self._checked_client.api.get_prediction(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
                id
            )
        )

    def list(
        self
    ) -> list[Prediction]:
        return self.prepare_models(
            self._checked_client.api.list_predictions(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
            )
        )

    def prepare_model(self, attrs: "JsonValue | Prediction", *args: Any) -> Prediction:
        return super().prepare_model(
            attrs,
            self.project,
            *args
        )


class PredictionEndpointMixin(EndpointMixin):

    def list_predictions(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/predictions"
            ),
            json=True
        )

    def get_prediction(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        prediction_id: int
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/predictions/{prediction_id}"
            ),
            json=True
        )
