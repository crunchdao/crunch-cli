from datetime import datetime
from typing import TYPE_CHECKING, Any, List, Optional, Union

from crunch.api._resource import Collection, EndpointMixin
from crunch.api._resource import Model as BaseModel

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.project import Project
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class Model(BaseModel[int]):

    def __init__(
        self,
        project: "Project",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["ModelCollection"] = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._project = project

    @property
    def project(self):
        return self._project

    @property
    def total_size(self) -> int:
        return self._attrs["totalSize"]

    @property
    def created_at(self) -> datetime:
        return datetime.fromisoformat(self._attrs["createdAt"])

    @property
    def files(self):
        from crunch.api._domain.model_file import ModelFileCollection

        return ModelFileCollection(self, self._client)


class ModelCollection(Collection[Model]):

    model = Model

    def __init__(
        self,
        project: "Project",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.project = project

    def get(
        self,
        id: int,
    ) -> Model:
        return self.prepare_model(
            self._checked_client.api.get_model(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
                id
            )
        )

    def list(
        self
    ) -> List[Model]:
        return self.prepare_models(
            self._checked_client.api.list_models(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
            )
        )

    def prepare_model(self, attrs: Union["JsonValue", Model], *args: Any) -> Model:
        return super().prepare_model(
            attrs,
            self.project,
            *args
        )


class ModelEndpointMixin(EndpointMixin):

    def list_models(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/models"
            ),
            json=True
        )

    def get_model(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        model_id: int
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/models/{model_id}"
            ),
            json=True
        )
