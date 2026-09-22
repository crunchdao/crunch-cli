from typing import TYPE_CHECKING, Any, List, Optional, Union

from crunch.api._resource import Collection, EndpointMixin
from crunch.api._resource import Model as BaseModel

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.model import Model
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class ModelFile(BaseModel[int]):

    def __init__(
        self,
        model: "Model",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["ModelFileCollection"] = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._model = model

    @property
    def resource_identifier(self) -> str:
        return self.name

    @property
    def model(self):
        return self._model

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def size(self) -> int:
        return self._attrs["size"]

    @property
    def mime_type(self) -> str:
        return self._attrs["mimeType"]


class ModelFileCollection(Collection[ModelFile]):

    model = ModelFile

    def __init__(
        self,
        model: "Model",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.model_ = model

    def list(
        self
    ) -> List[ModelFile]:
        return self.prepare_models(
            self._checked_client.api.list_model_files(
                self.model_.project.competition.id,
                self.model_.project.user_id,
                self.model_.project.name,
                self.model_.id,
            )
        )

    def prepare_model(self, attrs: Union["JsonValue", ModelFile], *args: Any) -> ModelFile:
        return super().prepare_model(
            attrs,
            self.model_,
            *args
        )


class ModelFileEndpointMixin(EndpointMixin):

    def list_model_files(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        model_id: int,
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/models/{model_id}/files"
            ),
            json=True
        )
