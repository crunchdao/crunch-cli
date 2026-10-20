from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from crunch.api._base import ApiCollection, ApiModel, Parent

if TYPE_CHECKING:
    from crunch.api._domain.project import ProjectReference


@dataclass
class Model(ApiModel):
    id: int
    name: str
    total_size: int
    created_at: datetime

    project = Parent["ProjectReference"]()

    @property
    def files(self) -> list["ModelFile"]:
        project = self.project

        files = ModelFile.from_dict_array(self.project.client.api.get_json([
            "v3", "competitions", project.competition.id, "projects", project.user_id, project.name, "models", self.id, "files"
        ]))

        for file in files:
            file.model = self

        return files


class ModelCollection(ApiCollection[Model]):

    model = Model

    def __init__(self, project: "ProjectReference"):
        super().__init__(project.client)

        self.project = project

    def _attach(self, model: Model) -> Model:
        model.project = self.project
        return model

    def get(self, /, id: int) -> Model:
        return self.from_dict(self.client.api.get_json([
            "v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "models", id
        ]))

    def list(self) -> list[Model]:
        return self.from_dict_array(self.client.api.get_json([
            "v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "models"
        ]))


@dataclass
class ModelFile(ApiModel):
    name: str
    size: int
    mime_type: str

    model = Parent[Model]()
