from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator

from crunch.api._base import ApiCollection, ApiEnum, ApiModel, Parent
from crunch.api._domain.enum_ import SplitKeyType

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._identifiers import CompetitionIdentifierType


class CompetitionFormat(ApiEnum):
    TIMESERIES = "TIMESERIES"
    DAG = "DAG"
    STREAM = "STREAM"
    SPATIAL = "SPATIAL"
    UNSTRUCTURED = "UNSTRUCTURED"

    @property
    def unstructured(self):
        return self == CompetitionFormat.UNSTRUCTURED


class CompetitionMode(ApiEnum):
    OFFLINE = "OFFLINE"
    REAL_TIME = "REAL_TIME"


class CompetitionStatus(ApiEnum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    LATE_SUBMISSIONS = "LATE_SUBMISSIONS"
    SCORING = "SCORING"
    CLOSED = "CLOSED"


@dataclass
class Competition(ApiModel):
    id: int
    name: str
    display_name: str
    short_description: str
    start: datetime
    end: datetime | None
    status: CompetitionStatus
    format: CompetitionFormat
    mode: CompetitionMode
    split_key_type: SplitKeyType
    url: str | None
    external: bool
    documentation_url: str | None
    notebook_url: str | None
    hosted_by_name: str
    prize_pool_short_text: str
    team_based: bool
    only_team_leader: bool
    max_team_size: int
    project_creation_limit: int
    encrypt_submissions: bool
    phala_key_url: str | None
    hide_train_frequency: bool
    hide_force_first_train: bool

    client = Parent["Client"]()

    def reload(self) -> "Competition":
        return self._update_from(self.client.competitions.get(self.name))

    @property
    def targets(self):
        from crunch.api._domain.target import TargetCollection
        return TargetCollection(self)

    @property
    def metrics(self):
        from crunch.api._domain.metric import CompetitionMetricCollection
        return CompetitionMetricCollection(self)

    @property
    def data_releases(self):
        from crunch.api._domain.data_release import DataReleaseCollection
        return DataReleaseCollection(self)

    @property
    def projects(self):
        from crunch.api._domain.project import ProjectCollection
        return ProjectCollection(self)

    @property
    def quickstarters(self):
        from crunch.api._domain.quickstarter import QuickstarterCollection
        return QuickstarterCollection(self)

    @property
    def rounds(self):
        from crunch.api._domain.round import RoundCollection
        return RoundCollection(self)

    @property
    def leaderboards(self):
        from crunch.api._domain.leaderboard import LeaderboardCollection
        return LeaderboardCollection(self)

    @property
    def library_requests(self):
        from crunch.api._domain.library import LibraryRequestCollection
        return LibraryRequestCollection(self)

    @property
    def runtime_definitions(self):
        from crunch.api._domain.runtime import RuntimeDefinitionCollection
        return RuntimeDefinitionCollection(self)


class CompetitionCollection(ApiCollection[Competition]):

    model = Competition

    def _attach(self, competition: Competition) -> Competition:
        competition.client = self.client
        return competition

    def get(self, /, identifier: "CompetitionIdentifierType") -> Competition:
        return self.from_dict(self.client.api.get_json(["v1", "competitions", identifier]))

    def iter(
        self,
        *,
        format: CompetitionFormat | None = None,
        mode: CompetitionMode | None = None,
        status: CompetitionStatus | None = None,
        continuous: bool | None = None,
        external: bool | None = None,
        featured: bool | None = None,
        organizer_name: str | None = None,
        team_based: bool | None = None,
    ) -> Iterator[Competition]:
        filters: dict[str, Any] = {
            "format": format.name if format is not None else None,
            "mode": mode.name if mode is not None else None,
            "status": status.name if status is not None else None,
            "continuous": continuous,
            "external": external,
            "featured": featured,
            "organizerName": organizer_name,
            "teamBased": team_based,
        }

        return self.from_dict_iterator(self.client.api.paginate_json(
            ["v2", "competitions"],
            params=filters,
            page_size=1000,
        ))
