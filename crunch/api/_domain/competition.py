from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional
from datetime import datetime
from crunch.api._resource import Collection, EndpointMixin, Model
from crunch.api._domain.enum_ import SplitKeyType

if TYPE_CHECKING:
    from crunch.api._identifiers import CompetitionIdentifierType
    from crunch.api._resource import JsonValue


class CompetitionFormat(Enum):

    TIMESERIES = "TIMESERIES"
    DAG = "DAG"
    STREAM = "STREAM"
    SPATIAL = "SPATIAL"
    UNSTRUCTURED = "UNSTRUCTURED"

    def __repr__(self):
        return self.name

    @property
    def unstructured(self):
        return self == CompetitionFormat.UNSTRUCTURED


class CompetitionMode(Enum):

    OFFLINE = "OFFLINE"
    REAL_TIME = "REAL_TIME"

    def __repr__(self):
        return self.name


class CompetitionStatus(Enum):

    PENDING = "PENDING"
    OPEN = "OPEN"
    CLOSED = "CLOSED"

    def __repr__(self):
        return self.name


class Competition(Model[int]):

    @property
    def resource_identifier(self) -> str:
        return self.name

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def display_name(self) -> str:
        return self._attrs["displayName"]

    @property
    def short_description(self) -> str:
        return self._attrs["shortDescription"]

    @property
    def start(self) -> datetime:
        return datetime.fromisoformat(self._attrs["start"])

    @property
    def end(self) -> Optional[datetime]:
        end_string = self._attrs["end"]
        return datetime.fromisoformat(end_string) if end_string else None

    @property
    def status(self):
        return CompetitionStatus[self._attrs["status"]]

    @property
    def format(self):
        return CompetitionFormat[self._attrs["format"]]

    @property
    def mode(self):
        return CompetitionMode[self._attrs["mode"]]

    @property
    def split_key_type(self):
        return SplitKeyType[self._attrs["splitKeyType"]]

    @property
    def external(self) -> bool:
        return self._attrs["external"]

    @property
    def documentation_url(self) -> str:
        return self._attrs["documentationUrl"]

    @property
    def notebook_url(self) -> Optional[str]:
        return self._attrs["notebookUrl"]

    @property
    def hosted_by_name(self) -> str:
        return self._attrs["hostedByName"]

    @property
    def prize_pool_short_text(self) -> str:
        return self._attrs["prizePoolShortText"]

    @property
    def team_based(self) -> bool:
        return self._attrs["teamBased"]

    @property
    def only_team_leader(self) -> bool:
        return self._attrs["onlyTeamLeader"]

    @property
    def max_team_size(self) -> int:
        return self._attrs["maxTeamSize"]

    @property
    def project_creation_limit(self) -> int:
        return self._attrs["projectCreationLimit"]

    @property
    def encrypt_submissions(self) -> bool:
        return self._attrs["encryptSubmissions"]

    @property
    def phala_key_url(self) -> Optional[str]:
        return self._attrs["phalaKeyUrl"]

    @property
    def data_releases(self):
        from crunch.api._domain.data_release import DataReleaseCollection

        return DataReleaseCollection(
            competition=self,
            client=self._client
        )

    @property
    def metrics(self):
        from crunch.api._domain.metric import MetricCollection

        return MetricCollection(
            competition=self,
            target=None,
            client=self._client
        )

    @property
    def targets(self):
        from crunch.api._domain.target import TargetCollection

        return TargetCollection(
            competition=self,
            client=self._client
        )

    @property
    def projects(self):
        from crunch.api._domain.project import ProjectCollection

        return ProjectCollection(
            competition=self,
            client=self._client
        )

    @property
    def quickstarters(self):
        from crunch.api._domain.quickstarter import QuickstarterCollection

        return QuickstarterCollection(
            competition=self,
            client=self._client
        )

    @property
    def rounds(self):
        from crunch.api._domain.round import RoundCollection

        return RoundCollection(
            competition=self,
            client=self._client
        )

    @property
    def leaderboards(self):
        from crunch.api._domain.leaderboard import LeaderboardCollection

        return LeaderboardCollection(
            competition=self,
            client=self._client
        )


class CompetitionCollection(Collection[Competition]):

    model = Competition

    def get(
        self,
        id_or_name: "CompetitionIdentifierType"
    ) -> Competition:
        return self.prepare_model(
            self._checked_client.api.get_competition(
                id_or_name
            )
        )

    def list(
        self,
        *,
        format: Optional[CompetitionFormat] = None,
        mode: Optional[CompetitionMode] = None,
        status: Optional[CompetitionStatus] = None,
        continuous: Optional[bool] = None,
        external: Optional[bool] = None,
        featured: Optional[bool] = None,
        organizer_name: Optional[str] = None,
        team_based: Optional[bool] = None,
    ) -> Iterator[Competition]:
        return self.prepare_models(
            self._checked_client.api.list_competitions_v2(
                format=format,
                mode=mode,
                status=status,
                continuous=continuous,
                external=external,
                featured=featured,
                organizer_name=organizer_name,
                team_based=team_based,
            )
        )


class CompetitionEndpointMixin(EndpointMixin):

    def list_competitions_v2(
        self,
        format: Optional[CompetitionFormat],
        mode: Optional[CompetitionMode],
        status: Optional[CompetitionStatus],
        continuous: Optional[bool],
        external: Optional[bool],
        featured: Optional[bool],
        organizer_name: Optional[str],
        team_based: Optional[bool],
    ) -> Iterator["JsonValue"]:
        params: Dict[str, Any] = {}

        if format is not None:
            params["format"] = format.name

        if mode is not None:
            params["mode"] = mode.name

        if status is not None:
            params["status"] = status.name

        if continuous is not None:
            params["continuous"] = continuous

        if external is not None:
            params["external"] = external

        if featured is not None:
            params["featured"] = featured

        if organizer_name is not None:
            params["organizerName"] = organizer_name

        if team_based is not None:
            params["teamBased"] = team_based

        return self._paginated(
            lambda page_request: self.get(
                "/v2/competitions",
                params={
                    **params,
                    "page": page_request.number,
                    "size": page_request.size,
                },
            ),
            page_size=1000,
        )

    def get_competition(
        self,
        identifier: "CompetitionIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{identifier}"
            ),
            json=True
        )
