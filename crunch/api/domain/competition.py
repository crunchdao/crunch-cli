from enum import Enum
from typing import Generator, Iterator, Optional
from crunch.api.identifiers import CompetitionIdentifierType
from crunch.api.resource import Collection, Model
from crunch.api.domain.enum_ import SplitKeyType


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


class Competition(Model):

    resource_identifier_attribute = "name"

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def format(self):
        return CompetitionFormat[self._attrs["format"]]

    @property
    def split_key_type(self):
        return SplitKeyType[self._attrs["splitKeyType"]]

    @property
    def external(self) -> bool:
        return self._attrs["external"]

    @property
    def encrypt_submissions(self) -> bool:
        return self._attrs["encryptSubmissions"]

    @property
    def checks(self):
        from .check import CheckCollection

        return CheckCollection(
            competition=self,
            client=self._client
        )

    @property
    def data_releases(self):
        from .data_release import DataReleaseCollection

        return DataReleaseCollection(
            competition=self,
            client=self._client
        )

    @property
    def metrics(self):
        from .metric import MetricCollection

        return MetricCollection(
            competition=self,
            target=None,
            client=self._client
        )

    @property
    def targets(self):
        from .target import TargetCollection

        return TargetCollection(
            competition=self,
            client=self._client
        )

    @property
    def projects(self):
        from .project import ProjectCollection

        return ProjectCollection(
            competition=self,
            client=self._client
        )

    @property
    def quickstarters(self):
        from .quickstarter import QuickstarterCollection

        return QuickstarterCollection(
            competition=self,
            client=self._client
        )

    @property
    def rounds(self):
        from .round import RoundCollection

        return RoundCollection(
            competition=self,
            client=self._client
        )

    @property
    def leaderboards(self):
        from .leaderboard import LeaderboardCollection

        return LeaderboardCollection(
            competition=self,
            client=self._client
        )


class CompetitionCollection(Collection[Competition]):

    model = Competition

    def __iter__(self) -> Iterator[Competition]:
        return super().__iter__()  # type: ignore

    def get(
        self,
        id_or_name: CompetitionIdentifierType
    ) -> Competition:
        return self.prepare_model(
            self._client.api.get_competition(
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
    ) -> Generator[Competition, None, None]:
        return self.prepare_models(
            self._client.api.list_competitions_v2(
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


class CompetitionEndpointMixin:

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
    ) -> Generator[dict, None, None]:
        params = {}

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
        identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{identifier}"
            ),
            json=True
        )
