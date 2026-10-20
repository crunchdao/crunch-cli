from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypedDict

from crunch.api._base import ApiCollection, ApiModel, Parent

if TYPE_CHECKING:
    from crunch.api._domain.competition import Competition
    from crunch.api._domain.crunch import Crunch
    from crunch.api._identifiers import LeaderboardIdentifierType, ProjectIdentifierType, UserIdentifierType


class LeaderboardChangeValue(TypedDict):
    unit: dict[str, Any]
    current: float | None
    previous: float | None
    change: float | None
    changeDirection: Literal["RED", "GREEN"]


class LeaderboardChangeTarget(TypedDict):
    id: int
    name: str
    displayName: str
    rank: LeaderboardChangeValue
    rewardRank: LeaderboardChangeValue
    mean: LeaderboardChangeValue
    metrics: dict[str, LeaderboardChangeValue]


class LeaderboardChange(TypedDict):
    submitted: bool
    targets: list[LeaderboardChangeTarget]


@dataclass
class Leaderboard(ApiModel):
    id: int
    name: str
    display_name: str
    default: bool

    # only in the detailed response, raw: used to build the dataframe
    targets: list[dict[str, Any]] | None = None

    # only in the detailed response, extracted from the `crunch` object
    crunch_id: int | None = None

    competition = Parent["Competition"]()

    @classmethod
    def __pre_deserialize__(cls, d: dict[Any, Any]) -> dict[Any, Any]:
        crunch = d.get("crunch")
        if crunch is not None:
            d = {**d, "crunchId": crunch["id"]}

        return super().__pre_deserialize__(d)

    def reload(self) -> "Leaderboard":
        return self._update_from(self.competition.leaderboards.get(
            self.name,
            crunch=self.crunch_id
        ))

    def as_dataframe(self):
        if self.targets is None:
            self.reload()

        rows: list[dict[str, Any]] = []

        for target in self.targets or []:
            crunch: dict[str, Any] = target["crunch"]

            target_row: dict[str, Any] = {
                "target.id": target.get("id"),
                "target.name": target.get("name"),
                "crunch.id": crunch.get("id"),
                "crunch.number": crunch.get("number"),
            }

            metrics: list[dict[str, Any]] = target.get("metrics") or []
            metrics_by_id = {
                metric["id"]: metric
                for metric in metrics
            }

            positions: list[dict[str, Any]] = target.get("positions") or []
            for position in positions:
                user: dict[str, Any] = position["user"]
                project: dict[str, Any] = position["project"]
                team: dict[str, Any] = position.get("team") or {}

                row: dict[str, Any] = {
                    **target_row,
                    "user.id": user.get("id"),
                    "user.login": user.get("login"),
                    "project.id": project.get("id"),
                    "project.name": project.get("name"),
                    "team.id": team.get("id"),
                    "team.name": team.get("name"),
                    "mean": position.get("mean"),
                    "best": position.get("best"),
                    "rank": position.get("rank"),
                    "reward_rank": position.get("rewardRank"),
                    "successful_run_count": position.get("successfulRunCount"),
                    "unsuccessful_run_count": position.get("unsuccessfulRunCount"),
                    "duplicate": position.get("duplicate"),
                    "deterministic": position.get("deterministic"),
                    "out_of_range": position.get("outOfRange"),
                    "team_leader": position.get("teamLeader"),
                    "round_change": position.get("roundChange"),
                    "phase_change": position.get("phaseChange"),
                    "crunch_change": position.get("crunchChange"),
                    "committed_rewards": position.get("committedRewards"),
                    "projected_rewards": position.get("projectedRewards"),
                    "bounty_rewards": position.get("bountyRewards"),
                }

                position_metrics: list[dict[str, Any]] = position.get("metrics") or []
                for position_metric in position_metrics:
                    metric = metrics_by_id[position_metric["metricId"]]
                    key = f"metric.{metric['name']}"

                    row[f"{key}.score"] = position_metric.get("score")
                    row[f"{key}.best"] = position_metric.get("best")

                rows.append(row)

        import pandas
        return pandas.DataFrame(rows)

    def get_changes(
        self,
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType | None" = None,
        crunch: "Crunch | None" = None,
    ) -> LeaderboardChange:
        return self.competition.client.api.get_json(
            ["v3", "competitions", self.competition.name, "leaderboards", self.name, "changes", "users", user_identifier],
            params={
                "projectIdentifier": project_identifier,
                "crunchId": crunch.id if crunch is not None else None,
            },
        )


class LeaderboardCollection(ApiCollection[Leaderboard]):

    model = Leaderboard

    def __init__(self, competition: "Competition"):
        super().__init__(competition.client)

        self.competition = competition

    def _attach(self, leaderboard: Leaderboard) -> Leaderboard:
        leaderboard.competition = self.competition
        return leaderboard

    def get(
        self,
        identifier: "LeaderboardIdentifierType",
        *,
        crunch: "Crunch | int | None" = None,
    ) -> Leaderboard:
        crunch_id = crunch if isinstance(crunch, int) or crunch is None else crunch.id

        return self.from_dict(self.client.api.get_json(
            ["v2", "competitions", self.competition.name, "leaderboards", identifier],
            params={
                "crunchId": crunch_id,
            },
        ))

    def get_default(
        self,
        *,
        crunch: "Crunch | int | None" = None,
    ) -> Leaderboard:
        return self.get("@default", crunch=crunch)

    @property
    def default(self) -> Leaderboard:
        return self.get_default()

    def list(self) -> list[Leaderboard]:
        return self.from_dict_array(self.client.api.get_json([
            "v2", "competitions", self.competition.name, "leaderboards"
        ]))
