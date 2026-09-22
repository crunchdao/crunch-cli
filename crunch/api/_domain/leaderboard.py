from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.competition import Competition
    from crunch.api._domain.crunch import Crunch
    from crunch.api._identifiers import CompetitionIdentifierType, LeaderboardIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class Leaderboard(Model[int]):

    def __init__(
        self,
        competition: "Competition",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["LeaderboardCollection"] = None
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._competition = competition

    @property
    def resource_identifier(self) -> str:
        return self.name

    @property
    def name(self):
        return self._attrs["name"]

    def as_dataframe(self):
        if "targets" not in self._attrs:
            self.reload()

        rows: List[Dict[str, Any]] = []

        for target in self._attrs.get("targets") or []:  # pyright: ignore[reportUnknownVariableType]
            crunch = target.get("crunch")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            target_row: Dict[str, Any] = {
                "target.id": target.get("id"),  # pyright: ignore[reportUnknownMemberType]
                "target.name": target.get("name"),  # pyright: ignore[reportUnknownMemberType]
                "crunch.id": crunch.get("id"),  # pyright: ignore[reportUnknownMemberType]
                "crunch.number": crunch.get("number"),  # pyright: ignore[reportUnknownMemberType]
            }

            metrics_by_id = {  # pyright: ignore[reportUnknownVariableType]
                metric.get("id"): metric  # pyright: ignore[reportUnknownMemberType]
                for metric in target.get("metrics") or []  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            }

            for position in target.get("positions") or []:  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                user = position.get("user")  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
                project = position.get("project")  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
                team = position.get("team") or {}  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]

                row: Dict[str, Any] = {
                    **target_row,
                    "user.id": user.get("id"),  # pyright: ignore[reportUnknownMemberType]
                    "user.login": user.get("login"),  # pyright: ignore[reportUnknownMemberType]
                    "project.id": project.get("id"),  # pyright: ignore[reportUnknownMemberType]
                    "project.name": project.get("name"),  # pyright: ignore[reportUnknownMemberType]
                    "team.id": team.get("id"),  # pyright: ignore[reportUnknownMemberType]
                    "team.name": team.get("name"),  # pyright: ignore[reportUnknownMemberType]
                    "mean": position.get("mean"),  # pyright: ignore[reportUnknownMemberType]
                    "best": position.get("best"),  # pyright: ignore[reportUnknownMemberType]
                    "rank": position.get("rank"),  # pyright: ignore[reportUnknownMemberType]
                    "reward_rank": position.get("rewardRank"),  # pyright: ignore[reportUnknownMemberType]
                    "successful_run_count": position.get("successfulRunCount"),  # pyright: ignore[reportUnknownMemberType]
                    "unsuccessful_run_count": position.get("unsuccessfulRunCount"),  # pyright: ignore[reportUnknownMemberType]
                    "duplicate": position.get("duplicate"),  # pyright: ignore[reportUnknownMemberType]
                    "deterministic": position.get("deterministic"),  # pyright: ignore[reportUnknownMemberType]
                    "out_of_range": position.get("outOfRange"),  # pyright: ignore[reportUnknownMemberType]
                    "team_leader": position.get("teamLeader"),  # pyright: ignore[reportUnknownMemberType]
                    "round_change": position.get("roundChange"),  # pyright: ignore[reportUnknownMemberType]
                    "phase_change": position.get("phaseChange"),  # pyright: ignore[reportUnknownMemberType]
                    "crunch_change": position.get("crunchChange"),  # pyright: ignore[reportUnknownMemberType]
                    "committed_rewards": position.get("committedRewards"),  # pyright: ignore[reportUnknownMemberType]
                    "projected_rewards": position.get("projectedRewards"),  # pyright: ignore[reportUnknownMemberType]
                    "bounty_rewards": position.get("bountyRewards"),  # pyright: ignore[reportUnknownMemberType]
                }

                for position_metric in position.get("metrics"):  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
                    metric_id = position_metric.get("metricId")  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
                    metric = metrics_by_id[metric_id]  # pyright: ignore[reportUnknownVariableType]
                    key = f"metric.{metric['name']}"  # pyright: ignore[reportUnknownMemberType]

                    row[f"{key}.score"] = position_metric.get("score")  # pyright: ignore[reportUnknownMemberType]
                    row[f"{key}.best"] = position_metric.get("best")  # pyright: ignore[reportUnknownMemberType]

                rows.append(row)

        import pandas
        return pandas.DataFrame(rows)


class LeaderboardCollection(Collection[Leaderboard]):

    model = Leaderboard

    def __init__(
        self,
        competition: "Competition",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self._competition = competition

    def get(
        self,
        identifier: "LeaderboardIdentifierType",
        *,
        crunch: Optional["Crunch"] = None
    ) -> Leaderboard:
        return self.prepare_model(
            self._checked_client.api.get_leaderboard(
                self._competition.resource_identifier,
                identifier,
                crunch_id=crunch.id if crunch else None
            )
        )

    def get_default(
        self,
        *,
        crunch: Optional["Crunch"] = None,
    ):
        return self.get(
            "@default",
            crunch=crunch
        )

    @property
    def default(self):
        return self.get_default()

    def list(
        self
    ) -> List[Leaderboard]:
        return self.prepare_models(
            self._checked_client.api.list_leaderboards(
                self._competition.resource_identifier,
            )
        )

    def prepare_model(self, attrs: Union["JsonValue", Leaderboard], *args: Any) -> Leaderboard:
        return super().prepare_model(
            attrs,
            self._competition,
            *args
        )


class LeaderboardEndpointMixin(EndpointMixin):

    def list_leaderboards(
        self,
        competition_identifier: "CompetitionIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/leaderboards"
            ),
            json=True
        )

    def get_leaderboard(
        self,
        competition_identifier: "CompetitionIdentifierType",
        leaderboard_identifier: "LeaderboardIdentifierType",
        crunch_id: Optional[int] = None,
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/leaderboards/{leaderboard_identifier}",
                params={
                    "crunchId": crunch_id
                }
            ),
            json=True
        )
