import typing
import warnings

from ..identifiers import LeaderboardIdentifierType
from ..resource import Collection, Model
from .competition import Competition

if typing.TYPE_CHECKING:
    from .crunch import Crunch


class Leaderboard(Model):

    resource_identifier_attribute = "name"

    def __init__(
        self,
        competition: Competition,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._competition = competition

    @property
    def name(self):
        return self._attrs["name"]

    def as_dataframe(self):
        if "targets" not in self._attrs:
            self.reload()

        rows = []

        for target in self._attrs.get("targets") or []:
            crunch = target.get("crunch")

            target_row = {
                "target.id": target.get("id"),
                "target.name": target.get("name"),
                "crunch.id": crunch.get("id"),
                "crunch.number": crunch.get("number"),
            }

            metrics_by_id = {
                metric.get("id"): metric
                for metric in target.get("metrics") or []
            }

            for position in target.get("positions") or []:
                user = position.get("user")
                project = position.get("project")
                team = position.get("team") or {}

                row = {
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

                for position_metric in position.get("metrics"):
                    metric_id = position_metric.get("metricId")
                    metric = metrics_by_id[metric_id]
                    key = f"metric.{metric['name']}"

                    row[f"{key}.score"] = position_metric.get("score")
                    row[f"{key}.best"] = position_metric.get("best")

                rows.append(row)

        import pandas
        return pandas.DataFrame(rows)


class LeaderboardCollection(Collection):

    model = Leaderboard

    def __init__(
        self,
        competition: Competition,
        client=None
    ):
        super().__init__(client)

        self._competition = competition

    def __iter__(self) -> typing.Iterator[Leaderboard]:
        return super().__iter__()

    def get(
        self,
        identifier: LeaderboardIdentifierType,
        *,
        crunch: typing.Optional["Crunch"] = None
    ) -> Leaderboard:
        return self.prepare_model(
            self._client.api.get_leaderboard(
                self._competition.resource_identifier,
                identifier,
                crunch_id=crunch.id if crunch else None
            )
        )

    def get_default(
        self,
        *,
        crunch: typing.Optional["Crunch"] = None,
    ):
        return self.get(
            "@default",
            crunch=crunch
        )

    @property
    def default(self):
        return self.get_default()

    def get_mine(
        self,
        *,
        crunch: typing.Optional["Crunch"] = None,
    ):
        return self._get_mine(
            crunch=crunch,
        )

    @property
    def mine(self):
        return self._get_mine()

    def _get_mine(
        self,
        *,
        crunch: typing.Optional["Crunch"] = None,
    ):
        warnings.warn("@mine leaderboard are not available anymore, defaulting to @default", category=DeprecationWarning, stacklevel=3)

        return self.get(
            "@default",
            crunch=crunch
        )

    def list(
        self
    ) -> typing.List[Leaderboard]:
        return self.prepare_models(
            self._client.api.list_leaderboards(
                self._competition.resource_identifier,
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self._competition
        )


class LeaderboardEndpointMixin:

    def list_leaderboards(
        self,
        competition_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/leaderboards"
            ),
            json=True
        )

    def get_leaderboard(
        self,
        competition_identifier,
        leaderboard_identifier,
        crunch_id=None,
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
