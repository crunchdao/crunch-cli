import dataclasses
import typing

import dataclasses_json

from ... import api
from .. import code_loader, execute, utils


@dataclasses.dataclass
class RankableProjectMetric(dataclasses_json.DataClassJsonMixin):
    id: int
    score: float


@dataclasses.dataclass
class RankableProject(dataclasses_json.DataClassJsonMixin):
    id: typing.Optional[float]
    metrics: typing.List[RankableProjectMetric] = dataclasses.field(default_factory=list)

    def get_metric(self, id: int):
        for metric in self.metrics:
            if metric.id == id:
                return metric

        return None


class LeaderboardModule:
    """
    Duck typing class that represent a `leaderboard.py` usable for a custom ranker.
    """

    rank: typing.Callable

    @staticmethod
    def load(loader: code_loader.CodeLoader):
        try:
            module = loader.load()
        except code_loader.NoCodeFoundError:
            return None

        assert hasattr(module, "rank"), "`rank` function is missing"

        return typing.cast(LeaderboardModule, module)


def rank(
    module: LeaderboardModule,
    metrics: typing.List[api.Metric],
    projects: typing.List[RankableProject],
    logger=print,
) -> typing.List[int]:
    """
    Call the rank function of a leaderboard module.

    Return: An ordered list of project ids to use as the ranking.
    """

    target_and_metrics = utils.group_metrics_by_target(metrics)

    ordered_project_ids = execute.call_function(
        module.rank,
        {
            "target_and_metrics": target_and_metrics,
            "projects": projects,
        },
        logger,
    )

    if not isinstance(ordered_project_ids, list) or any(not isinstance(x, int) for x in ordered_project_ids):
        raise ValueError(f"rank(...) must return a list[int]: {ordered_project_ids}")

    unique_ordered_project_ids = set(ordered_project_ids)
    if len(unique_ordered_project_ids) != len(ordered_project_ids):
        raise ValueError(f"rank(...) contains duplicates project ids")

    original_project_ids = {project.id for project in projects}
    if unique_ordered_project_ids != original_project_ids:
        raise ValueError(f"rank(...) missing or extra project ids")

    return ordered_project_ids
