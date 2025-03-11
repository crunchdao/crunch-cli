import dataclasses
import itertools
import typing

import dataclasses_json
import pandas

from ... import api
from .. import code_loader, execute, utils


@dataclasses.dataclass
class RankableProjectMetric(dataclasses_json.DataClassJsonMixin):
    id: int
    score: float


@dataclasses.dataclass
class RankableProject(dataclasses_json.DataClassJsonMixin):
    id: typing.Optional[float]
    rewardable: bool
    metrics: typing.List[RankableProjectMetric] = dataclasses.field(default_factory=list)

    def get_metric(self, id: int):
        for metric in self.metrics:
            if metric.id == id:
                return metric

        return None


@dataclasses.dataclass
class RankedProject(dataclasses_json.DataClassJsonMixin):
    id: int
    rank: int
    reward_rank: typing.Optional[typing.Union[int]]


@dataclasses.dataclass
class ComparedSimilarity(dataclasses_json.DataClassJsonMixin):
    left_id: int
    right_id: int
    target_id: int
    value: float


class LeaderboardModule(code_loader.ModuleWrapper):

    def compare_function(self, ensure=True):
        return self._get_function("compare", ensure)

    def compare(
        self,
        targets: typing.List[api.Target],
        predictions: typing.Dict[int, pandas.DataFrame],
        print=print,
    ) -> typing.List[ComparedSimilarity]:
        """
        Call the compare function of a leaderboard module.

        Return: An ordered list of project ids to use as the ranking.
        """

        combinations = list(itertools.combinations(sorted(predictions.keys()), 2))

        similarities = execute.call_function(
            self.compare_function(ensure=True),
            {
                "targets": targets,
                "predictions": predictions,
                "combinations": combinations,
            },
            print,
        )

        return similarities

    def rank_function(self, ensure=True):
        return self._get_function("rank", ensure)

    def rank(
        self,
        metrics: typing.List[api.Metric],
        projects: typing.List[RankableProject],
        print=print,
    ) -> typing.List[RankedProject]:
        """
        Call the rank function of a leaderboard module.

        Return: An ordered list of project ids to use as the ranking.
        """

        target_and_metrics = utils.group_metrics_by_target(metrics)

        ranked_projects = execute.call_function(
            self.rank_function(ensure=True),
            {
                "target_and_metrics": target_and_metrics,
                "projects": projects,
            },
            print,
        )

        if isinstance(ranked_projects, list) and all(isinstance(x, int) for x in ranked_projects):
            ranked_projects = [
                RankedProject(id, rank, float(rank))
                for rank, id in enumerate(ranked_projects, 1)
            ]

        if not isinstance(ranked_projects, list) or any(not isinstance(x, RankedProject) for x in ranked_projects):
            raise ValueError(f"rank(...) must return a list[RankedProject]: {ranked_projects}")

        ranked_project_ids = [x.id for x in ranked_projects]
        unique_ranked_project_ids = set(ranked_project_ids)
        if len(unique_ranked_project_ids) != len(ranked_project_ids):
            raise ValueError(f"rank(...) contains duplicates project ids")

        original_project_ids = {project.id for project in projects}
        if unique_ranked_project_ids != original_project_ids:
            raise ValueError(f"rank(...) missing or extra project ids")

        ranks = [x.rank for x in ranked_projects]
        if len(ranks) != len(set(ranks)):
            raise ValueError(f"rank(...) contains duplicate rank")

        return ranked_projects

    @staticmethod
    def load(loader: code_loader.CodeLoader):
        try:
            module = loader.load()
            return LeaderboardModule(module)
        except code_loader.NoCodeFoundError:
            return None
