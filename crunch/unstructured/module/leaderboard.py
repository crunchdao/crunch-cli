from dataclasses import dataclass, field
from enum import Enum
from itertools import combinations
from typing import Any, Callable, Dict, List, Optional, Union

from dataclasses_json import DataClassJsonMixin

from crunch.api import Metric, Target
from crunch.unstructured.code_loader import CodeLoader, ModuleWrapper, NoCodeFoundError
from crunch.unstructured.execute import call_function
from crunch.unstructured.utils import group_metrics_by_target

__all__ = [
    "RankableProject",
    "RankableProjectMetric",
    "RankPass",
    "RankedProject",
    "ComparedSimilarity",
    "LeaderboardModule",
]


@dataclass
class RankableProjectMetric(DataClassJsonMixin):
    id: int
    score: float


def _rankable_project_factory() -> List[RankableProjectMetric]:
    return []


@dataclass
class RankableProject(DataClassJsonMixin):
    id: Optional[float]
    group: str
    rewardable: bool
    metrics: List[RankableProjectMetric] = field(default_factory=_rankable_project_factory)

    def get_metric(self, id: int):
        for metric in self.metrics:
            if metric.id == id:
                return metric

        return None


class RankPass(Enum):
    PRE_DUPLICATE = "PRE_DUPLICATE"
    FINAL = "FINAL"


@dataclass
class RankedProject(DataClassJsonMixin):
    id: int
    rank: int
    reward_rank: Optional[Union[float, int]]


@dataclass
class ComparedSimilarity(DataClassJsonMixin):
    left_id: int
    right_id: int
    target_id: int
    value: float


class LeaderboardModule(ModuleWrapper):

    def get_compare_function(
        self,
        *,
        ensure: bool = True
    ) -> Callable[..., List[ComparedSimilarity]]:
        return self._get_function(
            name="compare",
            ensure=ensure,
        )

    def compare(
        self,
        *,
        targets: List[Target],
        prediction_directory_path_by_id: Dict[int, str],
        data_directory_path: str,
        print: Optional[Callable[[Any], None]] = None,
    ) -> List[ComparedSimilarity]:
        """
        Call the compare function of a leaderboard module.

        Return: An ordered list of project ids to use as the ranking.
        """

        combos = list(combinations(sorted(prediction_directory_path_by_id.keys()), 2))

        similarities = call_function(
            self.get_compare_function(ensure=True),
            {
                "targets": targets,
                "prediction_directory_path_by_id": prediction_directory_path_by_id,
                "combinations": combos,
                "data_directory_path": data_directory_path,
            },
            print=print,
        )

        return similarities

    def get_rank_function(
        self,
        *,
        ensure: bool = True
    ) -> Callable[..., List[RankedProject]]:
        return self._get_function(
            name="rank",
            ensure=ensure,
        )

    def rank(
        self,
        *,
        target: Target,
        metrics: List[Metric],
        projects: List[RankableProject],
        rank_pass: RankPass,
        print: Optional[Callable[[Any], None]] = None,
    ) -> List[RankedProject]:
        """
        Call the rank function of a leaderboard module.

        Return: An ordered list of project ids to use as the ranking.
        """

        ranked_projects = call_function(
            self.get_rank_function(ensure=True),
            {
                "target": target,
                "metrics": metrics,
                "target_and_metrics": [(target, metrics)],  # deprecated
                "projects": projects,
                "rank_pass": rank_pass,
            },
            print=print,
        )

        if (
            isinstance(ranked_projects, list)  # pyright: ignore[reportUnnecessaryIsInstance]
            and all(isinstance(x, int) for x in ranked_projects)
        ):
            ranked_projects = [
                RankedProject(
                    ranked_project.id,
                    rank,
                    float(rank)
                )
                for rank, ranked_project in enumerate(ranked_projects, 1)
            ]

        if (
            not isinstance(ranked_projects, list)  # pyright: ignore[reportUnnecessaryIsInstance]
            or any(not isinstance(x, RankedProject) for x in ranked_projects)  # pyright: ignore[reportUnnecessaryIsInstance]
        ):
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
    def load(loader: CodeLoader):
        try:
            module = loader.load()
            return LeaderboardModule(module)
        except NoCodeFoundError:
            return None
