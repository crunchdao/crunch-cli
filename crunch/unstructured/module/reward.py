from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Union

from dataclasses_json import DataClassJsonMixin

from crunch.api import Metric, Target
from crunch.scoring import ScoredMetricDetail as ScoredMetricDetail
from crunch.unstructured.code_loader import CodeLoader, ModuleWrapper, NoCodeFoundError
from crunch.unstructured.execute import call_function

__all__ = [
    "RewardModule",
    "RewardableProject",
    "RewardedProject",
]


@dataclass
class RewardableProjectMetric(DataClassJsonMixin):
    id: int
    score: float


def _rewardable_project_factory() -> List[RewardableProjectMetric]:
    return []


@dataclass
class RewardableProject(DataClassJsonMixin):
    id: Optional[float]
    group: str
    rewardable: bool
    metrics: List[RewardableProjectMetric] = field(default_factory=_rewardable_project_factory)

    def get_metric(self, id: int):
        for metric in self.metrics:
            if metric.id == id:
                return metric

        return None


@dataclass
class RewardedProject(DataClassJsonMixin):
    id: int
    amount: Optional[Union[float, int]]


class RewardModule(ModuleWrapper):

    def get_compute_bounties_function(
        self,
        *,
        ensure: bool = True
    ) -> Callable[..., List[RewardedProject]]:
        return self._get_function(
            name="compute_bounties",
            ensure=ensure,
        )

    def compute_bounties(
        self,
        *,
        target: Target,
        metrics: List[Metric],
        projects: List[RewardableProject],
        granted_amount: float,
        print: Optional[Callable[[Any], None]] = None,
    ):
        rewarded_projects = call_function(
            self.get_compute_bounties_function(ensure=True),
            kwargs={
                "target": target,
                "metrics": metrics,
                "projects": projects,
                "granted_amount": granted_amount,
            },
            print=print,
        )

        if (
            not isinstance(rewarded_projects, list)  # pyright: ignore[reportUnnecessaryIsInstance]
            or any(not isinstance(x, RewardedProject) for x in rewarded_projects)  # pyright: ignore[reportUnnecessaryIsInstance]
        ):
            raise ValueError(f"compute_bounties(...) must return a list[RewardedProject]: {rewarded_projects}")

        rewarded_project_ids = [x.id for x in rewarded_projects]
        unique_rewarded_project_ids = set(rewarded_project_ids)
        if len(unique_rewarded_project_ids) != len(rewarded_project_ids):
            raise ValueError(f"compute_bounties(...) contains duplicates project ids")

        original_project_ids = {project.id for project in projects}
        if unique_rewarded_project_ids != original_project_ids:
            raise ValueError(f"compute_bounties(...) missing or extra project ids")

        return rewarded_projects

    @staticmethod
    def load(loader: CodeLoader):
        try:
            module = loader.load()
            return RewardModule(module)
        except NoCodeFoundError:
            return None
