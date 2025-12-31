from typing import Any, Callable, Dict, List, Optional

from crunch.api import Metric, PhaseType
from crunch.scoring import ScoredMetric
from crunch.scoring import ScoredMetricDetail as ScoredMetricDetail
from crunch.unstructured.code_loader import CodeLoader, ModuleWrapper, NoCodeFoundError
from crunch.unstructured.execute import call_function
from crunch.unstructured.utils import group_metrics_by_target

__all__ = [
    "ScoringModule",
    "ScoredMetric",
    "ScoredMetricDetail",
]


class ScoringModule(ModuleWrapper):

    def get_check_function(
        self,
        *,
        ensure: bool = True
    ) -> Callable[..., None]:
        return self._get_function(
            name="check",
            ensure=ensure,
        )

    def check(
        self,
        *,
        phase_type: PhaseType,
        chain_height: int,
        metrics: List[Metric],
        prediction_directory_path: str,
        data_directory_path: str,
        print: Optional[Callable[[Any], None]] = None,
    ):
        (
            target_and_metrics,
            target_names,
        ) = self._prepare_target_and_metrics(metrics)

        call_function(
            self.get_check_function(ensure=True),
            kwargs={
                "phase_type": phase_type,
                "chain_height": chain_height,
                "prediction_directory_path": prediction_directory_path,
                "data_directory_path": data_directory_path,
                "target_names": target_names,
                "target_and_metrics": target_and_metrics,
            },
            print=print,
        )

    def get_score_function(
        self,
        *,
        ensure: bool = True
    ) -> Callable[..., Dict[int, ScoredMetric]]:
        return self._get_function(
            name="score",
            ensure=ensure,
        )

    def score(
        self,
        *,
        phase_type: PhaseType,
        chain_height: int,
        metrics: List[Metric],
        prediction_directory_path: str,
        data_directory_path: str,
        print: Optional[Callable[[Any], None]] = None,
    ) -> Dict[int, ScoredMetric]:
        (
            target_and_metrics,
            target_names,
        ) = self._prepare_target_and_metrics(metrics)

        metric_ids = {
            metric.id
            for metric in metrics
        }

        results = call_function(
            self.get_score_function(ensure=True),
            kwargs={
                "phase_type": phase_type,
                "chain_height": chain_height,
                "prediction_directory_path": prediction_directory_path,
                "data_directory_path": data_directory_path,
                "target_names": target_names,
                "target_and_metrics": target_and_metrics,
            },
            print=print,
        )

        if not isinstance(results, dict):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise ValueError(f"return results must be a dict, got: {results.__class__}")

        for metric_id, scored_metric in results.items():
            if metric_id not in metric_ids:
                raise ValueError(f"metric id {metric_id} does not exists")

            if not isinstance(scored_metric, ScoredMetric):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise ValueError(f"results[{metric_id}] must be a ScoredMetric, got: {scored_metric.__class__}")

            value = scored_metric.value
            if not isinstance(value, float):
                raise ValueError(f"results[{metric_id}].value must be a float, got: {value.__class__}")

        return results

    def _prepare_target_and_metrics(
        self,
        metrics: List[Metric],
    ):
        target_and_metrics = group_metrics_by_target(metrics)

        target_names = list({
            target.name
            for target, _ in target_and_metrics
        })

        return (
            target_and_metrics,
            target_names,
        )

    @staticmethod
    def load(loader: CodeLoader):
        try:
            module = loader.load()
            return ScoringModule(module)
        except NoCodeFoundError:
            return None
