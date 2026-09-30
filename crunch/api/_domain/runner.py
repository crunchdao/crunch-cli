from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional

from dataclasses_json import LetterCase, Undefined, config, dataclass_json

from crunch.api._resource import EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client


class RunnerRunSpanStatus(Enum):
    STARTED = "STARTED"
    ENDED = "ENDED"
    FAILED = "FAILED"


_datetime_config = config(
    encoder=lambda value: value.isoformat(),
    decoder=datetime.fromisoformat,
)

_status_config = config(
    encoder=lambda value: value.value,
    decoder=RunnerRunSpanStatus,
)


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class RunnerRunSpan:

    id: int
    parent_id: Optional[int]
    description: str
    started_at: datetime = field(metadata=_datetime_config)
    attributes: Optional[Dict[str, Any]]


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class StartedRunnerRunSpan(RunnerRunSpan):

    status: Literal[RunnerRunSpanStatus.STARTED] = field(
        metadata=_status_config,
        default=RunnerRunSpanStatus.STARTED
    )


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class EndedRunnerRunSpan(RunnerRunSpan):

    status: Literal[RunnerRunSpanStatus.ENDED, RunnerRunSpanStatus.FAILED] = field(
        metadata=_status_config,
    )

    ended_at: datetime = field(
        metadata=_datetime_config,
    )

    error: Optional[str]


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class RunnerRunMetric:

    timestamp: datetime = field(
        metadata=_datetime_config,
    )

    cpu: float   # % 0–100
    ram: int   # bytes used
    disk: int  # bytes used
    gpu: Optional[float]  # % 0–100
    vram: Optional[int]  # bytes used


class RunnerRun(Model[int]):

    def __init__(
        self,
        run_id: int,
        client: Optional["Client"] = None
    ):
        super().__init__(attrs={}, client=client, collection=None)

        self._run_id = run_id

    @property
    def code(self) -> Dict[str, str]:
        return self._checked_client.api.get_runner_run_code(
            self._run_id
        )

    @property
    def model(self) -> Dict[str, str]:
        return self._checked_client.api.get_runner_run_model(
            self._run_id
        )

    @property
    def data(self):
        from crunch.api._domain.data_release import DataReleaseCollection

        data_release_attrs = self._checked_client.api.get_runner_run_data(
            self._run_id
        )

        return DataReleaseCollection(None).prepare_model(data_release_attrs)

    def report_error(
        self,
        trace: str
    ):
        self._checked_client.api.report_runner_run_error(
            self._run_id,
            trace
        )

    def report_traces(
        self,
        spans: List[RunnerRunSpan],
        metrics: List[RunnerRunMetric],
    ):
        self._checked_client.api.report_runner_run_traces(
            self._run_id,
            spans=[
                span.to_dict()  # type: ignore[attr-defined]
                for span in spans
            ],
            metrics=[
                metric.to_dict()  # type: ignore[attr-defined]
                for metric in metrics
            ],
        )

    def submit_result(
        self,
        use_initial_model: bool,
        deterministic: Optional[bool],
        prediction_files: Dict[str, str],
        model_files: Dict[str, str],
    ):
        self._checked_client.api.submit_runner_run_result(
            self._run_id,
            use_initial_model,
            deterministic,
            prediction_files,
            model_files,
        )


class RunnerRunEndpointMixin(EndpointMixin):

    def get_runner_run_code(
        self,
        run_id: int,
    ):
        return self._result(
            self.get(
                f"/v1/runner/runs/{run_id}/code"
            ),
            json=True
        )

    def get_runner_run_data(
        self,
        run_id: int,
    ):
        return self._result(
            self.get(
                f"/v1/runner/runs/{run_id}/data"
            ),
            json=True
        )

    def get_runner_run_model(
        self,
        run_id: int,
    ):
        return self._result(
            self.get(
                f"/v1/runner/runs/{run_id}/model"
            ),
            json=True
        )

    def report_runner_run_traces(
        self,
        run_id: int,
        spans: List[Dict[str, Any]],
        metrics: List[Dict[str, Any]],
    ):
        return self._result(
            self.post(
                f"/v1/runner/runs/{run_id}/traces",
                json={
                    "spans": spans,
                    "metrics": metrics,
                }
            )
        )

    def report_runner_run_error(
        self,
        run_id: int,
        trace: str,
    ):
        return self._result(
            self.put(
                f"/v1/runner/runs/{run_id}/error",
                json={
                    "trace": trace,
                }
            )
        )

    def submit_runner_run_result(
        self,
        run_id: int,
        use_initial_model: bool,
        deterministic: Optional[bool],
        prediction_files: Dict[str, str],
        model_files: Dict[str, str],
    ):
        return self._result(
            self.post(
                f"/v1/runner/runs/{run_id}/result",
                json={
                    "useInitialModel": use_initial_model,
                    "deterministic": deterministic,
                    "predictionFiles": prediction_files,
                    "modelFiles": model_files,
                },
            )
        )
