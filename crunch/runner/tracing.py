import sys
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from queue import Empty as QueueEmpty
from queue import Queue
from threading import Event, Thread
from typing import Any, Dict, List, Optional, Union

from retry import retry

from crunch.api import RunnerRun
from crunch.api._domain.runner import EndedRunnerRunSpan, RunnerRunMetric, RunnerRunSpan, RunnerRunSpanStatus, StartedRunnerRunSpan
from crunch.runner.types import KwargsLike


class TraceExporter(ABC):

    @abstractmethod
    def push(self, spans: List[RunnerRunSpan], metrics: List[RunnerRunMetric]):
        ...


class VoidTraceExporter(TraceExporter):

    def push(self, spans: List[RunnerRunSpan], metrics: List[RunnerRunMetric]):
        pass


class LocalTraceExporter(TraceExporter):

    def __init__(self):
        self.span_by_id: Dict[int, LocalSpan] = {}
        self.metrics: List[RunnerRunMetric] = []

    def reset(self):
        self.span_by_id.clear()
        self.metrics.clear()

    def push(self, spans: List[RunnerRunSpan], metrics: List[RunnerRunMetric]):
        for span in spans:
            if isinstance(span, StartedRunnerRunSpan):
                self.span_by_id[span.id] = LocalSpan(
                    id=span.id,
                    parent_id=span.parent_id,
                    description=span.description,
                    status=span.status,
                    attributes=span.attributes,
                    started_at=span.started_at,
                    ended_at=None,
                    error=None,
                )
            elif isinstance(span, EndedRunnerRunSpan):
                local_span = self.span_by_id[span.id]
                assert local_span is not None, f"ended span {span.id} has no matching started span?"

                local_span.ended_at = span.ended_at
                local_span.status = span.status
                local_span.error = span.error

        self.metrics.extend(metrics)


@dataclass
class LocalSpan:

    id: int
    parent_id: Optional[int]
    description: str
    status: RunnerRunSpanStatus
    attributes: Optional[Dict[str, Any]]
    started_at: datetime
    ended_at: Optional[datetime]
    error: Optional[str]


class RemoteTraceExporter(TraceExporter):

    def __init__(self, run: RunnerRun):
        self.run = run

    @retry(tries=3, delay=10, backoff=5, jitter=(1, 5), logger=None)
    def push(self, spans: List[RunnerRunSpan], metrics: List[RunnerRunMetric]):
        self.run.report_traces(
            spans=spans,
            metrics=metrics
        )


class GpuPresence(Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    AUTO_DETECT = "AUTO_DETECT"


Attributes = Dict[str, Any]


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


class RunnerTracer:

    def __init__(
        self,
        exporter: TraceExporter,
        *,
        gpu_presence: GpuPresence = GpuPresence.AUTO_DETECT,
        batch_delay: int = 5,
        metrics_delay: int = 10,
    ):
        self.exporter = exporter
        self.gpu_presence = gpu_presence
        self.batch_delay = batch_delay
        self.metrics_delay = metrics_delay

        self._span_counter = 0
        self._current_parent_id: Optional[int] = None

        self._queue: Queue[Union[RunnerRunSpan, RunnerRunMetric]] = Queue()
        self._stop_event = Event()

        self._worker_thread = Thread(target=self._worker_loop, daemon=True)
        self._metrics_thread = Thread(target=self._metrics_loop, daemon=True)

    def next_span_id(self) -> int:
        self._span_counter += 1
        return self._span_counter

    def emit(self, item: Union[RunnerRunSpan, RunnerRunMetric]):
        if not self._stop_event.is_set():
            self._queue.put(item)

    def _get_batch(self) -> Optional[List[Union[RunnerRunSpan, RunnerRunMetric]]]:
        try:
            item = self._queue.get(timeout=0.5)
        except QueueEmpty:
            return None

        batch: List[Union[RunnerRunSpan, RunnerRunMetric]] = [item]

        while True:
            try:
                batch.append(self._queue.get_nowait())
            except QueueEmpty:
                break

        return batch

    def _worker_loop(self):
        while not self._stop_event.wait(self.batch_delay) or not self._queue.empty():
            batch = self._get_batch()
            if batch is None:
                continue

            spans = [item for item in batch if isinstance(item, RunnerRunSpan)]
            metrics = [item for item in batch if isinstance(item, RunnerRunMetric)]

            try:
                self.exporter.push(spans=spans, metrics=metrics)
            except Exception as exception:
                print(f"dropped {len(spans)} spans and {len(metrics)} metrics: {exception.__class__.__name__}({exception})", file=sys.stderr)
            finally:
                for _ in range(len(batch)):
                    self._queue.task_done()

    def _metrics_loop(self):
        import psutil
        import pynvml  # pyright: ignore[reportMissingTypeStubs]

        gpu_handle = None
        if self.gpu_presence != GpuPresence.UNAVAILABLE:
            try:
                pynvml.nvmlInit()
                gpu_handle = pynvml.nvmlDeviceGetHandleByIndex(0)  # pyright: ignore[reportUnknownMemberType]
            except pynvml.NVMLError as error:
                if self.gpu_presence == GpuPresence.AVAILABLE:
                    print(f"[pynvml] failed to initialize: {error}", file=sys.stderr)

                gpu_handle = None

        while not self._stop_event.wait(self.metrics_delay):
            self.emit(RunnerRunMetric(
                timestamp=_now_utc(),
                cpu=psutil.cpu_percent(interval=None),
                ram=psutil.virtual_memory().used,
                disk=psutil.disk_usage('/').used,
                gpu=pynvml.nvmlDeviceGetUtilizationRates(gpu_handle).gpu if gpu_handle else None,  # pyright: ignore[reportUnknownMemberType, reportArgumentType]
                vram=pynvml.nvmlDeviceGetMemoryInfo(gpu_handle).used if gpu_handle else None  # pyright: ignore[reportUnknownMemberType, reportArgumentType]
            ))

    def start(self):
        if self._worker_thread.is_alive():
            raise RuntimeError("tracer is already running")

        self._worker_thread.start()
        self._metrics_thread.start()

    def stop(self):
        self._stop_event.set()
        self._metrics_thread.join(timeout=2)

        self._queue.join()
        self._worker_thread.join(timeout=10)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # pyright: ignore[reportMissingParameterType, reportUnknownParameterType]
        self.stop()

    @contextmanager
    def span(
        self,
        description: str,
        attributes: Optional[Attributes] = None,
    ):
        previous_parent_id = self._current_parent_id

        span_id = self.next_span_id()
        self._current_parent_id = span_id
        started_at = _now_utc()

        self.emit(StartedRunnerRunSpan(
            id=span_id,
            parent_id=previous_parent_id,
            description=description,
            started_at=started_at,
            attributes=attributes,
        ))

        end_status = RunnerRunSpanStatus.ENDED
        end_error = None

        try:
            yield
        except BaseException as exception:
            end_status = RunnerRunSpanStatus.FAILED
            end_error = f"{exception.__class__.__name__}({str(exception)})"
            raise
        finally:
            self._current_parent_id = previous_parent_id

            self.emit(EndedRunnerRunSpan(
                id=span_id,
                parent_id=previous_parent_id,
                description=description,
                status=end_status,
                started_at=started_at,
                ended_at=_now_utc(),
                error=end_error,
                attributes=attributes,
            ))


def to_execute_span_attributes(
    command: str,
    parameters: Optional[KwargsLike],
    span_hidden_parameters: Optional[List[str]] = None,
    span_attributes: Optional[KwargsLike] = None,
) -> Dict[str, Any]:
    attributes: Dict[str, Any] = {
        "command": command,
    }

    if parameters is not None:
        hidden_parameters = set(span_hidden_parameters or [])

        parameters = {
            key: value
            for key, value in parameters.items()
            if not key.endswith("_path") and key not in hidden_parameters
        }

        if len(parameters) > 0:
            attributes["parameters"] = parameters

    if span_attributes is not None:
        attributes.update(span_attributes)

    return attributes
