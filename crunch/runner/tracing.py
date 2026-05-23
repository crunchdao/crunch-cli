from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from queue import Empty as QueueEmpty
from queue import Queue
from threading import Event, Thread
from typing import Any, Dict, List, Optional, Union

from crunch.api import RunnerRun
from crunch.api._domain.runner import EndedRunnerRunSpan, RunnerRunMetric, RunnerRunSpan, RunnerRunSpanStatus, StartedRunnerRunSpan
from crunch.runner.types import KwargsLike


class TraceExporter(ABC):

    @abstractmethod
    def push(self, spans: List[RunnerRunSpan], metrics: List[RunnerRunMetric]) -> bool:
        ...


class VoidTraceExporter(TraceExporter):

    def push(
        self,
        spans: List[RunnerRunSpan],
        metrics: List[RunnerRunMetric],
    ) -> bool:
        return True


class LocalTraceExporter(TraceExporter):

    def __init__(self):
        self.span_by_id: Dict[int, LocalSpan] = {}
        self.metrics: List[RunnerRunMetric] = []

    def reset(self):
        self.span_by_id.clear()
        self.metrics.clear()

    def push(
        self,
        spans: List[RunnerRunSpan],
        metrics: List[RunnerRunMetric],
    ) -> bool:
        for span in spans:
            if isinstance(span, StartedRunnerRunSpan):
                self.span_by_id[span.id] = LocalSpan(
                    id=span.id,
                    parent_id=span.parent_id,
                    name=span.name,
                    status=span.status,
                    attributes=span.attributes,
                    started_at=span.started_at,
                    ended_at=None,
                    error=None
                )
            elif isinstance(span, EndedRunnerRunSpan):
                existing = self.span_by_id.get(span.id)
                if existing is not None:
                    existing.ended_at = span.ended_at
                    existing.status = span.status
                    existing.error = span.error

        self.metrics.extend(metrics)

        return True


@dataclass
class LocalSpan:

    id: int
    parent_id: Optional[int]
    name: str
    status: RunnerRunSpanStatus
    attributes: Optional[Dict[str, Any]]
    started_at: datetime
    ended_at: Optional[datetime]
    error: Optional[str]


class RemoteTraceExporter(TraceExporter):

    def __init__(self, run: RunnerRun):
        self.run = run

    def push(
        self,
        spans: List[RunnerRunSpan],
        metrics: List[RunnerRunMetric],
    ) -> bool:
        try:
            self.run.report_traces(
                spans=spans,
                metrics=metrics
            )
        except Exception as e:
            print(f"Failed to send batch of {len(spans) + len(metrics)} items: {e}")
            return False

        return True


Attributes = Dict[str, Any]


class RunnerTracer:

    def __init__(
        self,
        exporter: TraceExporter,
        metrics_delay: int = 10,
    ):
        self.exporter = exporter
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
        while not self._stop_event.is_set() or not self._queue.empty():
            batch = self._get_batch()
            if batch is None:
                continue

            spans = [item for item in batch if isinstance(item, RunnerRunSpan)]
            metrics = [item for item in batch if isinstance(item, RunnerRunMetric)]

            try:
                self.exporter.push(spans=spans, metrics=metrics)
                for _ in range(len(batch)):
                    self._queue.task_done()
            except Exception as e:
                print(f"Failed to send batch of {len(batch)} items: {e}")
                for item in batch:
                    self._queue.put(item)

    def _metrics_loop(self):
        import psutil

        while not self._stop_event.wait(self.metrics_delay):
            self.emit(RunnerRunMetric(
                timestamp=datetime.now(),
                cpu_percentage=psutil.cpu_times_percent().user,
                memory_percentage=psutil.virtual_memory().percent
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

    def __exit__(self, exc_type, exc_val, exc_tb): # pyright: ignore[reportMissingParameterType, reportUnknownParameterType]
        self.stop()

    @contextmanager
    def span(
        self,
        name: str,
        attributes: Optional[Attributes] = None,
    ):
        previous_parent_id = self._current_parent_id

        span_id = self.next_span_id()
        self._current_parent_id = span_id

        self.emit(StartedRunnerRunSpan(
            id=span_id,
            parent_id=previous_parent_id,
            name=name,
            started_at=datetime.now(),
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
                status=end_status,
                ended_at=datetime.now(),
                error=end_error
            ))


def to_execute_span_attributes(
    command: str,
    parameters: Optional[KwargsLike],
) -> Dict[str, Any]:
    attributes: Dict[str, Any] = {
        "command": command,
    }

    if parameters is not None:
        attributes["parameters"] = {
            key: value
            for key, value in parameters.items()
            if not key.endswith("_path")
        }

    return attributes
