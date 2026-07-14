import logging
from typing import TYPE_CHECKING, Any, Optional

import crunch.monkey_patches as monkey_patches
from crunch.api import Competition, RoundIdentifierType
from crunch.unstructured import RunnerModule

if TYPE_CHECKING:
    from crunch.runner.tracing import LocalTraceExporter

_logged_installed = False

logger = logging.getLogger("crunch-cli:tester")
logger.parent = None


def install_logger():
    global _logged_installed

    if not _logged_installed:
        import coloredlogs
        coloredlogs.install(
            level=logging.INFO,
            logger=logger,
            fmt='%(asctime)s %(message)s',
            datefmt='%H:%M:%S',
        )

        _logged_installed = True

    return logger


def run(
    *,
    user_module: Any,
    runner_module: Optional[RunnerModule],
    model_directory_path: str,
    prediction_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    round_number: RoundIdentifierType,
    competition: Competition,
    has_gpu: bool = False,
    no_determinism_check: Optional[bool] = True,
    verbose: bool = False,
    trace_exporter: Optional["LocalTraceExporter"] = None,
):
    monkey_patches.pickle_unpickler_find_class()
    monkey_patches.joblib_parallel_initializer()

    if no_determinism_check is None:
        no_determinism_check = False

    from .runner.local import LocalRunner
    runner = LocalRunner(
        user_module=user_module,
        runner_module=runner_module,
        model_directory_path=model_directory_path,
        prediction_directory_path=prediction_directory_path,
        force_first_train=force_first_train,
        train_frequency=train_frequency,
        round_number=round_number,
        competition=competition,
        has_gpu=has_gpu,
        determinism_check_enabled=not no_determinism_check,
        verbose=verbose,
        logger=logger,
        trace_exporter=trace_exporter,
    )

    runner.start()
