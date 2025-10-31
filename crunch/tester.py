import logging
import typing

from crunch.runner.types import KwargsLike

from . import api, unstructured, monkey_patches

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
    user_module: typing.Any,
    runner_module: unstructured.RunnerModule,
    model_directory_path: str,
    prediction_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    round_number: str,
    competition: api.Competition,
    has_gpu: bool = False,
    checks: bool = True,
    no_determinism_check: typing.Optional[bool] = True,
    read_kwargs: KwargsLike = {},
    write_kwargs: KwargsLike = {},
):
    monkey_patches.pickle_unpickler_find_class()
    monkey_patches.joblib_parallel_initializer()

    if no_determinism_check is None:
        no_determinism_check = False

    from .runner.local import LocalRunner
    runner = LocalRunner(
        user_module,
        runner_module,
        model_directory_path,
        prediction_directory_path,
        force_first_train,
        train_frequency,
        round_number,
        competition,
        has_gpu,
        checks,
        not no_determinism_check,
        read_kwargs,
        write_kwargs,
        logger,
    )

    return runner.start()
