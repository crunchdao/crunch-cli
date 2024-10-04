import logging
import typing

from . import api

_logged_installed = False


def install_logger():
    global _logged_installed
    if _logged_installed:
        return

    import coloredlogs
    coloredlogs.install(
        level=logging.INFO,
        fmt='%(asctime)s %(message)s',
        datefmt='%H:%M:%S',
    )

    _logged_installed = True


def run(
    module: typing.Any,
    model_directory_path: str,
    force_first_train: bool,
    train_frequency: int,
    round_number: str,
    competition_format: api.CompetitionFormat,
    has_gpu=False,
    checks=True,
    no_determinism_check: typing.Optional[bool] = True,
    read_kwargs={},
    write_kwargs={},
):
    if competition_format == api.CompetitionFormat.STREAM:
        if no_determinism_check == False:
            logging.warning("determinism check not available for stream competitions")
            logging.warning("")

        no_determinism_check = True
    elif no_determinism_check is None:
        no_determinism_check = False

    from .runner.local import LocalRunner
    runner = LocalRunner(
        module,
        model_directory_path,
        force_first_train,
        train_frequency,
        round_number,
        competition_format,
        has_gpu,
        checks,
        not no_determinism_check,
        read_kwargs,
        write_kwargs,
    )

    return runner.start()
