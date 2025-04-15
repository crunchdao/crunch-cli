import typing

from .. import code_loader, execute, file


class SubmissionModule:
    """
    Duck typing class that represent a `submission.py` usable for a custom checker.
    """

    check: typing.Callable

    @staticmethod
    def load(loader: code_loader.CodeLoader):
        try:
            module = loader.load()
        except code_loader.NoCodeFoundError:
            return None

        assert hasattr(module, "check"), "`check` function is missing"

        return typing.cast(SubmissionModule, module)


def check(
    module: SubmissionModule,
    submission_files: typing.List[file.File],
    model_files: typing.List[file.File],
    logger=print,
):
    return execute.call_function(
        module.check,
        {
            "submission_files": submission_files,
            "model_files": model_files,
        },
        logger,
    )
