import typing

from . import code_loader, execute, file


class SubmissionModule:

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
    _call(
        module.check,
        submission_files,
        model_files,
        logger,
    )


def _call(
    function: typing.Callable,
    submission_files: typing.List[file.File],
    model_files: typing.List[file.File],
    print=print,
):
    return execute.call_function(
        function,
        {
            "submission_files": submission_files,
            "model_files": model_files,
        },
        print,
    )
