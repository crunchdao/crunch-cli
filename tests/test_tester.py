import types
from typing import Any
from unittest import mock

import crunch.tester as tester


def _runner_prediction_directory(prediction_directory_path: str | None) -> Any:
    repository = types.SimpleNamespace(prediction_directory_path="/project/prediction")

    with (
        mock.patch.object(tester.monkey_patches, "pickle_unpickler_find_class"),
        mock.patch.object(tester.monkey_patches, "joblib_parallel_initializer"),
        mock.patch("crunch.runner.local.LocalRunner") as runner,
    ):
        tester.run(repository, None, None, "resources", True, 1, "@current", None, prediction_directory_path=prediction_directory_path)  # type: ignore

    return runner.call_args.args[4]


def test_the_project_prediction_directory_by_default():
    assert _runner_prediction_directory(None) == "/project/prediction"


def test_another_prediction_directory():
    assert _runner_prediction_directory("/project/scratch/prediction_b") == "/project/scratch/prediction_b"
