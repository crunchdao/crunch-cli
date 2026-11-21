import pathlib

import pytest

from crunch.command.push import list_code_files


def _workspace(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> str:
    for name in ("main.py", "requirements.txt", "resources/model.joblib", "resources_v2/model.joblib", "resources_sub6/bundle.pt", "catboost_info/learn_error.tsv", "sub/main.py", "sub/weights.pt", "sub/models/a.bin", "models/b.bin"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("content")

    # the ignore rules resolve the paths from the current directory, and `crunch push` runs from the project's root
    monkeypatch.chdir(tmp_path)
    return str(tmp_path)


def test_without_patterns_only_the_model_directory_is_left_out(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch):
    files = list_code_files(_workspace(tmp_path, monkeypatch), "resources_v2")

    assert "resources_v2/model.joblib" not in files
    assert {"resources/model.joblib", "resources_sub6/bundle.pt", "catboost_info/learn_error.tsv", "sub/weights.pt"} <= set(files)


def test_patterns_from_the_root(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch):
    root = _workspace(tmp_path, monkeypatch)

    files = list_code_files(root, "resources_v2", ["resources/", "resources_*/", "catboost_info/", "*.pt", "/models/"], root)

    # `*.pt` matches at any depth, `/models/` only at the root
    assert sorted(files) == ["main.py", "requirements.txt", "sub/main.py", "sub/models/a.bin"]


def test_patterns_are_relative_to_the_call_directory(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch):
    root = _workspace(tmp_path, monkeypatch)

    files = list_code_files(root, "resources_v2", ["models/", "*.pt", "*.joblib"], str(tmp_path / "sub"))

    # as a `.gitignore` in `sub/`: only what is below it, not `models/` nor the `.joblib` and `.pt` files outside it
    assert "sub/models/a.bin" not in files and "sub/weights.pt" not in files
    assert {"models/b.bin", "resources/model.joblib", "resources_sub6/bundle.pt", "sub/main.py"} <= set(files)
