import pathlib
from typing import Any
from unittest import mock

import pytest
from crunch_convert import RequirementLanguage

from crunch.command.push import RequirementsMode, _upload_files, list_code_files  # pyright: ignore[reportPrivateUsage]
from crunch.command.push._cache import NoUploadCache


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


def _push_requirements(tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str], *, frozen: bool, **options: Any) -> str:
    requirements_path = tmp_path / "requirements.txt"
    requirements_path.write_text("pandas\n")

    def freeze(requirements: Any, **arguments: Any) -> Any:
        return ["pandas==3.1.1"] if frozen else requirements

    def format_files(requirements: Any, **arguments: Any) -> dict[RequirementLanguage, str]:
        return {language: "pandas==3.1.1\n" for language in RequirementLanguage}

    with (
        mock.patch("crunch_convert.requirements_txt.CachedWhitelist", return_value=mock.MagicMock()),
        mock.patch("crunch_convert.requirements_txt.freeze", side_effect=freeze),
        mock.patch("crunch_convert.requirements_txt.format_files_from_named", side_effect=format_files),
    ):
        _upload_files(
            group_name="code",
            storage={},
            found_files={"requirements.txt": str(requirements_path)},
            dry=True,
            client=mock.MagicMock(),
            preferred_chunk_size=1,
            requirements_mode=RequirementsMode.FREEZE,
            upload_cache=NoUploadCache(),
            **options,
        )

    return capsys.readouterr().out


def test_a_frozen_requirements_file_is_explained(tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]):
    output = _push_requirements(tmp_path, capsys, frozen=True)

    assert "froze file: requirements.txt" in output
    assert "requirements.txt: pinned to the installed versions in the submission" in output
    assert "requirements.original.txt, and stays unchanged locally" in output and "--no-pip-freeze" in output
    assert (tmp_path / "requirements.txt").read_text() == "pandas\n"


def test_a_notebook_push_names_its_own_option(tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]):
    output = _push_requirements(tmp_path, capsys, frozen=True, freeze_opt_out_hint="`include_installed_packages_version=False`")

    assert "use `include_installed_packages_version=False` to push your file as it is" in output
    assert "--no-pip-freeze" not in output


def test_an_already_pinned_requirements_file_is_not(tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]):
    output = _push_requirements(tmp_path, capsys, frozen=False)

    assert "using original file: requirements.txt" in output
    assert "pinned to the installed versions" not in output
