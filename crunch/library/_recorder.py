import builtins
import importlib
import os
import sys
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

_SITE_PACKAGES_DIRECTORY_NAMES = {"site-packages", "dist-packages"}


@dataclass(frozen=True)
class ImportLocation:
    file_path: str
    line: int | None

    def format(self, directory_path: str) -> str:
        """
        As `file:line`, relative to the directory, that IDEs turn into a link.
        """

        place = os.path.relpath(self.file_path, directory_path)

        if self.line is not None:
            place += f":{self.line}"

        return place


class ImportRecorder:
    """
    Records the top-level modules imported by the files of the project, other than the project's own modules, with the first place each one is imported.
    The imports made by the libraries are not recorded, so that their own dependencies are never reported.
    """

    def __init__(self, project_directory_path: str):
        self._project_directory_path = os.path.normcase(os.path.abspath(project_directory_path))
        self._is_project_file_by_path: dict[str, bool] = {}
        self._seen_module_names: set[str] = set()
        self._recording = False

        self.imports: dict[str, ImportLocation] = {}

    def __enter__(self):
        self._original_import = builtins.__import__
        self._original_import_module = importlib.import_module

        builtins.__import__ = self._import
        importlib.import_module = self._import_module
        self._recording = True

        return self

    def __exit__(self, *args: Any):
        self._recording = False

        # a wrapper installed after this one stays in place, this one then only forwards
        if builtins.__import__ == self._import:
            builtins.__import__ = self._original_import

        if importlib.import_module == self._import_module:
            importlib.import_module = self._original_import_module

    def _import(
        self,
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] = (),
        level: int = 0,
    ):
        module = self._original_import(name, globals, locals, fromlist, level)

        if level == 0 and globals is not None:
            self._record(name, globals)

        return module

    def _import_module(self, name: str, package: str | None = None):
        module = self._original_import_module(name, package)

        if not name.startswith("."):
            self._record(name, sys._getframe(1).f_globals)  # pyright: ignore[reportPrivateUsage]

        return module

    def _record(self, name: str, globals: Mapping[str, object]):
        if not self._recording:
            return

        module_name = name.partition(".")[0]
        if module_name in self._seen_module_names:
            return

        file_path = globals.get("__file__")
        if not isinstance(file_path, str) or not self._is_project_file(file_path):
            return

        self._seen_module_names.add(module_name)

        if self._is_project_module(module_name):
            return

        self.imports[module_name] = ImportLocation(
            file_path=file_path,
            line=_find_line(globals),
        )

    def _is_project_module(self, module_name: str) -> bool:
        """
        Whether the imported module is one of the project's own files or folders, like a `utils.py` next to the `main.py`.
        """

        module = sys.modules.get(module_name)

        file_path = getattr(module, "__file__", None)
        paths = [file_path] if file_path else getattr(module, "__path__", [])

        return any(self._is_project_file(path) for path in paths)

    def _is_project_file(self, file_path: str) -> bool:
        """
        Whether the file is written by the user: inside the project directory, but not inside a virtual environment created there.
        """

        is_project_file = self._is_project_file_by_path.get(file_path)

        if is_project_file is None:
            normalized_path = os.path.normcase(os.path.abspath(file_path))

            is_project_file = (
                normalized_path.startswith(self._project_directory_path + os.sep)
                and _SITE_PACKAGES_DIRECTORY_NAMES.isdisjoint(normalized_path.split(os.sep))
            )

            self._is_project_file_by_path[file_path] = is_project_file

        return is_project_file


def _find_line(globals: Mapping[str, object]) -> int | None:
    frame = sys._getframe(1)  # pyright: ignore[reportPrivateUsage]

    while frame is not None:
        if frame.f_globals is globals:
            return frame.f_lineno

        frame = frame.f_back

    return None
