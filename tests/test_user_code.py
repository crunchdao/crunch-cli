import concurrent.futures
import multiprocessing
import os
import pickle
import subprocess
import sys
import tempfile
import unittest

from crunch import constants, monkey_patches
from crunch.command.test import load_user_code

_PTH_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "crunch-monkeypatch.pth")

_MAIN = """import os
import pickle
import sys


class Model:

    def __init__(self, weight):
        self.weight = weight

    def predict(self, x):
        return x * self.weight


def describe(x):
    return x * x, os.path.basename(__file__)


def load_model(path):
    with open(path, "rb") as fd:
        return pickle.load(fd)


if __name__ == "__main__":
    with open(sys.argv[1], "wb") as fd:
        pickle.dump(Model(3), fd)
"""


def square(x: int):
    return x * x


class LoadUserCodeTest(unittest.TestCase):

    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self._main_file_path = os.path.join(self._directory.name, "main.py")

        with open(self._main_file_path, "w", encoding="utf-8") as fd:
            fd.write(_MAIN)

        self._model_file_path = os.path.join(self._directory.name, "model.pkl")
        subprocess.run([sys.executable, self._main_file_path, self._model_file_path], check=True)

        self._module_name = f"user_code_{os.getpid()}_{id(self)}"

        self._original_sys_path = list(sys.path)
        self._original_environment = {
            name: os.environ.get(name)
            for name in ("PYTHONPATH", constants.USER_CODE_MODULE_NAME_ENV_VAR, constants.USER_CODE_MAIN_FILE_PATH_ENV_VAR)
        }

    def tearDown(self):
        for name, value in self._original_environment.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

        sys.modules.pop(self._module_name, None)
        sys.path[:] = self._original_sys_path
        self._directory.cleanup()

    def _install_pth_in_child_processes(self):
        """The `.pth` file is not installed by an editable install: a `sitecustomize` runs its line instead."""

        site_directory_path = os.path.join(self._directory.name, "site")
        os.makedirs(site_directory_path)

        with open(_PTH_FILE_PATH, encoding="utf-8") as fd:
            pth_line = fd.read().strip()

        with open(os.path.join(site_directory_path, "sitecustomize.py"), "w", encoding="utf-8") as fd:
            fd.write(pth_line + "\n")

        os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [site_directory_path, os.environ.get("PYTHONPATH")]))

    def _spawn_executor(self):
        return concurrent.futures.ProcessPoolExecutor(1, mp_context=multiprocessing.get_context("spawn"))

    def test_model_trained_outside_of_crunch(self):
        monkey_patches.pickle_unpickler_find_class()

        module = load_user_code(self._main_file_path, self._module_name)

        self.assertEqual(6, module.load_model(self._model_file_path).predict(2))

    def test_notebook_keeps_its_main_module(self):
        monkey_patches.pickle_unpickler_find_class()
        monkey_patches.prepare_for_process_initializer(self._module_name)

        main_module = sys.modules["__main__"]
        notebook_class = type("NotebookModel", (), {"__module__": "__main__"})
        setattr(main_module, "NotebookModel", notebook_class)

        try:
            self.assertIs(notebook_class, pickle.loads(pickle.dumps(notebook_class)))
        finally:
            delattr(main_module, "NotebookModel")

    def test_spawned_process_without_the_pth(self):
        load_user_code(self._main_file_path, self._module_name)

        with self._spawn_executor() as executor:
            self.assertEqual(9, executor.submit(square, 3).result())

    def test_spawned_process_with_the_pth(self):
        self._install_pth_in_child_processes()

        module = load_user_code(self._main_file_path, self._module_name)

        with self._spawn_executor() as executor:
            self.assertEqual([(0, "main.py"), (1, "main.py"), (4, "main.py")], list(executor.map(module.describe, range(3))))

            model = executor.submit(module.load_model, self._model_file_path).result()

        self.assertIsInstance(model, module.Model)
        self.assertEqual(6, model.predict(2))
