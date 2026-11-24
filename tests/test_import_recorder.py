import builtins
import importlib
import os
import sys
import tempfile
import textwrap
import unittest

from crunch.library import ImportRecorder


class ImportRecorderTest(unittest.TestCase):

    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self._prefix = f"crunch_import_recorder_{os.getpid()}_{id(self) % 100000}"

        self.project_path = os.path.join(self._directory.name, "project")
        self.site_packages_path = os.path.join(self._directory.name, "site-packages")
        os.makedirs(self.project_path)
        os.makedirs(self.site_packages_path)

        sys.path.insert(0, self.project_path)
        sys.path.insert(0, self.site_packages_path)

        self.library = self._name("library")
        self._write(self.site_packages_path, self.library, "")

    def tearDown(self):
        sys.path.remove(self.project_path)
        sys.path.remove(self.site_packages_path)

        for name in list(sys.modules):
            if name.startswith(self._prefix):
                del sys.modules[name]

        importlib.invalidate_caches()
        self._directory.cleanup()

    def _name(self, name: str):
        return f"{self._prefix}_{name}"

    def _write(self, directory_path: str, module_name: str, content: str):
        with open(os.path.join(directory_path, f"{module_name}.py"), "w") as fd:
            fd.write(textwrap.dedent(content))

    def _record(self, main: str):
        main_name = self._name("main")
        self._write(self.project_path, main_name, main)
        importlib.invalidate_caches()

        with ImportRecorder(self.project_path) as recorder:
            importlib.import_module(main_name)

        return {
            module_name: (os.path.basename(location.file_path), location.line)
            for module_name, location in recorder.imports.items()
        }

    def test_import(self):
        imports = self._record(f"""
            import os

            import {self.library}
        """)

        main_file_name = f"{self._name('main')}.py"
        self.assertEqual({"os": (main_file_name, 2), self.library: (main_file_name, 4)}, imports)

    def test_first_place_only(self):
        imports = self._record(f"import {self.library}\nimport {self.library}\n")

        self.assertEqual(1, imports[self.library][1])

    def test_from_import(self):
        imports = self._record(f"from {self.library} import *\n")

        self.assertEqual([self.library], list(imports))

    def test_import_module(self):
        imports = self._record(f"import importlib\nimportlib.import_module('{self.library}')\n")

        self.assertEqual(["importlib", self.library], list(imports))

    def test_import_inside_a_function(self):
        imports = self._record(f"""
            def train():
                import {self.library}

            train()
        """)

        self.assertEqual(3, imports[self.library][1])

    def test_imported_by_a_library(self):
        dependency = self._name("dependency")
        self._write(self.site_packages_path, dependency, "")
        self._write(self.site_packages_path, self.library, f"import {dependency}\n")

        imports = self._record(f"import {self.library}\n")

        self.assertEqual([self.library], list(imports))

    def test_own_module(self):
        helper = self._name("helper")
        self._write(self.project_path, helper, f"import {self.library}\n")

        imports = self._record(f"import {helper}\n")

        self.assertEqual({self.library: (f"{helper}.py", 1)}, imports)

    def test_restores_the_import_functions(self):
        original_import = builtins.__import__
        original_import_module = importlib.import_module

        with ImportRecorder(self.project_path):
            self.assertIsNot(original_import, builtins.__import__)

        self.assertIs(original_import, builtins.__import__)
        self.assertIs(original_import_module, importlib.import_module)
