import contextlib
import importlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest

from crunch import monkey_patches

_THIRD_PARTY_MODULES = ["tqdm", "keras", "tensorflow", "catboost", "pycaret", "joblib"]


class ApplyAllTest(unittest.TestCase):

    def test_imports_no_third_party_library(self):
        output = subprocess.run(
            [sys.executable, "-c", "import json, sys; import crunch.monkey_patches as patches; patches.apply_all(); print(json.dumps(sorted(sys.modules)))"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout

        imported = set(json.loads(output.splitlines()[-1]))

        self.assertEqual([], [name for name in _THIRD_PARTY_MODULES if name in imported])


class LazyPatchTest(unittest.TestCase):

    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self._package_name = f"crunch_lazy_patch_{os.getpid()}_{id(self)}"

        package_path = os.path.join(self._directory.name, self._package_name)
        os.makedirs(package_path)

        with open(os.path.join(package_path, "__init__.py"), "w") as fd:
            fd.write("")

        with open(os.path.join(package_path, "target.py"), "w") as fd:
            fd.write("VALUE = 1\n")

        with open(os.path.join(package_path, "relative.py"), "w") as fd:
            fd.write("from . import target\n")

        with open(os.path.join(package_path, "broken.py"), "w") as fd:
            fd.write("raise RuntimeError('broken on purpose')\n")

        sys.path.insert(0, self._directory.name)
        importlib.invalidate_caches()

        self._calls: list[str] = []

    def tearDown(self):
        sys.path.remove(self._directory.name)

        for name in list(sys.modules):
            if name == self._package_name or name.startswith(self._package_name + "."):
                del sys.modules[name]

        self._directory.cleanup()

    def _module_name(self, name: str):
        return f"{self._package_name}.{name}"

    def _patch(self, name: str, *, fail: bool = False):
        module_name = self._module_name(name)

        def patch_target():
            self._calls.append(module_name)

            if fail:
                raise ValueError("failed on purpose")

            setattr(sys.modules[module_name], "PATCHED", True)

        patch = monkey_patches.Patch(patch_target, module_name)
        patch()

        return patch

    def test_not_applied_before_the_import(self):
        self._patch("target")

        self.assertEqual([], self._calls)
        self.assertNotIn(self._module_name("target"), sys.modules)

    def test_absolute_import(self):
        self._patch("target")

        module = importlib.import_module(self._module_name("target"))

        self.assertTrue(module.PATCHED)
        self.assertEqual([self._module_name("target")], self._calls)

    def test_relative_import(self):
        self._patch("target")

        importlib.import_module(self._module_name("relative"))

        self.assertTrue(sys.modules[self._module_name("target")].PATCHED)

    def test_find_spec_before_the_import(self):
        self._patch("target")

        self.assertIsNotNone(importlib.util.find_spec(self._module_name("target")))
        self.assertEqual([], self._calls)

        module = importlib.import_module(self._module_name("target"))

        self.assertTrue(module.PATCHED)

    def test_already_imported(self):
        importlib.import_module(self._module_name("target"))

        self._patch("target")

        self.assertTrue(sys.modules[self._module_name("target")].PATCHED)

    def test_applied_once(self):
        patch = self._patch("target")

        module = importlib.import_module(self._module_name("target"))
        importlib.reload(module)
        patch()

        self.assertEqual([self._module_name("target")], self._calls)

    def test_module_keeps_its_real_loader(self):
        self._patch("target")

        module = importlib.import_module(self._module_name("target"))

        self.assertEqual("SourceFileLoader", type(module.__loader__).__name__)
        self.assertIs(module.__loader__, module.__spec__.loader)

    def test_failing_patch_does_not_break_the_import(self):
        self._patch("target", fail=True)

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            module = importlib.import_module(self._module_name("target"))

        self.assertEqual(1, module.VALUE)
        self.assertIn("could not apply the `patch_target` patch: ValueError: failed on purpose", stderr.getvalue())

    def test_failing_import_keeps_watching(self):
        self._patch("broken")

        with self.assertRaises(RuntimeError):
            importlib.import_module(self._module_name("broken"))

        with open(os.path.join(self._directory.name, self._package_name, "broken.py"), "w") as fd:
            fd.write("VALUE = 2\n")

        importlib.invalidate_caches()
        module = importlib.import_module(self._module_name("broken"))

        self.assertTrue(module.PATCHED)
