import json
import subprocess
import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).parent.parent

# only needed once a command runs, must not be imported by `crunch --help`
_HEAVY_MODULES = [
    "requests",
    "urllib3",
    "mashumaro",
    "pendulum",
    "tqdm",
    "psutil",
    "pandas",
    "crunch_convert",
    "crunch.api._endpoint",
    "crunch.tester",
    "crunch.command.push",
    "crunch.command.download",
    "crunch.runner.tracing",
]


def _imported_modules(code: str) -> set[str]:
    output = subprocess.run(
        [sys.executable, "-c", f"{code}\nimport sys, json\nprint(json.dumps(sorted(sys.modules)))"],
        capture_output=True,
        text=True,
        check=True,
        cwd=_ROOT,
    ).stdout

    return set(json.loads(output.splitlines()[-1]))


class StartupTest(unittest.TestCase):

    def test_cli_import_is_light(self):
        imported = _imported_modules("import crunch.cli")

        self.assertEqual([], [name for name in _HEAVY_MODULES if name in imported])

    def test_api_import_is_light(self):
        imported = _imported_modules("import crunch.api")

        self.assertEqual([], [name for name in _HEAVY_MODULES if name in imported])
