import os

from ..container import Columns, Features

is_inside = os.getenv("CRUNCH_INSIDE_RUNNER", "false").lower() == "true"
