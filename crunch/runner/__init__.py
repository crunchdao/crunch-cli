import os

from .columns import Columns

is_inside = os.getenv("CRUNCH_INSIDE_RUNNER", "false").lower() == "true"
