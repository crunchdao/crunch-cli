"""
crunch
~~~~~~
The crunch package - a Python cli used to submit
your work to the crunchdao platform easily!
"""

from . import api, checker, orthogonalization, scoring
from .inline import load as load_notebook
from .orthogonalization import run as alpha_score
from .runner import is_inside as is_inside_runner, Columns

from .container import StreamMessage
