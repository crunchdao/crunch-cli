from .domain import *
from .client import Client
from . import auth

# update errors list:
# python -c 'import crunch.api.errors; print("\n".join(list(filter(lambda x: x.endswith("Exception"), vars(crunch.api.errors).keys()))))'

from .errors import (
    ApiException,
    InternalServerException,
    RetryableException,
    AnnotatedConnectException,
    NullPointerException,
    ValidationFailedException,
    CheckException,
    CompetitionNameNotFoundException,
    CrunchNotFoundException,
    CurrentPhaseNotFoundException,
    DailySubmissionLimitExceededException,
    ForbiddenLibraryException,
    InvalidProjectTokenException,
    NeverSubmittedException,
    ProjectNotFoundException,
    RunNotFoundException,
    RestrictedPhaseActionException,
    RoundNotFoundException,
    CurrentRoundNotFoundException,
    LatestRoundNotFoundException,
    NextRoundNotFoundException
)
