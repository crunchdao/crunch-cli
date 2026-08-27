import os

from dotenv import load_dotenv

import crunch.constants as constants
from crunch.external import humanfriendly

debug: bool = None  # pyright: ignore[reportAssignmentType]
web_base_url: str = None  # pyright: ignore[reportAssignmentType]
api_base_url: str = None  # pyright: ignore[reportAssignmentType]
competitions_source: str = None  # pyright: ignore[reportAssignmentType]


def load_from_env():
    global debug
    global web_base_url, api_base_url
    global competitions_source

    load_dotenv(".env", verbose=False)

    if not debug:
        debug = humanfriendly.coerce_boolean(os.getenv(constants.DEBUG_ENV_VAR))

    if web_base_url is None:  # pyright: ignore[reportUnnecessaryComparison]
        web_base_url = os.getenv(constants.WEB_BASE_URL_ENV_VAR, constants.WEB_BASE_URL_PRODUCTION)
        api_base_url = os.getenv(constants.API_BASE_URL_ENV_VAR, constants.API_BASE_URL_PRODUCTION)

    if competitions_source is None:  # pyright: ignore[reportUnnecessaryComparison]
        competitions_source = os.getenv(constants.COMPETITIONS_SOURCE_ENV_VAR, constants.COMPETITIONS_SOURCE_DEFAULT)
