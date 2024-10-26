import os

import dotenv

from . import constants
from .external import humanfriendly

debug: bool = None
web_base_url: str = None
api_base_url: str = None


def load_from_env():
    global debug, web_base_url, api_base_url

    dotenv.load_dotenv(".env", verbose=False)

    if not debug:
        debug = humanfriendly.coerce_boolean(os.getenv(constants.DEBUG_ENV_VAR))

    if web_base_url is None:
        web_base_url = os.getenv(constants.WEB_BASE_URL_ENV_VAR, constants.WEB_BASE_URL_PRODUCTION)
        api_base_url = os.getenv(constants.API_BASE_URL_ENV_VAR, constants.API_BASE_URL_PRODUCTION)
