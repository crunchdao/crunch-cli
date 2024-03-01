import os

import dotenv
import humanfriendly

from . import constants

debug: bool = None
web_base_url: str = None
api_base_url: str = None
_loaded = False


def load_from_env():
    global debug, web_base_url, api_base_url, _loaded

    if _loaded:
        return

    dotenv.load_dotenv(".env", verbose=True)

    debug = humanfriendly.coerce_boolean(os.getenv(constants.DEBUG_ENV_VAR))
    web_base_url = os.getenv(constants.WEB_BASE_URL_ENV_VAR, constants.WEB_BASE_URL_DEFAULT)
    api_base_url = os.getenv(constants.API_BASE_URL_ENV_VAR, constants.API_BASE_URL_DEFAULT)

    _loaded = True
