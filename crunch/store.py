import os
import humanfriendly
import dotenv

from . import constants, utils

debug: bool = None
web_base_url: str = None
api_base_url: str = None
session: utils.CustomSession = None

def load_from_env():
    global debug, web_base_url, api_base_url, session

    if session is not None:
        return

    dotenv.load_dotenv(".env", verbose=True)

    debug = humanfriendly.coerce_boolean(os.getenv(constants.DEBUG_ENV_VAR))
    web_base_url = os.getenv(constants.WEB_BASE_URL_ENV_VAR, constants.WEB_BASE_URL_DEFAULT)
    api_base_url = os.getenv(constants.API_BASE_URL_ENV_VAR, constants.API_BASE_URL_DEFAULT)

    session = utils.CustomSession(
        web_base_url,
        api_base_url,
        debug,
    )
