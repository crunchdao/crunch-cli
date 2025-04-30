import os
import typing

import dotenv

from . import constants
from .external import humanfriendly

debug: bool = None
web_base_url: str = None
api_base_url: str = None
competitions_repository: str = None
competitions_branch: str = None
competitions_directory_path: typing.Optional[str] = None


def load_from_env():
    global debug
    global web_base_url, api_base_url
    global competitions_repository, competitions_branch, competitions_directory_path

    dotenv.load_dotenv(".env", verbose=False)

    if not debug:
        debug = humanfriendly.coerce_boolean(os.getenv(constants.DEBUG_ENV_VAR))

    if web_base_url is None:
        web_base_url = os.getenv(constants.WEB_BASE_URL_ENV_VAR, constants.WEB_BASE_URL_PRODUCTION)
        api_base_url = os.getenv(constants.API_BASE_URL_ENV_VAR, constants.API_BASE_URL_PRODUCTION)

    if competitions_repository is None:
        competitions_repository = os.getenv(constants.COMPETITIONS_REPOSITORY_ENV_VAR, constants.COMPETITIONS_REPOSITORY)
        competitions_branch = os.getenv(constants.COMPETITIONS_BRANCH_ENV_VAR, constants.COMPETITIONS_BRANCH)
        competitions_directory_path = os.getenv(constants.COMPETITIONS_DIRECTORY_PATH_ENV_VAR, None)
