import os
from typing import Optional

from dotenv import load_dotenv

from crunch.constants import API_BASE_URL_ENV_VAR, COMPETITIONS_SOURCE_DEFAULT, COMPETITIONS_SOURCE_ENV_VAR, DEBUG_ENV_VAR, ENVIRONMENT_ALIASES, ENVIRONMENT_ENV_VAR, ENVIRONMENT_PRODUCTION, ENVIRONMENTS, WEB_BASE_URL_ENV_VAR
from crunch.external import humanfriendly

__all__ = [
    "debug",
    "environment",
    "api_base_url",
    "web_base_url",
    "competitions_source",
    "reload",
    "override",
]

debug: bool = False

environment: str = ENVIRONMENT_PRODUCTION
api_base_url, web_base_url = ENVIRONMENTS[ENVIRONMENT_PRODUCTION]

competitions_source: str = COMPETITIONS_SOURCE_DEFAULT


def reload() -> None:
    load_dotenv(".env", verbose=False)

    global debug, environment, api_base_url, web_base_url, competitions_source

    debug = False
    environment = ENVIRONMENT_PRODUCTION
    api_base_url, web_base_url = ENVIRONMENTS[ENVIRONMENT_PRODUCTION]
    competitions_source = COMPETITIONS_SOURCE_DEFAULT

    _apply(
        new_debug=humanfriendly.coerce_boolean(os.getenv(DEBUG_ENV_VAR)),
        new_environment=os.getenv(ENVIRONMENT_ENV_VAR),
        new_api_base_url=os.getenv(API_BASE_URL_ENV_VAR),
        new_web_base_url=os.getenv(WEB_BASE_URL_ENV_VAR),
        new_competitions_source=os.getenv(COMPETITIONS_SOURCE_ENV_VAR),
        verbose=False,
    )


def override(
    *,
    debug: Optional[bool] = None,
    environment: Optional[str] = None,
    api_base_url: Optional[str] = None,
    web_base_url: Optional[str] = None,
    competitions_source: Optional[str] = None,
) -> None:
    _apply(
        new_debug=debug,
        new_environment=environment,
        new_api_base_url=api_base_url,
        new_web_base_url=web_base_url,
        new_competitions_source=competitions_source,
        verbose=True,
    )


def _apply(
    *,
    new_debug: Optional[bool],
    new_environment: Optional[str],
    new_api_base_url: Optional[str],
    new_web_base_url: Optional[str],
    new_competitions_source: Optional[str],
    verbose: bool,
) -> None:
    global debug, environment, api_base_url, web_base_url, competitions_source

    if new_debug is not None:
        debug = new_debug

    if new_competitions_source is not None:
        competitions_source = new_competitions_source

    if new_environment is not None:
        new_environment = ENVIRONMENT_ALIASES.get(new_environment, new_environment)

        if new_environment in ENVIRONMENTS:
            environment = new_environment
            api_base_url, web_base_url = ENVIRONMENTS[new_environment]

            if verbose:
                print(f"environment: using {new_environment} urls")

            return

        if verbose:
            print(f"environment: unknown environment `{new_environment}`, ignoring it")

    if new_api_base_url is not None:
        api_base_url = new_api_base_url

    if new_web_base_url is not None:
        web_base_url = new_web_base_url


reload()
