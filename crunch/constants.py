DOT_CRUNCHDAO_DIRECTORY = ".crunchdao"
DOT_DATA_DIRECTORY = "data"
TOKEN_FILE = "token"
OLD_PROJECT_FILE = "project"
PROJECT_FILE = "project.json"
DOT_GITIGNORE_FILE = ".gitignore"
REQUIREMENTS_TXT = "requirements.txt"
CONVERTED_MAIN_PY = "__converted_main.py"
DEFAULT_MODEL_DIRECTORY = "resources"

IGNORED_FILES = [
    ".git/",
    f"{DOT_CRUNCHDAO_DIRECTORY}/",
    f"{DOT_DATA_DIRECTORY}/",
    f"__pycache__/",
    f".env",
]

DEBUG_ENV_VAR = "CRUNCH_DEBUG"
API_KEY_ENV_VAR = "CRUNCH_API_KEY"

API_BASE_URL_ENV_VAR = "API_BASE_URL"
API_BASE_URL_DEFAULT = "https://api.hub.crunchdao.com/"
API_BASE_URL_STAGING = "https://api.hub.crunchdao.io/"
WEB_BASE_URL_ENV_VAR = "WEB_BASE_URL"
WEB_BASE_URL_DEFAULT = "https://hub.crunchdao.com/"
WEB_BASE_URL_STAGING = "https://hub.crunchdao.io/"
