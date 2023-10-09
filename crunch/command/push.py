import os
import tarfile
import tempfile
import shutil
import pkg_resources

import gitignorefile

from .. import constants, utils
from .setup import _setup_demo


def _list_files(
    directory_path: str,
):
    directory_path = f"{directory_path}/"
    for root, _, files in os.walk(directory_path, topdown=False):
        root = utils.to_unix_path(root)

        if root.startswith(directory_path):
            root = root[len(directory_path):]
        elif root == directory_path:
            root = ""

        for file in files:
            file = utils.to_unix_path(os.path.join(root, file))

            yield file


def _list_code_files(
    model_directory_path: str
):
    ignore_files = [
        *constants.IGNORED_FILES,
        utils.to_unix_path(f"{model_directory_path}/")
    ]

    matches = gitignorefile.Cache()
    parts = tuple(gitignorefile._path_split(os.path.abspath(".")))[:-1]
    matches._Cache__gitignores[parts] = []

    for file in _list_files("."):
        ignored = False
        for ignore in ignore_files:
            if ignore in file:
                ignored = True
                break

        if ignored or matches(file):
            continue

        yield file


def _list_model_files(
    model_directory_path: str,
):
    for name in _list_files(model_directory_path):
        path = utils.to_unix_path(os.path.join(model_directory_path, name))
        yield path, name


def push(
    session: utils.CustomSession,
    message: str,
    main_file_path: str,
    model_directory_path: str,
    include_installed_packages_version: bool,
    export_path: str = None,
):
    project_info = utils.read_project_info()
    push_token = utils.read_token()
    
    installed_packages_version = {}
    if include_installed_packages_version:
        installed_packages_version = {
            package.project_name: package.version
            for package in pkg_resources.working_set
            if utils.is_valid_version(package.version)
        }

    fds = []

    try:
        if not os.path.exists(constants.DOT_GITIGNORE_FILE):
            _setup_demo(".",  [constants.DOT_GITIGNORE_FILE])

        with tempfile.NamedTemporaryFile(prefix="submission-", suffix=".tar", dir=constants.DOT_CRUNCHDAO_DIRECTORY) as tmp:
            with tarfile.open(fileobj=tmp, mode="w") as tar:
                for file in _list_code_files(model_directory_path):
                    print(f"compress {file}")
                    tar.add(file)

            tmp.seek(0)

            if export_path:
                print(f"export {export_path}")

                with open(export_path, "wb") as fd:
                    shutil.copyfileobj(tmp, fd)
            else:
                files = [
                    ("codeFile", ('code.tar', tmp, "application/x-tar"))
                ]

                for path, name in _list_model_files(model_directory_path):
                    print(f"model {name}")

                    fd = open(path, "rb")
                    fds.append(fd)

                    files.append(("modelFiles", (name, fd)))

                print(f"export {project_info.competition_name}:project/{project_info.user_id}")
                submission = session.post(
                    f"/v2/competitions/{project_info.competition_name}/projects/{project_info.user_id}/submissions",
                    data={
                        "message": message,
                        "mainFilePath": main_file_path,
                        "modelDirectoryPath": model_directory_path,
                        "pushToken": push_token,
                        "notebook": False,
                        **{
                            f"preferredPackagesVersion[{key}]": value
                            for key, value in installed_packages_version.items()
                        }
                    },
                    files=tuple(files)
                ).json()

                _print_success(session, project_info, submission)

                return submission
    finally:
        for fd in fds:
            fd.close()


def _print_success(session: utils.CustomSession, project_info: utils.ProjectInfo, submission: dict):
    print("\n---")
    print(f"submission #{submission['number']} succesfully uploaded!")

    url = session.format_web_url(f"/competitions/{project_info.competition_name}/projects/{project_info.user_id}/submissions/{submission['number']}")
    print(f"Find it on your dashboard: {url}")
