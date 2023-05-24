import os
import tarfile
import tempfile
import shutil

import gitignorefile

from .. import constants, utils


def _list_files(
    directory_path: str,
):
    for root, dirs, files in os.walk(directory_path, topdown=False):
        if root.startswith(f"{directory_path}/"):
            root = root[len(directory_path) + 1:]
        elif root == directory_path:
            root = ""

        for file in files:
            file = os.path.join(root, file)

            yield file


def _list_code_files(
    model_directory_path: str
):
    ignore_files = [
        *constants.IGNORED_FILES,
        f"{model_directory_path}/".replace("//", "/")
    ]

    matches = gitignorefile.Cache()

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
        path = os.path.join(model_directory_path, name)
        yield path, name


def push(
    session: utils.CustomSession,
    message: str,
    main_file_path: str,
    model_directory_path: str,
    export_path: str = None
):
    project_name = utils.read_project_name()
    push_token = utils.read_token()

    fds = []

    try:
        with tempfile.NamedTemporaryFile(prefix="submission-", suffix=".tar", dir=constants.DOT_CRUNCHDAO_DIRECTORY) as tmp:
            with tarfile.open(fileobj=tmp, mode="w") as tar:
                for file in _list_code_files(model_directory_path):
                    print(f"compress {file}")
                    tar.add(file)

            tmp.seek(0)

            files = [
                ("codeFile", ('code.tar', tmp, "application/x-tar"))
            ]

            for path, name in _list_model_files(model_directory_path):
                print(f"model {name}")

                fd = open(path, "rb")
                fds.append(fd)

                files.append(("modelFiles", (name, fd)))

            if export_path:
                print(f"export {export_path}")
                shutil.copyfile(tmp.name, export_path)
            else:
                print(f"export project/{project_name}")
                submission = session.post(
                    f"/v1/projects/{project_name}/submissions",
                    data={
                        "message": message,
                        "mainFilePath": main_file_path,
                        "modelDirectoryPath": model_directory_path,
                        "pushToken": push_token,
                        "notebook": False
                    },
                    files=tuple(files)
                ).json()

                return submission
    finally:
        for fd in fds:
            fd.close()


def push_summary(submission, session: utils.CustomSession):
    print("\n---")
    print(f"submission #{submission['number']} succesfully uploaded!")

    url = session.format_web_url(f"/project/submissions/{submission['number']}")
    print(f"Find it on your dashboard: {url}")
    