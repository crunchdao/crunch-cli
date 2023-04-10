import os
import tarfile
import tempfile
import gitignorefile

from .. import utils
from .. import constants


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
):
    project_name = utils.read_project_name()
    push_token = utils.read_token()

    fds = []

    try:
        with tempfile.NamedTemporaryFile(prefix="submission-", suffix=".tar") as tmp:
            with tarfile.open(fileobj=tmp, mode="w") as tar:
                for file in _list_code_files(model_directory_path):
                    print(f"compress {file}")
                    tar.add(file)

            model_files = []

            for path, name in _list_model_files(model_directory_path):
                print(f"model {name}")

                fd = open(path, "rb")
                fds.append(fd)

                model_files.append((name, fd))

            tmp_fd = open(tmp.name, "rb")
            fds.append(tmp_fd)

            files = [
                ("codeFile", ('code.tar', tmp_fd, "application/x-tar"))
            ]

            for model_file in model_files:
                files.append(("modelFiles", model_file))

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
    finally:
        for fd in fds:
            fd.close()

    return submission


def push_summary(submission, session: utils.CustomSession):
    print("\n---")
    print(f"submission #{submission['number']} succesfully uploaded!")

    url = session.format_web_url(
        f"/project/submissions/{submission['number']}")
    print(f"Find it on your dashboard: {url}")

    model = submission.get("model")
    if model:
        print("\n")
        print(f"model #{model['number']} succesfully uploaded!")

        url = session.format_web_url(f"/project/models/{model['number']}")
        print(f"Find it on your dashboard: {url}")
