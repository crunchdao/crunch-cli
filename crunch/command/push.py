import os
import tarfile
import tempfile
import gitignorefile

from .. import utils
from .. import constants


def push(
    session: utils.CustomSession,
    message: str,
    main_file_path: str,
):
    project_name = utils.read_project_name()
    push_token = utils.read_token()

    matches = gitignorefile.Cache()

    with tempfile.NamedTemporaryFile(prefix="submission-", suffix=".tar") as tmp:
        with tarfile.open(fileobj=tmp, mode="w") as tar:
            for root, dirs, files in os.walk(".", topdown=False):
                if root.startswith("./"):
                    root = root[2:]
                elif root == ".":
                    root = ""

                for file in files:
                    file = os.path.join(root, file)

                    ignored = False
                    for ignore in constants.IGNORED_FILES:
                        if ignore in file:
                            ignored = True
                            break

                    if ignored or matches(file):
                        continue

                    print(f"compress {file}")
                    tar.add(file)

        with open(tmp.name, "rb") as fd:
            submission = session.post(
                f"/v1/projects/{project_name}/submissions",
                data={
                    "message": message,
                    "mainFilePath": main_file_path,
                    "pushToken": push_token,
                    "notebook": False
                },
                files={
                    "file": ('code.tar', fd, "application/x-tar")
                }
            ).json()

    return submission


def push_summary(submission, session: utils.CustomSession):
    print("\n---")
    print(f"submission #{submission['number']} succesfully uploaded!")

    url = session.format_web_url(f"/project/submissions/{submission['number']}")
    print(f"Find it on your dashboard: {url}")
