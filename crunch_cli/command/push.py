import os
import tarfile
import tempfile
import requests
import gitignorefile
import urllib.parse

from .. import utils
from .. import constants


def push(
    session: requests.Session,
    message: str,
    web_base_url: str = None,
):
    utils.change_root()

    project_name = utils.read_project_name()
    push_token = utils.read_token()

    matches = gitignorefile.Cache()

    with tempfile.NamedTemporaryFile(prefix="version-", suffix=".tar") as tmp:
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
            version = session.post(
                f"/v1/projects/{project_name}/versions",
                data={
                    "message": message,
                    "pushToken": push_token,
                },
                files={
                    "tarFile": ('code.tar', fd, "application/x-tar")
                }
            ).json()

    print("\n---")
    print(f"version #{version['number']} uploaded!")

    if web_base_url is not None:
        url = urllib.parse.urljoin(web_base_url, f"/project/versions/{version['number']}")
        print(f"check your version: {url}")
