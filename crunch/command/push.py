import os
import shutil
import tarfile
import tempfile

from .. import api, constants, store, utils


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
    from ..external import gitignorefile

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


def _print_sse_handler(event):
    data = event.data
    if isinstance(data, dict):
        data = " ".join(
            f"{key}={value}"
            for key, value in data.items()
        )

    print(f"server: {event.event}: {data}")


def push(
    message: str,
    main_file_path: str,
    model_directory_path: str,
    include_installed_packages_version: bool,
    dry: bool,
    export_path: str = None,
):
    client, project = api.Client.from_project()
    competition = project.competition

    installed_packages_version = utils.pip_freeze() if include_installed_packages_version else {}

    fds = []

    try:
        with tempfile.NamedTemporaryFile(prefix="submission-", suffix=".tar", dir=constants.DOT_CRUNCHDAO_DIRECTORY) as tmp:
            with tarfile.open(fileobj=tmp, mode="w") as tar:
                for file in _list_code_files(model_directory_path):
                    size = os.path.getsize(file)
                    print(f"compress {file} ({utils.format_bytes(size)})")

                    tar.add(file)

            total_size = tmp.tell()
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
                    size = os.path.getsize(path)
                    print(f"model {name} ({utils.format_bytes(size)})")

                    fd = open(path, "rb")
                    fds.append(fd)

                    files.append(("modelFiles", (name, fd)))
                    total_size += size

                print(f"export {competition.name}:project/{project.user_id}/{project.name}")
                if dry:
                    print("create dry (no upload)")
                else:
                    print(f"create on server ({utils.format_bytes(total_size)})")

                    submission = project.submissions.create(
                        message=message,
                        main_file_path=main_file_path,
                        model_directory_path=model_directory_path,
                        type=api.SubmissionType.CODE,
                        preferred_packages_version=installed_packages_version,
                        files=files,
                        sse_handler=_print_sse_handler if store.debug else None
                    )

                    _print_success(client, submission)

                    return submission
    finally:
        for fd in fds:
            fd.close()


def _print_success(
    client: api.Client,
    submission: api.Submission
):
    print("\n---")
    print(f"submission #{submission.number} succesfully uploaded!")

    files = [
        file
        for file in submission.files.list()
        if file.found_hardcoded_string
    ]

    if len(files):
        print("\nwarning: some of your files have hardcoded strings")

        for file in files:
            print(f"- {file.name}")

    print()

    project = submission.project
    competition = project.competition

    url = client.format_web_url(f"/competitions/{competition.name}/projects/{project.user_id}/{project.name}/submissions/{submission.number}")
    print(f"find it on your dashboard: {url}")
