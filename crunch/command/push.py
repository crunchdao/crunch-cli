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
    import gitignorefile

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
    message: str,
    main_file_path: str,
    model_directory_path: str,
    include_installed_packages_version: bool,
    dry: bool,
    export_path: str = None,
):
    import pkg_resources

    client, project = api.Client.from_project()
    competition = project.competition

    installed_packages_version = {}
    if include_installed_packages_version:
        installed_packages_version = {
            package.project_name: package.version
            for package in pkg_resources.working_set
            if utils.is_valid_version(package.version)
        }

    fds = []

    try:
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

                print(f"export {competition.name}:project/{project.user_id}/{project.name}")
                if dry:
                    print("create dry (no upload)")
                else:
                    print("create on server")

                    if store.debug:
                        def sse_handler(event):
                            data = event.data
                            if isinstance(data, dict):
                                data = " ".join(
                                    f"{key}={value}"
                                    for key, value in data.items()
                                )

                            print(f"server: {event.event}: {data}")
                    else:
                        sse_handler = None

                    submission = project.submissions.create(
                        message=message,
                        main_file_path=main_file_path,
                        model_directory_path=model_directory_path,
                        type=api.SubmissionType.CODE,
                        preferred_packages_version=installed_packages_version,
                        files=files,
                        sse_handler=sse_handler
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
