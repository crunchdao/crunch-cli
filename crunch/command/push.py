import os
import typing

from .. import api, constants, utils


def _to_unix_path(input: str):
    return os.path.normpath(input)\
        .replace("\\", "/")\
        .replace("//", "/")


def _build_gitignore(
    directory_path: str,
    ignored_paths: typing.List[str],
    use_parent_gitignore: bool,
):
    from ..external import gitignorefile

    ignored_files = gitignorefile._IgnoreRules(
        rules=[
            gitignorefile._rule_from_pattern(line)
            for line in ignored_paths
        ],
        base_path=directory_path,
    )

    parts_depth = 2 if use_parent_gitignore else 1
    parts = tuple(gitignorefile._path_split(directory_path))[:-parts_depth]

    git_ignores = gitignorefile.Cache()
    git_ignores._Cache__gitignores[parts] = []

    return lambda name: (ignored_files.match(name), git_ignores(name))


def _list_files(
    directory_path: str,
    ignored_paths: typing.List[str],
    /,
    use_parent_gitignore: bool = False,
):
    directory_path = _to_unix_path(directory_path)
    is_ignored = _build_gitignore(directory_path, ignored_paths, use_parent_gitignore)

    for root, _, files in os.walk(directory_path, topdown=False):
        root = _to_unix_path(root)

        if root.startswith(directory_path):
            root = root[len(directory_path) + len("/"):]
        elif root == directory_path:
            root = ""

        for file in files:
            relative_path = _to_unix_path(os.path.join(root, file))
            absolute_path = _to_unix_path(os.path.join(directory_path, relative_path))

            if any(is_ignored(absolute_path)):
                continue

            yield absolute_path, relative_path


def list_code_files(
    submission_directory_path: str,
    model_directory_relative_path: str,
):
    return _list_files(
        submission_directory_path,
        [
            *constants.IGNORED_CODE_FILES,
            _to_unix_path(f"/{model_directory_relative_path}/"),
        ],
    )


def list_model_files(
    submission_directory_path: str,
    model_directory_relative_path: str,
):
    model_directory_path = os.path.join(
        submission_directory_path,
        model_directory_relative_path,
    )

    return _list_files(
        model_directory_path,
        constants.IGNORED_MODEL_FILES,
        use_parent_gitignore=True,
    )


def push(
    message: str,
    main_file_path: str,
    model_directory_relative_path: str,
    include_installed_packages_version: bool,
    dry: bool,
):
    submission_directory_path = os.path.abspath(".")

    client, project = api.Client.from_project()
    competition = project.competition

    installed_packages_version = utils.pip_freeze() if include_installed_packages_version else {}

    preferred_chunk_size = 50_000_000

    def do_upload(path, name, size):
        if dry:
            return None

        return client.uploads.send_from_file(path, name, size, preferred_chunk_size, progress_bar=True)

    code_uploads = {}
    model_uploads = {}

    try:
        total_size = 0

        for path, name in list_code_files(submission_directory_path, model_directory_relative_path):
            size = os.path.getsize(path)
            total_size += size

            print(f"found code file: {name} ({utils.format_bytes(size)})")
            code_uploads[name] = do_upload(path, name, size)

        for path, name in list_model_files(submission_directory_path, model_directory_relative_path):
            size = os.path.getsize(path)
            total_size += size

            print(f"found model file: {name} ({utils.format_bytes(size)})")
            model_uploads[name] = do_upload(path, name, size)

        print(f"total size: {utils.format_bytes(total_size)}")

        if dry:
            print("dry run, not uploading files")
            return None

        print(f"export {competition.name}:project/{project.user_id}/{project.name}")
        submission = project.submissions.create(
            message=message,
            main_file_path=main_file_path,
            model_directory_path=model_directory_relative_path,
            type=api.SubmissionType.CODE,
            preferred_packages_version=installed_packages_version,
            code_files={
                path: upload.id
                for path, upload in code_uploads.items()
            },
            model_files={
                path: upload.id
                for path, upload in model_uploads.items()
            },
        )

        _print_success(client, submission)

        return submission
    finally:
        if not dry:
            _cleanup(code_uploads, model_uploads)


def _cleanup(
    code_uploads: dict[str, api.Upload],
    model_uploads: dict[str, api.Upload]
):
    for upload in code_uploads.values():
        try:
            upload.delete()
        except api.ApiException as exception:
            print(f"cleanup error {exception}")

    for upload in model_uploads.values():
        try:
            upload.delete()
        except api.ApiException as exception:
            print(f"cleanup error {exception}")


def _print_success(
    client: api.Client,
    submission: api.Submission
):
    print("\n---")
    print(f"submission #{submission.number} succesfully uploaded!")
    print()

    project = submission.project
    competition = project.competition

    url = client.format_web_url(f"/competitions/{competition.name}/projects/{project.user_id}/{project.name}/submissions/{submission.number}")
    print(f"find it on your dashboard: {url}")
