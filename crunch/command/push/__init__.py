import os
import sys
from datetime import datetime
from enum import Enum, auto
from io import BytesIO
from typing import BinaryIO, Callable, Dict, List, Literal, Optional, Tuple, overload

import click

from crunch import store
from crunch.api import Client, ForbiddenLibraryException, Submission, SubmissionType, Upload, UploadStatus
from crunch.constants import COLAB_DETECTION_ENV_VAR, COLAB_IGNORED_CODE_FILES, IGNORED_CODE_FILES, IGNORED_MODEL_FILES, SUBMISSION_MESSAGE_LENGTH
from crunch.external.humanfriendly import format_size, format_timespan

from ._cache import FileUploadCache, NoUploadCache, UploadCache, to_modification_time


class RequirementsMode(Enum):
    # ignore while processing the resources/ directory
    IGNORE = auto()

    # include the raw file, just try to validate it locally
    INCLUDE = auto()

    # try to freeze it and include both files if different
    FREEZE = auto()


def _to_unix_path(input: str):
    if input == ".":
        return input + "/"

    has_trailing_slash = input.endswith(("/", "\\"))

    input = os.path.normpath(input)\
        .replace("\\", "/")\
        .replace("//", "/")

    if has_trailing_slash and not input.endswith("/"):
        input += "/"

    return input


def _build_gitignore(
    directory_path: str,
    ignored_paths: List[str],
    use_parent_gitignore: bool,
) -> Callable[[str], Tuple[bool, bool]]:
    from crunch.external import gitignorefile

    rules: List[gitignorefile._IgnoreRule] = []  # type: ignore
    for line in ignored_paths:
        line = line.rstrip("\r\n")
        rule = gitignorefile._rule_from_pattern(line)  # type: ignore
        if rule:
            rules.append(rule)

    ignored_files = gitignorefile._IgnoreRules(  # type: ignore
        rules=rules,
        base_path=directory_path,
    )

    parts_depth = 2 if use_parent_gitignore else 1
    parts = tuple(gitignorefile._path_split(directory_path))[:-parts_depth]  # type: ignore

    git_ignores = gitignorefile.Cache()
    git_ignores._Cache__gitignores[parts] = []  # type: ignore

    return lambda name: (
        ignored_files.match(name),  # type: ignore
        git_ignores(name)
    )


def _list_files(
    directory_path: str,
    ignored_paths: List[str],
    *,
    use_parent_gitignore: bool = False,
) -> Dict[str, str]:
    directory_path = _to_unix_path(directory_path)
    directory_path_prefix = (
        len(directory_path)
        + int(not directory_path.endswith("/"))  # add 1 if value not ends with a slash
    )

    is_ignored = _build_gitignore(directory_path, ignored_paths, use_parent_gitignore)
    found_files: Dict[str, str] = {}

    for root, _, files in os.walk(directory_path, topdown=False):
        root = _to_unix_path(root)

        if root.startswith(directory_path):
            root = root[directory_path_prefix:]
        elif root == directory_path:
            root = ""

        for file in files:
            relative_path = _to_unix_path(os.path.join(root, file))
            absolute_path = _to_unix_path(os.path.join(directory_path, relative_path))

            if any(is_ignored(relative_path)):
                continue

            found_files[relative_path] = absolute_path

    return found_files


def list_code_files(
    submission_directory_path: str,
    model_directory_relative_path: str,
):
    is_in_colab = os.getenv(COLAB_DETECTION_ENV_VAR) is not None

    return _list_files(
        submission_directory_path,
        [
            *IGNORED_CODE_FILES,
            *(COLAB_IGNORED_CODE_FILES if is_in_colab else []),
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
        IGNORED_MODEL_FILES,
        use_parent_gitignore=True,
    )


def _upload_files(
    *,
    group_name: str,
    storage: Dict[str, Upload],
    found_files: Dict[str, str],
    dry: bool,
    client: Client,
    preferred_chunk_size: int,
    requirements_mode: RequirementsMode,
    upload_cache: UploadCache,
):
    from crunch_convert import RequirementLanguage, requirements_txt

    total_size: int = 0

    now = datetime.now()

    def format_age(created_at: datetime) -> str:
        return format_timespan(now - created_at, max_units=1)

    def handle_bytes(
        *,
        data: bytes,
        name: str,
        log_action: Optional[str] = None,
    ):
        checksum, reused_upload = upload_cache.try_reuse_bytes(data=data)
        if reused_upload is not None:
            size = reused_upload.size

            print(f"reused cached {group_name} file: {name} ({format_size(size)}, {format_age(reused_upload.created_at)} old)")
            storage[name] = reused_upload

        else:
            size = len(data)

            upload = handle(
                io=BytesIO(data),
                name=name,
                size=size,
                log_action=log_action,
            )

            if upload is not None:
                upload_cache.register_bytes(checksum=checksum, upload=upload)

        nonlocal total_size
        total_size += size

    def handle_file(
        *,
        name: str,
        absolute_path: str,
    ):
        checksum, reused_upload = upload_cache.try_reuse_file(relative_path=name, absolute_path=absolute_path)
        if reused_upload is not None:
            size = reused_upload.size

            print(f"reused cached {group_name} file: {name} ({format_size(size)}, {format_age(reused_upload.created_at)} old)")
            storage[name] = reused_upload

        else:
            with open(absolute_path, "rb") as fd:
                stat = os.fstat(fd.fileno())
                size = stat.st_size

                upload = handle(
                    io=fd,
                    name=name,
                    size=size,
                )

            if upload is not None:
                upload_cache.register_file(checksum=checksum, upload=upload, relative_path=name, size=stat.st_size, modification_time=to_modification_time(stat))

        nonlocal total_size
        total_size += size

    def handle(
        io: BinaryIO,
        name: str,
        size: int,
        log_action: Optional[str] = None,
    ) -> Optional[Upload]:
        if log_action:
            print(f"{log_action}: {name} ({format_size(size)})")
        else:
            print(f"found {group_name} file: {name} ({format_size(size)})")

        if dry:
            return

        upload = storage[name] = client.uploads.send_from_io(
            io=io,
            name=name,
            size=size,
            preferred_chunk_size=preferred_chunk_size,
            progress_bar=True,
        )

        return upload

    def handle_requirements(
        *,
        path: str,
        language: RequirementLanguage,
        skip_freezing: bool = False,
    ):
        with open(path, "r") as fd:
            original_requirements_file = fd.read()

        try:
            requirements = requirements_txt.parse_from_file(
                language=language,
                file_content=original_requirements_file,
            )
        except requirements_txt.RequirementParseError as error:
            print(f"{language.txt_file_name}: {error}")
            raise click.Abort()

        whitelist = requirements_txt.CachedWhitelist(
            requirements_txt.CrunchHubWhitelist(
                api_base_url=store.api_base_url,
            )
        )

        forbidden_names: List[str] = []
        for requirement in requirements:
            library = whitelist.find_library(
                language=requirement.language,
                name=requirement.name,
            )

            if library is None:
                forbidden_names.append(requirement.name)

        if forbidden_names:
            raise ForbiddenLibraryException(
                "forbidden packages has been found",

                # TODO Find a better way!
                requirements=[
                    {"name": name, "language": language.name}
                    for name in forbidden_names
                ]
            )

        if skip_freezing:
            frozen_requirements = requirements
        else:
            frozen_requirements = requirements_txt.freeze(
                requirements=requirements,
                freeze_only_if_required=False,
                version_finder=requirements_txt.LocalSitePackageVersionFinder(),
            )

        if requirements == frozen_requirements:
            handle_bytes(
                data=original_requirements_file.encode("utf-8"),
                name=language.txt_file_name,
                log_action="using original file",
            )

            return False
        else:
            frozen_requirements_files = requirements_txt.format_files_from_named(
                frozen_requirements,
                header="frozen from local environment",
                whitelist=whitelist,
            )

            frozen_requirements_file = frozen_requirements_files[language]

            handle_bytes(
                data=frozen_requirements_file.encode("utf-8"),
                name=language.txt_file_name,
                log_action="froze file",
            )

            handle_bytes(
                data=original_requirements_file.encode("utf-8"),
                name=language.original_txt_file_name,
                log_action="rename original file",
            )

            return True

    if requirements_mode != RequirementsMode.IGNORE:
        for language in RequirementLanguage:
            text_file_absolute_path = found_files.pop(language.txt_file_name, None)
            if text_file_absolute_path is None:
                continue

            has_frozen = handle_requirements(
                path=text_file_absolute_path,
                language=language,
                skip_freezing=requirements_mode != RequirementsMode.FREEZE,
            )

            if has_frozen:
                found_files.pop(language.original_txt_file_name, None)

    for name, absolute_path in found_files.items():
        handle_file(
            name=name,
            absolute_path=absolute_path,
        )

    print(f"total {group_name} size: {format_size(total_size)}")


@overload
def push(
    *,
    message: str,
    main_file_path: str,
    model_directory_relative_path: str,
    include_installed_packages_version: bool,
    no_afterword: bool,
    dry: Literal[True]
) -> None:
    ...


@overload
def push(
    *,
    message: str,
    main_file_path: str,
    model_directory_relative_path: str,
    include_installed_packages_version: bool,
    no_afterword: bool,
    dry: Literal[False],
) -> Submission:
    ...


def push(
    *,
    message: str,
    main_file_path: str,
    model_directory_relative_path: str,
    include_installed_packages_version: bool,
    no_afterword: bool,
    dry: bool,
) -> Optional[Submission]:
    message_length = len(message)
    if message_length > SUBMISSION_MESSAGE_LENGTH:
        print(f"submission: message too long: {message_length}/{SUBMISSION_MESSAGE_LENGTH}", file=sys.stderr)
        raise click.Abort()

    submission_directory_path = os.path.abspath(".")

    client, project = Client.from_project()
    competition = project.competition

    preferred_chunk_size = 50_000_000

    keep_cached = not dry
    upload_cache: UploadCache = (
        FileUploadCache.load(submission_directory_path, client)
        if keep_cached
        else NoUploadCache()
    )

    code_files: Dict[str, Upload] = {}
    model_files: Dict[str, Upload] = {}

    try:
        _upload_files(
            group_name="code",
            storage=code_files,
            found_files=list_code_files(submission_directory_path, model_directory_relative_path),
            dry=dry,
            client=client,
            preferred_chunk_size=preferred_chunk_size,
            requirements_mode=RequirementsMode.FREEZE if include_installed_packages_version else RequirementsMode.INCLUDE,
            upload_cache=upload_cache,
        )

        _upload_files(
            group_name="model",
            storage=model_files,
            found_files=list_model_files(submission_directory_path, model_directory_relative_path),
            dry=dry,
            client=client,
            preferred_chunk_size=preferred_chunk_size,
            requirements_mode=RequirementsMode.IGNORE,
            upload_cache=upload_cache,
        )

        if dry:
            print("dry run, not uploading files")
            return None

        print(f"export {competition.name}:project/{project.user_id}/{project.name}")
        submission = project.submissions.create(
            message=message,
            main_file_path=main_file_path,
            model_directory_path=model_directory_relative_path,
            type=SubmissionType.CODE,
            code_files=_to_upload_ids(code_files),
            model_files=_to_upload_ids(model_files),
        )

        if not no_afterword:
            _print_success(client, submission)

        return submission
    finally:
        upload_cache.persist()

        _cleanup(client, code_files, keep_cached)
        _cleanup(client, model_files, keep_cached)


def _to_upload_ids(uploads: Optional[Dict[str, Upload]]) -> Dict[str, str]:
    if uploads is None:
        return {}

    return {
        path: upload.id
        for path, upload in uploads.items()
    }


def _cleanup(
    client: Client,
    files: Dict[str, Upload],
    keep_cached: bool,
):
    upload_ids_to_delete = [
        upload.id
        for upload in files.values()
        if not keep_cached or upload.status != UploadStatus.SUCCEEDED
    ]

    client.uploads.batch_delete(upload_ids_to_delete)


def _print_success(
    client: Client,
    submission: Submission,
):
    print("\n---")
    print(f"submission #{submission.number} succesfully uploaded!")
    print()

    project = submission.project
    competition = project.competition

    url = client.format_web_url(f"/competitions/{competition.name}/projects/{project.user_id}/{project.name}/submissions/{submission.number}")
    print(f"find it on your dashboard: {url}")
