import json
import os
import sys
from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING, BinaryIO, Callable, Dict, Iterable, List, Literal, Optional, Tuple, overload

import click
import requests

from crunch import store
from crunch.api import ApiException, Client, ForbiddenLibraryException, Project, Submission, SubmissionType, Upload
from crunch.constants import COLAB_DETECTION_ENV_VAR, COLAB_IGNORED_CODE_FILES, ENCRYPTION_JSON, IGNORED_CODE_FILES, IGNORED_MODEL_FILES, SUBMISSION_MESSAGE_LENGTH
from crunch.utils import format_bytes

if TYPE_CHECKING:
    from crunch_encrypt.ecies import EphemeralPublicKeyPem, PublicKeyPem


@dataclass
class EncryptedFileInfo:

    name: str
    public_key_pem: "EphemeralPublicKeyPem"


@dataclass
class EncryptionInfo:

    id: str
    public_key_pem: "PublicKeyPem"
    certificate_chain: str

    def format_json(self, files: List[EncryptedFileInfo]) -> str:
        return json.dumps({
            "version": "1.0",
            "submission_id": self.id,
            "public_key": self.public_key_pem,
            "certificate_chain": self.certificate_chain,
            "files": [
                {
                    "name": file.name,
                    "pubkey": file.public_key_pem,
                }
                for file in files
            ],
        }, indent=4)


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
    from ..external import gitignorefile

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
):
    directory_path = _to_unix_path(directory_path)
    directory_path_prefix = (
        len(directory_path)
        + int(not directory_path.endswith("/"))  # add 1 if value not ends with a slash
    )

    is_ignored = _build_gitignore(directory_path, ignored_paths, use_parent_gitignore)

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

            yield absolute_path, relative_path


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
    file_iterator: Iterable[Tuple[str, str]],
    dry: bool,
    client: Client,
    preferred_chunk_size: int,
    encryption_info: Optional[EncryptionInfo],
    encrypted_files_storage: List[EncryptedFileInfo],
    freeze_requirements: bool,
):
    from crunch_convert import RequirementLanguage, requirements_txt

    total_size = 0

    def handle_bytes(
        data: bytes,
        name: str,
        encrypt_if_possible: bool = True,
        log_action: Optional[str] = None,
    ):
        handle(
            io=BytesIO(data),
            name=name,
            size=len(data),
            encrypt_if_possible=encrypt_if_possible,
            log_action=log_action,
        )

    def handle(
        io: BinaryIO,
        name: str,
        size: int,
        encrypt_if_possible: bool = True,
        log_action: Optional[str] = None,
    ):
        nonlocal total_size

        total_size += size

        if log_action:
            print(f"{log_action}: {name} ({format_bytes(size)})")
        else:
            print(f"found {group_name} file: {name} ({format_bytes(size)})")

        if dry:
            return

        if encrypt_if_possible and encryption_info:
            (
                upload,
                ephemeral_public_key_pem,
            ) = client.uploads.send_from_io(
                io=io,
                name=name,
                size=size,
                public_key_pem=encryption_info.public_key_pem,
                preferred_chunk_size=preferred_chunk_size,
                progress_bar=True,
            )

            encrypted_files_storage.append(EncryptedFileInfo(
                name=name,
                public_key_pem=ephemeral_public_key_pem,
            ))
        else:
            upload = client.uploads.send_from_io(
                io=io,
                name=name,
                size=size,
                public_key_pem=None,
                preferred_chunk_size=preferred_chunk_size,
                progress_bar=True,
            )

        storage[name] = upload

    def handle_requirements(
        *,
        path: str,
        language: RequirementLanguage,
        validate_locally: bool,
    ):
        with open(path, "r") as fd:
            original_requirements_file = fd.read()

        requirements = requirements_txt.parse_from_file(
            language=language,
            file_content=original_requirements_file,
        )

        whitelist = requirements_txt.CachedWhitelist(
            requirements_txt.CrunchHubWhitelist(
                api_base_url=store.api_base_url,
            )
        )

        if validate_locally:
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
                    packages=forbidden_names
                )

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

    original_requirements_txts = (
        RequirementLanguage.PYTHON.original_txt_file_name,
        RequirementLanguage.R.original_txt_file_name,
    )

    python_requirements_txt = RequirementLanguage.PYTHON.txt_file_name
    r_requirements_txt = RequirementLanguage.R.txt_file_name

    # TODO Should this even be a question? Always doing it locally would save on bandwidth.
    # The backend would validate it a second time, but that is still better.
    # Also, requirements files should be processed first.
    validate_requirements_locally = encryption_info is not None

    for path, name in file_iterator:
        if freeze_requirements:
            if name in original_requirements_txts:
                continue

            elif name == python_requirements_txt:
                handle_requirements(
                    path=path,
                    language=RequirementLanguage.PYTHON,
                    validate_locally=validate_requirements_locally,
                )

                continue

            elif name == r_requirements_txt:
                handle_requirements(
                    path=path,
                    language=RequirementLanguage.R,
                    validate_locally=validate_requirements_locally,
                )

                continue

        with open(path, "rb") as fd:
            size = os.fstat(fd.fileno()).st_size
            handle(fd, name, size)

    if dry:
        return

    if len(storage) and encryption_info:
        json_data = encryption_info.format_json(
            files=encrypted_files_storage,
        ).encode("utf-8")

        handle_bytes(
            data=json_data,
            name=ENCRYPTION_JSON,
            encrypt_if_possible=False,
            log_action=f"create {group_name} encryption file",
        )

    print(f"total {group_name} size: {format_bytes(total_size)}")


def _get_encryption_info(
    client: Client,
    project: Project,
) -> Optional[EncryptionInfo]:
    competition = project.competition
    if not competition.encrypt_submissions:
        return None

    configuration = client.webapp.configuration

    encryption_id = project.submissions.get_next_encryption_id()
    print(f"using encryption id: {encryption_id}")

    phala = requests.get(f"{configuration.phala.key_url}/keypair/{encryption_id}").json()

    return EncryptionInfo(
        id=encryption_id,
        public_key_pem=phala["public_key"],  # type: ignore
        certificate_chain=phala["certificate_chain"],  # type: ignore
    )


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

    encryption_info = _get_encryption_info(client, project)

    code_uploads: Dict[str, Upload] = {}
    encrypted_code_files: List[EncryptedFileInfo] = []

    model_uploads: Dict[str, Upload] = {}
    encrypted_model_files: List[EncryptedFileInfo] = []

    try:
        _upload_files(
            group_name="code",
            storage=code_uploads,
            file_iterator=list_code_files(submission_directory_path, model_directory_relative_path),
            dry=dry,
            client=client,
            preferred_chunk_size=preferred_chunk_size,
            encryption_info=encryption_info,
            encrypted_files_storage=encrypted_code_files,
            freeze_requirements=include_installed_packages_version,
        )

        _upload_files(
            group_name="model",
            storage=model_uploads,
            file_iterator=list_model_files(submission_directory_path, model_directory_relative_path),
            dry=dry,
            client=client,
            preferred_chunk_size=preferred_chunk_size,
            encryption_info=encryption_info,
            encrypted_files_storage=encrypted_model_files,
            freeze_requirements=False,
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
            code_files={
                path: upload.id
                for path, upload in code_uploads.items()
            },
            model_files={
                path: upload.id
                for path, upload in model_uploads.items()
            },
        )

        if not no_afterword:
            _print_success(client, submission)

        return submission
    finally:
        if not dry:
            _cleanup(code_uploads, model_uploads)


def _cleanup(
    code_uploads: dict[str, Upload],
    model_uploads: dict[str, Upload]
):
    for upload in code_uploads.values():
        try:
            upload.delete()
        except ApiException as exception:
            print(f"cleanup error {exception}")

    for upload in model_uploads.values():
        try:
            upload.delete()
        except ApiException as exception:
            print(f"cleanup error {exception}")


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
