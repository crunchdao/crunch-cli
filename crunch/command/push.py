import json
import os
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import requests
from crunch_encrypt.ecies import (EphemeralPublicKeyPem, PublicKeyPem,
                                  generate_keypair_pem)

import crunch.constants as constants
import crunch.utils as utils
from crunch.api import ApiException, Client, Submission, SubmissionType, Upload


@dataclass
class EncryptedFileInfo:

    name: str
    public_key_pem: EphemeralPublicKeyPem


@dataclass
class EncryptionInfo:

    id: str
    public_key_pem: PublicKeyPem
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
    return os.path.normpath(input)\
        .replace("\\", "/")\
        .replace("//", "/")


def _build_gitignore(
    directory_path: str,
    ignored_paths: List[str],
    use_parent_gitignore: bool,
) -> Callable[[str], tuple[bool, bool]]:
    from ..external import gitignorefile

    ignored_files = gitignorefile._IgnoreRules(  # type: ignore
        rules=[
            gitignorefile._rule_from_pattern(line)  # type: ignore
            for line in ignored_paths
        ],
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
):
    total_size = 0

    for path, name in file_iterator:
        if name != "a":
            continue

        size = os.path.getsize(path)
        total_size += size

        print(f"found {group_name} file: {name} ({utils.format_bytes(size)})")
        if dry:
            continue

        if encryption_info:
            (
                upload,
                ephemeral_public_key_pem,
            ) = client.uploads.send_from_file_with_encryption(
                path=path,
                name=name,
                original_size=size,
                public_key_pem=encryption_info.public_key_pem,
                preferred_chunk_size=preferred_chunk_size,
                progress_bar=True
            )

            encrypted_files_storage.append(EncryptedFileInfo(
                name=name,
                public_key_pem=ephemeral_public_key_pem,
            ))
        else:
            upload = client.uploads.send_from_file(
                path=path,
                name=name,
                size=size,
                preferred_chunk_size=preferred_chunk_size,
                progress_bar=True
            )

        storage[name] = upload

    if not dry and len(storage) and encryption_info:
        name = "encryption.json"

        json_data = encryption_info.format_json(
            files=encrypted_files_storage,
        ).encode("utf-8")

        size = len(json_data)
        total_size += size

        print(f"create {group_name} encryption file: {name} ({utils.format_bytes(size)})")

        upload = client.uploads.send_from_bytes(
            data=json_data,
            name=name,
            preferred_chunk_size=preferred_chunk_size,
            progress_bar=True,
        )

        storage[name] = upload

    print(f"total {group_name} size: {utils.format_bytes(total_size)}")


def push(
    message: str,
    main_file_path: str,
    model_directory_relative_path: str,
    include_installed_packages_version: bool,
    dry: bool,
):
    submission_directory_path = os.path.abspath(".")

    client, project = Client.from_project()
    competition = project.competition

    preferred_chunk_size = 50_000_000
    encrypted = competition.encrypt_submissions

    encryption_info: Optional[EncryptionInfo] = None
    if encrypted:
        encryption_id = project.submissions.get_next_encryption_id()

        phala = requests.get(f"https://5bf6ff1b8e2002e6f0ca42465d1ef16abeabcd92-9010.dstack-pha-prod10.phala.network/keypair/{encryption_id}").json()

        encryption_info = EncryptionInfo(
            id=encryption_id,
            public_key_pem=phala["public_key"],  # type: ignore
            certificate_chain=phala["certificate_chain"],  # type: ignore
        )

        # private_key, public_key = generate_keypair_pem()
        # encryption_info = EncryptionInfo(
        #     id=encryption_id,
        #     public_key_pem=public_key,
        #     certificate_chain="",
        # )
        # open("private_key.pem", "w").write(private_key)

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
