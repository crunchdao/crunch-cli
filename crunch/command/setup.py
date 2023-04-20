import click
import os
import tarfile
import io
import requests

from .. import constants

def setup(
    session: requests.Session,
    clone_token: str,
    submission_number: str,
    project_name: str,
    directory: str,
    model_directory: str,
):
    if os.path.exists(directory):
        print(f"{directory}: already exists")
        raise click.Abort()

    push_token = session.post(f"/v1/projects/{project_name}/tokens", json={
        "type": "PERMANENT",
        "cloneToken": clone_token
    }).json()

    dot_crunchdao_path = os.path.join(
        directory, constants.DOT_CRUNCHDAO_DIRECTORY)
    os.makedirs(dot_crunchdao_path)

    project_file_path = os.path.join(
        dot_crunchdao_path, constants.PROJECT_FILE)
    with open(project_file_path, "w") as fd:
        fd.write(project_name)

    token_file_path = os.path.join(dot_crunchdao_path, constants.TOKEN_FILE)
    with open(token_file_path, "w") as fd:
        fd.write(push_token['plain'])

    code_tar = io.BytesIO(
        session.get(f"/v1/projects/{project_name}/clone", params={
            "pushToken": push_token['plain'],
            "submissionNumber": submission_number,
        }).content
    )

    tar = tarfile.open(fileobj=code_tar)
    for member in tar.getmembers():
        path = os.path.join(directory, member.name)
        print(f"extract {path}")
        
        os.makedirs(os.path.dirname(path), exist_ok=True)

        fileobj = tar.extractfile(member)
        with open(path, "wb") as fd:
            fd.write(fileobj.read())
    
    path = os.path.join(directory, model_directory)
    os.makedirs(path, exist_ok=True)
