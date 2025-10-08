import os

from crunch.api import Client, Submission, SubmissionType


def push_prediction(
    message: str,
    file_path: str,
) -> Submission:
    client, project = Client.from_project()

    print()
    print(f"deprecation: submitting a prediction is not the way to go")
    print(f"deprecation: please take some time to try to convert it to the new expected format")
    print(f"deprecation: read about the new code interface: https://docs.crunchdao.com/competitions/code-interface")
    print()

    upload = client.uploads.send_from_file(
        path=file_path,
        name=os.path.basename(file_path),
        size=os.path.getsize(file_path),
    )

    try:
        submission = project.submissions.create(
            message=message,
            main_file_path="main.py",
            model_directory_path="resources",
            type=SubmissionType.PREDICTION,
            code_files={
                "prediction": upload.id,
            },
            model_files={},
        )

        _print_success(client, submission)

        return submission
    finally:
        upload.delete()


def _print_success(
    client: Client,
    submission: Submission
):
    print("\n---")
    print(f"prediction as submission #{submission.number} succesfully uploaded!")

    print()

    project = submission.project
    competition = project.competition

    url = client.format_web_url(f"/competitions/{competition.name}/projects/{project.user_id}/{project.name}/submissions/{submission.number}")
    print(f"find it on your dashboard: {url}")
