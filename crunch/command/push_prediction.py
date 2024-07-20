from .. import api


def push_prediction(
    message: str,
    file_path: str,
):
    client, project = api.Client.from_project()

    print()
    print(f"deprecation: submitting a prediction is not the way to go")
    print(f"deprecation: please take some time to try to convert it to the new expected format")
    print(f"deprecation: read about the new code interface: https://docs.crunchdao.com/competitions/code-interface")
    print()

    with open(file_path, "rb") as fd:
        submission = project.submissions.create(
            message=message,
            main_file_path=None,
            model_directory_path=None,
            type=api.SubmissionType.PREDICTION,
            preferred_packages_version={},
            files=[
                ("codeFile", (file_path, fd))
            ]
        )

        _print_success(client, submission)

        return submission


def _print_success(
    client: api.Client,
    submission: api.Submission
):
    print("\n---")
    print(f"prediction as submission #{submission.number} succesfully uploaded!")

    print()

    project = submission.project
    competition = project.competition

    url = client.format_web_url(f"/competitions/{competition.name}/projects/{project.user_id}/{project.name}/submissions/{submission.number}")
    print(f"find it on your dashboard: {url}")
