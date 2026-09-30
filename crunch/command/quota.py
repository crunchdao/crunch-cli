from datetime import timedelta

from crunch.command._common import get_project
from crunch.external.humanfriendly import format_size


def quota(
    *,
    show_tips: bool = False,
):
    project = get_project()

    submit = project.submit_quota
    compute = project.compute_quota

    print("submit quota:")
    print(f"  code files: {format_size(submit.code_files.total_size, binary=True)}")
    print(f"  model files: {format_size(submit.model_files.total_size, binary=True)}")
    print(f"  prediction files: {format_size(submit.prediction_file.total_size, binary=True)}")
    print(f"  submissions per day: {submit.submissions_per_day.current}/{submit.submissions_per_day.maximum}")

    print()
    print("compute quota:")
    print(f"  available: {timedelta(seconds=compute.available)}")
    print(f"  remaining: {timedelta(seconds=compute.remaining)}")
    print(f"  used: {timedelta(seconds=compute.used)}")
    print(f"  running: {'a run is still running' if compute.running else 'no run is currently running, one can be created'}")

    if show_tips:
        print()
        print("docs:")
        print(f"  - Size quotas refer to the total size of all files per submission or prediction.")
        print(f"  - A run is always given an additional 30 minutes, but it is not possible to start a run with zero remaining compute time.")
        print(f"  - The submission per day counter resets at midnight UTC.")
