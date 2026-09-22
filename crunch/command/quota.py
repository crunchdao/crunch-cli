from datetime import timedelta

from crunch.command._common import get_project
from crunch.external.humanfriendly import format_size


def quota():
    project = get_project()

    submit = project.submit_quota
    compute = project.compute_quota

    print("")
    print("Submit Quota:")
    print(f"  Code Files: {format_size(submit.code_files.total_size, binary=True)}")
    print(f"  Model Files: {format_size(submit.model_files.total_size, binary=True)}")
    print(f"  Prediction Files: {format_size(submit.prediction_file.total_size, binary=True)}")
    print(f"  Submissions per day: {submit.submissions_per_day.current}/{submit.submissions_per_day.maximum}")

    print("")
    print("Compute Quota:")
    print(f"  Available: {timedelta(seconds=compute.available)}")
    print(f"  Remaining: {timedelta(seconds=compute.remaining)}")
    print(f"  Used: {timedelta(seconds=compute.used)}")
    print(f"  Running: {'A run is still running' if compute.running else 'No run is currently running, one can be created'}")
