import functools
import json
import os
import random
import traceback
import typing

import click
import pandas

from .. import __version__, api, constants, utils

if typing.TYPE_CHECKING:
    from . import CodeLoader, ModuleFileName


def _load_code(context: click.Context, file_name: "ModuleFileName") -> typing.Tuple[api.Competition, "CodeLoader"]:
    from . import CodeLoader, ModuleFileName

    competition, load_code = typing.cast(
        typing.Tuple[
            api.Competition,
            typing.Callable[[ModuleFileName], CodeLoader],
        ],
        context.obj
    )

    loader = load_code(file_name=file_name)
    print(f"organizer: loaded {file_name} code from {loader.location}")

    return competition, loader


@click.group(name="test")
@click.pass_context
def organize_test_group(
    context: click.Context,
):
    from . import deduce_code_loader

    competition: api.Competition = context.obj

    load_code = functools.partial(
        deduce_code_loader,
        competition_name=context.obj.name,
    )

    context.obj = (competition, load_code)


@organize_test_group.group(name="leaderboard")
def leaderboard_group():
    pass


@leaderboard_group.command(name="rank")
@click.option("--scores-file", "score_file_path", type=click.Path(exists=True, dir_okay=False))
@click.option("--rank-pass", type=click.Choice(["PRE_DUPLICATE", "FINAL"]), default="FINAL")
@click.option("--shuffle", is_flag=True)
@click.pass_context
def leaderboard_rank(
    context: click.Context,
    score_file_path: str,
    rank_pass: str,
    shuffle: bool,
):
    from . import LeaderboardModule, RankableProject, RankPass

    competition, loader = _load_code(context, "leaderboard")

    rank_pass = RankPass[rank_pass]

    module = LeaderboardModule.load(loader)
    if module is None:
        print(f"no custom leaderboard script found")
        raise click.Abort()

    with open(score_file_path, "r") as fd:
        root = json.load(fd)
        if not isinstance(root, list):
            raise ValueError("root must be a list")

        projects = [
            RankableProject.from_dict(item)
            for item in root
        ]

        if shuffle:
            random.shuffle(projects)

    try:
        metrics = competition.metrics.list()

        ranked_projects = module.rank(
            metrics,
            projects,
            rank_pass,
        )

        print(f"\n\nLeaderboard is ranked (pass: {rank_pass.name})")

        used_metric_ids = list({
            metric.id
            for project in projects
            for metric in project.metrics
        })

        metric_name_by_id = {
            metric.id: metric.name
            for metric in metrics
            if metric.id in used_metric_ids
        }

        score_by_metric_id_by_project_id = {
            project.id: {
                metric.id: metric.score
                for metric in project.metrics
            }
            for project in projects
        }

        print(f"\nResults:")
        utils.ascii_table(
            (
                "Rank",
                "Reward Rank",
                "Project ID",
                *[
                    f"Metric: {metric_name_by_id[id]}"
                    for id in used_metric_ids
                ]
            ),
            [
                (
                    ranked_project.rank,
                    ranked_project.reward_rank,
                    ranked_project.id,
                    *(
                        score_by_metric_id_by_project_id[ranked_project.id].get(metric_id)
                        for metric_id in used_metric_ids
                    )
                )
                for ranked_project in ranked_projects
            ]
        )
    except api.ApiException as error:
        utils.exit_via(error)
    except BaseException as error:
        print(f"\n\nLeaderboard rank function failed: {error}")

        traceback.print_exc()


@leaderboard_group.command(name="compare")
@click.option("--prediction-file", "prediction_file_paths", type=(int, click.Path(exists=True, dir_okay=False)), multiple=True)
@click.option("--data-directory", "data_directory_path", type=click.Path(file_okay=False, readable=True), required=True)
@click.pass_context
def leaderboard_compare(
    context: click.Context,
    prediction_file_paths: typing.List[typing.Tuple[int, str]],
    data_directory_path: str,
):
    from . import LeaderboardModule

    competition, loader = _load_code(context, "leaderboard")

    module = LeaderboardModule.load(loader)
    if module is None:
        print(f"no custom leaderboard script found")
        raise click.Abort()

    predictions = {}
    for prediction_id, prediction_file_path in prediction_file_paths:
        if prediction_id in predictions:
            print(f"prediction id {prediction_id} specified multiple time")
            raise click.Abort()

        predictions[prediction_id] = pandas.read_parquet(prediction_file_path)

    try:
        targets = competition.targets.list()

        similarities = module.compare(
            targets,
            predictions,
            data_directory_path,
        )

        print(f"\n\nSimilarities have been compared")

        target_per_id = {
            target.id: target
            for target in targets
        }

        prediction_name_per_id = {
            id: os.path.splitext(path)[0]
            for id, path in prediction_file_paths
        }

        print(f"\nResults:")
        utils.ascii_table(
            (
                "Target Name",
                "Left",
                "Right",
                "Similarity"
            ),
            [
                (
                    target_per_id[similarity.target_id].name,
                    prediction_name_per_id[similarity.left_id],
                    prediction_name_per_id[similarity.right_id],
                    similarity.value,
                )
                for similarity in similarities
            ]
        )
    except api.ApiException as error:
        utils.exit_via(error)
    except BaseException as error:
        print(f"\n\nLeaderboard rank function failed: {error}")

        traceback.print_exc()


@organize_test_group.group(name="scoring")
def scoring_group():
    pass


LOWER_PHASE_TYPES = list(map(lambda x: x.name, [
    api.PhaseType.SUBMISSION,
    api.PhaseType.OUT_OF_SAMPLE,
]))


@scoring_group.command(name="check")
@click.option("--data-directory", "data_directory_path", type=click.Path(file_okay=False, readable=True), required=True)
@click.option("--prediction-file", "prediction_file_path", type=click.Path(dir_okay=False, readable=True), required=True)
@click.option("--phase-type", "phase_type_string", type=click.Choice(LOWER_PHASE_TYPES), default=LOWER_PHASE_TYPES[0])
@click.pass_context
def scoring_check(
    context: click.Context,
    data_directory_path: str,
    prediction_file_path: str,
    phase_type_string: str,
):
    from . import ParticipantVisibleError, ScoringModule, scoring_check

    competition, loader = _load_code(context, "scoring")

    phase_type = api.PhaseType[phase_type_string]

    try:
        scoring_check(
            ScoringModule.load(loader),
            phase_type,
            competition.metrics.list(),
            utils.read(prediction_file_path),
            data_directory_path
        )

        print(f"\n\nPrediction is valid!")
    except ParticipantVisibleError as error:
        print(f"\n\nPrediction is not valid: {error}")
    except api.ApiException as error:
        utils.exit_via(error)
    except BaseException as error:
        print(f"\n\nPrediction check function failed: {error}")

        traceback.print_exc()


@scoring_group.command(name="score")
@click.option("--data-directory", "data_directory_path", type=click.Path(file_okay=False, readable=True), required=True)
@click.option("--prediction-file", "prediction_file_path", type=click.Path(dir_okay=False, readable=True), required=True)
@click.option("--phase-type", "phase_type_string", type=click.Choice(LOWER_PHASE_TYPES), default=LOWER_PHASE_TYPES[0])
@click.pass_context
def scoring_score(
    context: click.Context,
    data_directory_path: str,
    prediction_file_path: str,
    phase_type_string: str,
):
    from . import ParticipantVisibleError, ScoringModule, scoring_score

    competition, loader = _load_code(context, "scoring")

    phase_type = api.PhaseType[phase_type_string]

    try:
        metrics = competition.metrics.list()
        results = scoring_score(
            ScoringModule.load(loader),
            phase_type,
            metrics,
            utils.read(prediction_file_path),
            data_directory_path,
        )

        metric_by_id = {
            metric.id: metric
            for metric in metrics
        }

        print(f"\n\nPrediction is scorable!")

        print(f"\nResults:")
        utils.ascii_table(
            ("Target", "Metric", "Score", "Details"),
            [
                (
                    metric_by_id[metric_id].target.name,
                    metric_by_id[metric_id].name,
                    str(scored_metric.value),
                    " ".join((
                        f"{detail.key}={detail.value}"
                        for detail in scored_metric.details
                    ))
                )
                for metric_id, scored_metric in results.items()
            ]
        )
    except ParticipantVisibleError as error:
        print(f"\n\nPrediction is not scorable: {error}")
    except api.ApiException as error:
        utils.exit_via(error)
    except BaseException as error:
        print(f"\n\nPrediction score function failed: {error}")

        traceback.print_exc()


@organize_test_group.group(name="submission")
def submission_group():
    pass


@submission_group.command(name="check")
@click.option("--root-directory", "root_directory_path", type=click.Path(exists=True, file_okay=False), required=True)
@click.option("--model-directory", "model_directory_path", type=click.Path(file_okay=False), default=constants.DEFAULT_MODEL_DIRECTORY, help="Resources directory relative to root directory.")
@click.pass_context
def submission_check(
    context: click.Context,
    root_directory_path: str,
    model_directory_path: str,
):
    from ..command.push import list_code_files, list_model_files
    from . import (File, ParticipantVisibleError, SubmissionModule,
                   submission_check)

    _, loader = _load_code(context, "submission")

    module = SubmissionModule.load(loader)
    if module is None:
        print(f"no custom submission check found")
        raise click.Abort()

    def from_local(path: str, name: str):
        _, extension = os.path.splitext(path)
        can_load = extension in constants.TEXT_FILE_EXTENSIONS

        return File(
            name,
            uri=path if can_load else None,
            size=os.path.getsize(path),
        )

    submission_files = [
        from_local(path, name)
        for path, name in list_code_files(root_directory_path, model_directory_path)
    ]

    model_files = [
        from_local(path, name)
        for path, name in list_model_files(root_directory_path, model_directory_path)
    ]

    try:
        submission_check(
            SubmissionModule.load(loader),
            submission_files,
            model_files,
        )

        print(f"\n\nSubmission is valid!")
    except ParticipantVisibleError as error:
        print(f"\n\nSubmission is not valid: {error}")
    except api.ApiException as error:
        utils.exit_via(error)
    except BaseException as error:
        print(f"\n\nSubmission check function failed: {error}")

        traceback.print_exc()
