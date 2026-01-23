import json
import random
import traceback
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence, Tuple, cast

import click

from crunch.api import ApiException, Competition, PhaseType, SubmissionType
from crunch.constants import DEFAULT_MODEL_DIRECTORY
from crunch.utils import exit_via

if TYPE_CHECKING:
    from . import CodeLoader, ModuleFileName


def _load_code(context: click.Context, file_name: "ModuleFileName") -> Tuple[Competition, "CodeLoader"]:
    from . import CodeLoader

    competition, load_code = cast(
        Tuple[
            Competition,
            Callable[["ModuleFileName"], CodeLoader],
        ],
        context.obj
    )

    loader = load_code(file_name)
    print(f"organizer: loaded {file_name} code from {loader.location}")

    return competition, loader


@click.group(name="test")
@click.pass_context
def organize_test_group(
    context: click.Context,
):
    from . import deduce_code_loader

    competition: Competition = context.obj

    def load_code(file_name: "ModuleFileName") -> "CodeLoader":
        return deduce_code_loader(
            competition_name=competition.name,
            file_name=file_name,
        )

    context.obj = (competition, load_code)


@organize_test_group.group(name="leaderboard")
def leaderboard_group():
    pass


@leaderboard_group.command(name="rank")
@click.option("--scores-file", "score_file_path", type=click.Path(exists=True, dir_okay=False))
@click.option("--rank-pass", "rank_pass_string", type=click.Choice(["PRE_DUPLICATE", "FINAL"]), default="FINAL")
@click.option("--target-name", required=False, default=None)
@click.option("--shuffle", is_flag=True)
@click.pass_context
def leaderboard_rank(
    context: click.Context,
    score_file_path: str,
    rank_pass_string: str,
    target_name: Optional[str],
    shuffle: bool,
):
    from crunch.unstructured import LeaderboardModule, RankableProject, RankPass

    competition, loader = _load_code(context, "leaderboard")

    module = LeaderboardModule.load(loader)
    if module is None:
        print(f"no custom leaderboard script found")
        raise click.Abort()

    rank_pass = RankPass[rank_pass_string]

    with open(score_file_path, "r") as fd:
        root = json.load(fd)
        if not isinstance(root, list):
            raise ValueError("root must be a list")

        projects: List[RankableProject] = []
        for index, item in enumerate(root):  # type: ignore
            if not isinstance(item, dict):
                raise ValueError(f"root[{index}] must be a dict: {item}")

            projects.append(RankableProject.from_dict(item))  # type: ignore

        if shuffle:
            random.shuffle(projects)

    if target_name is None:
        target = next(
            (
                target
                for target in competition.targets.list()
                if target.primary
            ),
            None
        )

        if target is None:
            raise ValueError("primary target not found?")
    else:
        target = competition.targets.get(target_name)

    metrics = target.metrics.list()

    try:
        ranked_projects = module.rank(
            target=target,
            metrics=metrics,
            projects=projects,
            rank_pass=rank_pass,
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
        _ascii_table(
            headers=(
                "Rank",
                "Reward Rank",
                "Project ID",
                *[
                    f"Metric: {metric_name_by_id[id]}"
                    for id in used_metric_ids
                ]
            ),
            values=[
                (
                    str(ranked_project.rank),
                    str(ranked_project.reward_rank),
                    str(ranked_project.id),
                    *(
                        str(score_by_metric_id_by_project_id[ranked_project.id].get(metric_id))
                        for metric_id in used_metric_ids
                    )
                )
                for ranked_project in ranked_projects
            ],
        )
    except ApiException as error:
        exit_via(error)
    except BaseException as error:
        print(f"\n\nLeaderboard rank function failed: {error}")

        traceback.print_exc()


@leaderboard_group.command(name="compare")
@click.option("--prediction-directory", "prediction_directory_paths", type=(int, click.Path(file_okay=False, readable=True)), multiple=True)
@click.option("--data-directory", "data_directory_path", type=click.Path(file_okay=False, readable=True), required=True)
@click.pass_context
def leaderboard_compare(
    context: click.Context,
    prediction_directory_paths: List[Tuple[int, str]],
    data_directory_path: str,
):
    from crunch.unstructured import LeaderboardModule

    competition, loader = _load_code(context, "leaderboard")

    module = LeaderboardModule.load(loader)
    if module is None:
        print(f"no custom leaderboard script found")
        raise click.Abort()

    prediction_directory_path_by_id: Dict[int, str] = {}
    for prediction_id, prediction_file_path in prediction_directory_paths:
        if prediction_id in prediction_directory_path_by_id:
            print(f"prediction id {prediction_id} specified multiple time")
            raise click.Abort()

        prediction_directory_path_by_id[prediction_id] = prediction_file_path

    try:
        targets = competition.targets.list()

        similarities = module.compare(
            targets=targets,
            prediction_directory_path_by_id=prediction_directory_path_by_id,
            data_directory_path=data_directory_path,
        )

        print(f"\n\nSimilarities have been compared")

        target_per_id = {
            target.id: target
            for target in targets
        }

        print(f"\nResults:")
        _ascii_table(
            headers=(
                "Target Name",
                "Left",
                "Right",
                "Similarity"
            ),
            values=[
                (
                    target_per_id[similarity.target_id].name,
                    str(similarity.left_id),
                    str(similarity.right_id),
                    str(similarity.value),
                )
                for similarity in similarities
            ],
        )
    except ApiException as error:
        exit_via(error)
    except BaseException as error:
        print(f"\n\nLeaderboard rank function failed: {error}")

        traceback.print_exc()


@organize_test_group.group(name="scoring")
def scoring_group():
    pass


PHASE_TYPE_NAMES = [
    PhaseType.SUBMISSION.name,
    PhaseType.OUT_OF_SAMPLE.name,
]


@scoring_group.command(name="check")
@click.option("--data-directory", "data_directory_path", type=click.Path(file_okay=False, readable=True), required=True)
@click.option("--prediction-directory", "prediction_directory_path", type=click.Path(file_okay=False, readable=True), required=True)
@click.option("--phase-type", "phase_type_string", type=click.Choice(PHASE_TYPE_NAMES), default=PHASE_TYPE_NAMES[0])
@click.option("--chain-height", "chain_height", type=int, required=False)
@click.pass_context
def scoring_check(
    context: click.Context,
    data_directory_path: str,
    prediction_directory_path: str,
    phase_type_string: str,
    chain_height: Optional[int],
):
    from crunch.unstructured import ParticipantVisibleError, ScoringModule

    competition, loader = _load_code(context, "scoring")

    module = ScoringModule.load(loader)
    if module is None:
        print(f"no custom scoring check found")
        raise click.Abort()

    phase_type = PhaseType[phase_type_string]

    if chain_height is None:
        chain_height = phase_type.first_chain_height()

    try:
        module.check(
            phase_type=phase_type,
            chain_height=chain_height,
            metrics=competition.metrics.list(),
            prediction_directory_path=prediction_directory_path,
            data_directory_path=data_directory_path,
        )

        print(f"\n\nPrediction is valid!")
    except ParticipantVisibleError as error:
        print(f"\n\nPrediction is not valid: {error}")
    except ApiException as error:
        exit_via(error)
    except BaseException as error:
        print(f"\n\nPrediction check function failed: {error}")

        traceback.print_exc()


@scoring_group.command(name="score")
@click.option("--data-directory", "data_directory_path", type=click.Path(file_okay=False, readable=True), required=True)
@click.option("--prediction-directory", "prediction_directory_path", type=click.Path(file_okay=False, readable=True), required=True)
@click.option("--phase-type", "phase_type_string", type=click.Choice(PHASE_TYPE_NAMES), default=PHASE_TYPE_NAMES[0])
@click.option("--chain-height", default=1)
@click.pass_context
def scoring_score(
    context: click.Context,
    data_directory_path: str,
    prediction_directory_path: str,
    phase_type_string: str,
    chain_height: int,
):
    from crunch.unstructured import ParticipantVisibleError, ScoringModule

    competition, loader = _load_code(context, "scoring")

    module = ScoringModule.load(loader)
    if module is None:
        print(f"no custom scoring score found")
        raise click.Abort()

    phase_type = PhaseType[phase_type_string]

    try:
        metrics = competition.metrics.list()
        results = module.score(
            phase_type=phase_type,
            chain_height=chain_height,
            metrics=metrics,
            prediction_directory_path=prediction_directory_path,
            data_directory_path=data_directory_path,
        )

        metric_by_id = {
            metric.id: metric
            for metric in metrics
        }

        print(f"\n\nPrediction is scorable!")

        print(f"\nResults:")
        _ascii_table(
            headers=(
                "Target",
                "Metric",
                "Score",
                "Details",
            ),
            values=[
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
    except ApiException as error:
        exit_via(error)
    except BaseException as error:
        print(f"\n\nPrediction score function failed: {error}")

        traceback.print_exc()


@organize_test_group.group(name="submission")
def submission_group():
    pass


SUBMISSION_TYPE_NAMES = [
    SubmissionType.CODE.name,
    SubmissionType.PREDICTION.name,
]


@submission_group.command(name="check")
@click.option("--submission-type", "submission_type_string", type=click.Choice(SUBMISSION_TYPE_NAMES), default=SUBMISSION_TYPE_NAMES[0])
@click.option("--root-directory", "root_directory_path", type=click.Path(exists=True, file_okay=False), required=True)
@click.option("--model-directory", "model_directory_path", type=click.Path(file_okay=False), default=DEFAULT_MODEL_DIRECTORY, help="Resources directory relative to root directory.")
@click.pass_context
def submission_check(
    context: click.Context,
    submission_type_string: str,
    root_directory_path: str,
    model_directory_path: str,
):
    from crunch.command.push import list_code_files, list_model_files
    from crunch.unstructured import File, ParticipantVisibleError, SubmissionModule

    _, loader = _load_code(context, "submission")

    module = SubmissionModule.load(loader)
    if module is None:
        print(f"no custom submission check found")
        raise click.Abort()

    submission_type = SubmissionType[submission_type_string]

    submission_files = [
        File.from_local(path, name)
        for path, name in list_code_files(root_directory_path, model_directory_path)
    ]

    model_files = [
        File.from_local(path, name)
        for path, name in list_model_files(root_directory_path, model_directory_path)
    ]

    try:
        module.check(
            submission_type=submission_type,
            submission_files=submission_files,
            model_files=model_files,
        )

        print(f"\n\nSubmission is valid!")
    except ParticipantVisibleError as error:
        print(f"\n\nSubmission is not valid: {error}")
    except ApiException as error:
        exit_via(error)
    except BaseException as error:
        print(f"\n\nSubmission check function failed: {error}")

        traceback.print_exc()


def _ascii_table(
    *,
    headers: Sequence[str],
    values: List[Sequence[Sequence[str]]],
):
    rows: List[Sequence[str]] = [
        list(map(str, row))
        for row in values
    ]

    rows.insert(0, headers)

    max_length_per_columns = [
        max((len(row[index]) for row in rows))
        for index in range(len(rows[0]))
    ]

    for row in rows:
        print("  ", end="")

        for column_index, value in enumerate(row):
            width = max_length_per_columns[column_index] + 3
            print(value.ljust(width), end="")

        print()
