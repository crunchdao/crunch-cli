import json
import random
import traceback
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence, Tuple, TypeVar, cast

import click

from crunch.api import ApiException, Competition, PhaseType, SubmissionType, Target
from crunch.constants import DEFAULT_MODEL_DIRECTORY
from crunch.utils import exit_via

if TYPE_CHECKING:
    from . import CodeLoader, ModuleFileName


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
@click.option("--scores-file", "score_file_path", type=click.Path(exists=True, dir_okay=False), required=False)
@click.option("--scores-from-api", is_flag=True, required=False)
@click.option("--rank-pass", "rank_pass_string", type=click.Choice(["PRE_DUPLICATE", "FINAL"]), default="FINAL")
@click.option("--target-name", required=False, default=None)
@click.option("--shuffle", is_flag=True)
@click.pass_context
def leaderboard_rank(
    context: click.Context,
    score_file_path: Optional[str],
    scores_from_api: bool,
    rank_pass_string: str,
    target_name: Optional[str],
    shuffle: bool,
):
    from crunch.unstructured import LeaderboardModule, RankableProject, RankPass

    rank_pass = RankPass[rank_pass_string]

    competition, module = _load_code(context, "leaderboard", LeaderboardModule.load)

    target, metrics = _find_target(competition, target_name)

    projects: List[RankableProject] = (
        _load_projects_from_api(competition, target, RankableProject.from_dict)  # type: ignore
        if scores_from_api else
        _load_projects_from_file(score_file_path, RankableProject.from_dict)  # type: ignore
    )

    if shuffle:
        random.shuffle(projects)

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
                    f"Metric\n{metric_name_by_id[id]}"
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

    competition, module = _load_code(context, "leaderboard", LeaderboardModule.load)

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


@organize_test_group.group(name="reward")
def reward_group():
    pass


@reward_group.command(name="compute-bounties")
@click.option("--scores-file", "score_file_path", type=click.Path(exists=True, dir_okay=False), required=False)
@click.option("--scores-from-api", is_flag=True, required=False)
@click.option("--target-name", required=False, default=None)
@click.option("--granted-amount", type=float, default=10_000.0)
@click.option("--shuffle", is_flag=True)
@click.pass_context
def reward_compute_bounties(
    context: click.Context,
    score_file_path: Optional[str],
    scores_from_api: bool,
    target_name: Optional[str],
    granted_amount: float,
    shuffle: bool,
):
    from crunch.unstructured import RewardableProject, RewardModule

    competition, module = _load_code(context, "reward", RewardModule.load)

    target, metrics = _find_target(competition, target_name)

    projects: List[RewardableProject] = (
        _load_projects_from_api(competition, target, RewardableProject.from_dict)  # type: ignore
        if scores_from_api else
        _load_projects_from_file(score_file_path, RewardableProject.from_dict)  # type: ignore
    )

    if shuffle:
        random.shuffle(projects)

    try:
        rewarded_projects = module.compute_bounties(
            target=target,
            metrics=metrics,
            projects=projects,
            granted_amount=granted_amount,
        )

        distributed_amount = sum(rewarded_project.amount for rewarded_project in rewarded_projects if rewarded_project.amount is not None)
        print(f"\n\nBounty rewards have been computed (distributed {distributed_amount:.4f} out of {granted_amount:.4f} granted)")

        print(f"\nResults:")
        _ascii_table(
            headers=(
                "Index",
                "Project ID",
                "Amount",
            ),
            values=[
                (
                    str(rank),
                    str(rewarded_project.id),
                    f"{rewarded_project.amount:10.4f}" if rewarded_project.amount is not None else "(none)",
                )
                for rank, rewarded_project in enumerate(rewarded_projects)
            ],
        )
    except ApiException as error:
        exit_via(error)
    except BaseException as error:
        print(f"\n\nBounty rewards compute function failed: {error}")

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

    competition, module = _load_code(context, "scoring", ScoringModule.load)

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

    phase_type = PhaseType[phase_type_string]

    competition, module = _load_code(context, "scoring", ScoringModule.load)

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

    submission_type = SubmissionType[submission_type_string]

    _, module = _load_code(context, "submission", SubmissionModule.load)

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


T = TypeVar("T")


def _load_code(context: click.Context, file_name: "ModuleFileName", module_loader: Callable[["CodeLoader"], Optional[T]]) -> Tuple[Competition, T]:
    from . import CodeLoader

    competition, load_code = cast(
        Tuple[
            Competition,
            Callable[["ModuleFileName"], CodeLoader],
        ],
        context.obj
    )

    loader = load_code(file_name)
    print(f"organizer: loading {file_name} code from {loader}")

    module = module_loader(loader)
    if module is None:
        print(f"organizer: no custom {file_name} script found")
        raise click.Abort()

    return competition, module


def _find_target(competition: Competition, name_candidate: Optional[str]):
    if name_candidate is None:
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
        target = competition.targets.get(name_candidate)

    metrics = target.metrics.list()

    return (
        target,
        metrics,
    )


def _load_projects_from_file(
    score_file_path: Optional[str],
    project_type: Callable[..., T],
) -> List[T]:
    if score_file_path is None:
        raise ValueError("score file path must be specified")

    with open(score_file_path, "r") as fd:
        root = json.load(fd)
        if not isinstance(root, list):
            raise ValueError("root must be a list")

        projects: List[T] = []
        for index, item in enumerate(root):  # type: ignore
            if not isinstance(item, dict):
                raise ValueError(f"root[{index}] must be a dict: {item}")

            projects.append(project_type.from_dict(item))  # type: ignore

    return projects


def _load_projects_from_api(
    competition: Competition,
    target: Target,
    mapper: Callable[..., T],
) -> List[T]:
    default_leaderboard = competition.leaderboards.default

    for target_leaderboard in default_leaderboard._attrs.get("targets") or []:  # type: ignore
        if target_leaderboard.get("id") != target.id:  # type: ignore
            continue

        positions = target_leaderboard.get("positions") or []  # type: ignore
        break

    else:
        print(f"leaderboard: target {target.name} not found, returning empty list")
        return []

    projects: List[T] = []
    for position in positions:  # type: ignore
        user = position["user"]  # type: ignore
        project = position["project"]  # type: ignore

        team = position.get("team")  # type: ignore
        group = f"team-{team.get('name')}" if team else f"user-{user['login']}"  # type: ignore

        item = {  # type: ignore
            "id": project["id"],
            "group": group,
            "rewardable": all([  # TODO Should be moved to backend instead
                position["duplicate"] is False,
                position["deterministic"] is not False,
                position["outOfRange"] is False,
                position["disqualification"] is None,
            ]),
            "metrics": [
                {
                    "id": metric["metricId"],
                    "score": metric["score"],
                }
                for metric in position["metrics"]  # type: ignore
            ],
        }

        projects.append(mapper(item))  # type: ignore

    return projects


def _ascii_table(
    *,
    headers: Sequence[str],
    values: List[Sequence[Sequence[str]]],
    spacing: int = 3,
):
    rows: List[Sequence[str]] = [
        list(map(str, row))
        for row in values
    ]

    header_liness: List[Sequence[str]] = [
        header.split("\n")
        for header in headers
    ]

    max_header_lines_count = max(len(header_lines) for header_lines in header_liness)
    for _ in range(max_header_lines_count):
        rows.insert(0, [""] * len(headers))

    for index, header_lines in enumerate(header_liness):
        for line_index, line in enumerate(header_lines):
            # Headers are lists, so they are indexable and mutable.
            rows[line_index][index] = line  # pyright: ignore[reportIndexIssue]

    max_length_per_columns = [
        max((len(row[index]) for row in rows))
        for index in range(len(rows[0]))
    ]

    separators = [
        "-" * (max_length_per_columns[index])
        for index in range(len(max_length_per_columns))
    ]
    rows.insert(max_header_lines_count, separators)

    for index, row in enumerate(rows):
        print("  ", end="")

        for column_index, value in enumerate(row):
            width = max_length_per_columns[column_index] + spacing
            print(value.ljust(width), end="")

        print()
