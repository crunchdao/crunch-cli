from crunch.api import Unit
from crunch.command._common import get_project


def leaderboard():
    project = get_project()
    competition = project.competition

    all_metrics = {
        metric.name: metric
        for metric in competition.metrics.list()
    }

    leaderboard = competition.leaderboards.default
    changes = leaderboard.get_changes(
        user_identifier=project.user_id,
        project_identifier=project.name,
    )

    targets = changes["targets"]  # pyright: ignore[reportUnknownVariableType, reportArgumentType, reportCallIssue]
    if not len(targets):  # pyright: ignore[reportUnknownArgumentType]
        print("leaderboard: model is not appearing on the leaderboard")
        return

    for index, target_changes in enumerate(targets):  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
        if index != 0:
            print()

        print(f"Target: {target_changes['displayName']}")

        rank_unit = Unit.from_dict(target_changes['rank']['unit'])  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue, reportUnknownVariableType, reportArgumentType, reportCallIssue]
        current_rank = target_changes['rewardRank']['current']  # pyright: ignore[reportUnknownVariableType]
        print(f"  Rank: {rank_unit.format_value(current_rank) if current_rank is not None else ('unranked')}")  # pyright: ignore[reportUnknownMemberType]

        for metric_name, metric_changes in target_changes['metrics'].items():  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
            metric = all_metrics.get(metric_name)  # pyright: ignore[reportUnknownArgumentType]

            metric_unit = Unit.from_dict(metric_changes['unit'])  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue, reportUnknownVariableType]
            current_value = metric_changes['current']  # pyright: ignore[reportUnknownVariableType]

            print(f"  {metric.display_name}: {metric_unit.format_value(current_value) if current_value is not None else ('unavailable')}")  # pyright: ignore[reportUnknownMemberType, reportOptionalMemberAccess]
