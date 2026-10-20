from crunch import store
from crunch.api import Unit
from crunch.command._common import get_project
from crunch.repository import Repository


def leaderboard(
    repository: Repository,
):
    project = get_project(repository)
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

    targets = changes["targets"]
    if not len(targets):
        print("leaderboard: model is not appearing on the leaderboard")
        return

    for index, target_changes in enumerate(targets):
        if index != 0:
            print()

        print(f"target: {target_changes['displayName']!r}")

        rank_unit = Unit.from_dict(target_changes['rank']['unit'])
        current_rank = target_changes['rewardRank']['current']
        print(f"  rank: {rank_unit.format_value(current_rank) if current_rank is not None else ('unranked')}")

        for metric_name, metric_changes in target_changes['metrics'].items():
            metric = all_metrics.get(metric_name)
            display_name = metric.display_name if metric is not None else metric_name

            metric_unit = Unit.from_dict(metric_changes['unit'])
            current_value = metric_changes['current']

            print(f"  metric: {display_name!r}: {metric_unit.format_value(current_value) if current_value is not None else ('unavailable')}")

    if store.show_tips:
        print()
        print(f"docs:")
        print(f"  - It takes a little while for leaderboards to update after a run is finished.")
