from typing import Any, Literal, Sequence

from crunch import store
from crunch.api import Metric, Unit
from crunch.command._common import get_project
from crunch.repository import Repository
from crunch.utils import ascii_table

__all__ = [
    "leaderboard",
    "leaderboard_positions",
]

_Position = dict[str, Any]
_RowKind = Literal["regular", "team", "member"]


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
        print("tips:")
        print("  - It takes a little while for leaderboards to update after a run is finished.")
        print("  - To see the other positions, use `crunch leaderboard --top 10`, `--around 3` or `--all`.")


def leaderboard_positions(
    repository: Repository,
    *,
    top: int | None,
    around: int | None,
    include_unrewarded: bool,
):
    """The positions as the web leaderboard shows them: ranked by reward rank, with the teams grouped if the competition only rewards their leader."""

    project = get_project(repository)
    competition = project.competition
    leaderboard = competition.leaderboards.default

    layout = leaderboard.layout or {}
    phase = leaderboard.phase or {}
    only_rewarded = bool(phase.get("showOnlyRewardedProjectsDefault")) and not include_unrewarded
    show_best = phase.get("type") != "OUT_OF_SAMPLE" and not layout.get("hideBest")
    show_projects = bool(phase.get("showProjectNames"))
    show_teams = not competition.only_team_leader and not layout.get("hideTeam") and phase.get("showProjectNames") is not False

    def is_own(position: _Position) -> bool:
        return _nested(position, "user", "id") == project.user_id and _nested(position, "project", "name") == project.name

    targets = leaderboard.targets or []
    if not len(targets):
        print("leaderboard: no target")
        return

    for index, target in enumerate(targets):
        if index != 0:
            print()

        print(f"target: {target['displayName']!r}")

        groups = _group_teams(target.get("positions") or [], competition.only_team_leader)
        if only_rewarded:
            # Hidden by default on the web: a rank, but no reward rank.
            groups = [
                (head, members)
                for head, members in groups
                if not (head.get("rank") and not head.get("rewardRank"))
            ]

        if not len(groups):
            print("  (nobody is ranked yet)")
            continue

        own_index = next((
            group_index
            for group_index, (head, members) in enumerate(groups)
            if any(map(is_own, [head, *members])
        )), None)
        if around is not None and own_index is None:
            if top is None:
                print("  (your project is not on this leaderboard: use --top or --all to see positions)")
                continue
            print("  (your project is not on this leaderboard: --around is ignored)")

        target_metrics: list[dict[str, Any]] = target.get("metrics") or []
        metrics = [Metric.from_dict(metric) for metric in target_metrics]
        best_unit = next((metric.unit for metric in metrics if metric.weight != 0), None)
        headers = ["", "rank", "user"] + ["project"] * show_projects + ["team"] * show_teams + ["best"] * show_best
        headers += [f"{'last ' if show_best else ''}{metric.display_name or metric.name}" for metric in metrics] + ["remarks"]

        def line(position: _Position, kind: _RowKind) -> list[str]:
            team = _nested(position, "team", "name")
            position_metrics: list[dict[str, Any]] = position.get("metrics") or []
            scores = {score.get("metricId"): score.get("score") for score in position_metrics}

            values = [
                "*" if kind != "team" and is_own(position) else "",
                _format_rank(position.get("rewardRank")) if kind != "member" else "",
                _format_name(position, kind),
            ]

            values += [str(_nested(position, "project", "name") or "") if kind != "team" else ""] * show_projects
            values += [team or "(solo)"] * show_teams

            # A team row has the best of its leader (nothing without one), no score and no remark of its own.
            values += [_format_value(position.get("best"), best_unit) if kind != "team" or position.get("teamLeader") else ""] * show_best
            values += [_format_value(scores.get(metric.id), metric.unit) if kind != "team" else "" for metric in metrics]
            values.append(_remarks(position) if kind != "team" else "")

            return values

        # `...` where positions are left out: before, between and after the ones shown.
        skipped = ["", "..."] + [""] * (len(headers) - 2)

        rows: list[Sequence[Any]] = []
        previous_index = -1
        for group_index in _select(len(groups), top=top, around=around, own_index=own_index):
            if group_index != previous_index + 1:
                rows.append(skipped)

            previous_index = group_index

            head, members = groups[group_index]
            rows.append(line(head, "team" if members else "regular"))
            rows.extend(line(member, "member") for member in members)

        if previous_index != len(groups) - 1:
            rows.append(skipped)

        ascii_table(headers=headers, values=rows)

    if store.show_tips:
        print()
        print("tips:")
        print("  - Your project is marked with `*`, and the rank is the reward rank, as on the web leaderboard.")
        print("  - Combine `--top <n>` and `--around <n>`, or show every position with `--all`.")

        if only_rewarded:
            print("  - The projects that are not rewarded are hidden, as on the web leaderboard: show them with `--include-unrewarded`.")


def _group_teams(
    positions: list[_Position],
    only_team_leader: bool,
) -> list[tuple[_Position, list[_Position]]]:
    """
    Each position with no members, or, when only team leaders are rewarded, each team as its leader's position (or
    its first member's, without a leader) with its members: the leader first, then by rank. As the web leaderboard does.
    """

    if not only_team_leader:
        return [(position, []) for position in positions]

    teams: dict[str, list[_Position]] = {}
    for position in positions:
        team = _nested(position, "team", "name")
        if team:
            teams.setdefault(team, []).append(position)

    groups: list[tuple[_Position, list[_Position]]] = []
    for position in positions:
        team = _nested(position, "team", "name")
        if not team:
            groups.append((position, []))
            continue

        members = teams[team]
        head = next((member for member in members if member.get("teamLeader")), members[0])
        if position is head:
            groups.append((
                head,
                sorted(members, key=lambda member: (not member.get("teamLeader"), member.get("rank") or 0))
            ))

    return groups


def _select(
    count: int,
    *,
    top: int | None,
    around: int | None,
    own_index: int | None,
) -> list[int]:
    """The indexes to show: the first `top` and the `around` ones on each side of the own one, or every one."""

    if top is None and around is None:
        return list(range(count))

    selected = set(range(min(top or 0, count)))
    if around is not None and own_index is not None:
        selected.update(range(
            max(0, own_index - around),
            min(count, own_index + around + 1)
        ))

    return sorted(selected)


def _nested(position: _Position, key: str, field: str) -> Any:
    """A field of an object of the position (`user`, `project`, `team`): None when the object is missing."""

    value: dict[str, Any] = position.get(key) or {}
    return value.get(field)


def _format_name(position: _Position, kind: _RowKind) -> str:
    if kind == "team":
        return f"{_nested(position, 'team', 'name')} (team)"

    login = str(_nested(position, "user", "login") or "")
    if kind == "member":
        return f"  {login}{' (leader)' if position.get('teamLeader') else ''}"

    return login


def _remarks(position: _Position) -> str:
    disqualification: dict[str, Any] = position.get("disqualification") or {}

    remarks = [
        f"disqualified: {disqualification.get('badge') or 'yes'}" if disqualification else "",
        "duplicate" if position.get("duplicate") else "",
        "out of range" if position.get("outOfRange") else "",
        "not deterministic" if position.get("deterministic") is False else "",
    ]

    return ", ".join(remark for remark in remarks if remark)


def _format_rank(value: float | None) -> str:
    if value is None:
        return ""

    return str(int(value)) if float(value).is_integer() else str(value)


def _format_value(value: float | None, unit: Unit | None) -> str:
    if value is None:
        return ""

    return unit.format_value(value) if unit is not None else str(value)
