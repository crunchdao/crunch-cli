import types
from typing import Any
from unittest import mock

import pytest

import crunch.command.leaderboard as command
from crunch.command.leaderboard import _group_teams, _select  # pyright: ignore[reportPrivateUsage]

PERCENT = {"prefix": None, "suffix": "%", "scale": 2}
METRIC = {"id": 128, "name": "roc-auc", "displayName": "ROC AUC", "weight": 100, "score": True, "multiplier": 100.0, "unit": PERCENT}


def _position(rank: int, reward_rank: float | None, login: str, project: str, score: float, *, user_id: int = 0, team: str | None = None, leader: bool = False) -> dict[str, Any]:
    return {
        "rank": rank,
        "rewardRank": reward_rank,
        "best": score,
        "user": {"id": user_id, "login": login},
        "project": {"name": project, "writeupUrl": f"https://example.com/{login}"},
        "team": {"name": team} if team else None,
        "teamLeader": leader,
        "metrics": [{"metricId": 128, "score": score, "best": score}],
    }


def _render(capsys: pytest.CaptureFixture[str], positions: list[dict[str, Any]], *, phase_type: str = "OUT_OF_SAMPLE", only_team_leader: bool = False, only_rewarded: bool = False, **options: Any) -> list[str]:
    leaderboard = types.SimpleNamespace(
        targets=[{"displayName": "Prediction", "metrics": [METRIC], "positions": positions}],
        layout={"hideTeam": False, "hideBest": False},
        phase={"type": phase_type, "showProjectNames": True, "showOnlyRewardedProjectsDefault": only_rewarded},
    )
    competition = types.SimpleNamespace(only_team_leader=only_team_leader, leaderboards=types.SimpleNamespace(default=leaderboard))
    project = types.SimpleNamespace(user_id=7, name="mine", competition=competition)

    def get_project(repository: Any) -> Any:
        return project

    with mock.patch.object(command, "get_project", get_project), mock.patch.object(command.store, "show_tips", False):
        command.leaderboard_positions(None, top=options.get("top"), around=options.get("around"), include_unrewarded=False)  # pyright: ignore[reportArgumentType]

    return [line.split() for line in capsys.readouterr().out.splitlines()[1:]]  # pyright: ignore[reportReturnType]


def test_select():
    assert _select(4, top=None, around=None, own_index=2) == [0, 1, 2, 3]
    assert _select(20, top=3, around=2, own_index=10) == [0, 1, 2, 8, 9, 10, 11, 12]
    assert _select(20, top=2, around=3, own_index=None) == [0, 1]


def test_teams_are_grouped_under_their_leader_or_first_member():
    leader = _position(2, 2.0, "brandao", "a", 90.1, team="alphabot", leader=True)
    member = _position(1, None, "rafael", "b", 90.3, team="alphabot")
    first = _position(3, 3.0, "dave", "d", 90.0, team="nolead")
    second = _position(4, None, "erin", "e", 89.0, team="nolead")
    solo = _position(5, 4.0, "solo", "s", 88.0)

    groups = _group_teams([member, leader, first, second, solo], only_team_leader=True)

    assert groups == [(leader, [leader, member]), (first, [first, second]), (solo, [])]
    assert _group_teams([member, solo], only_team_leader=False) == [(member, []), (solo, [])]


def test_the_rank_is_the_reward_rank_with_the_team_column(capsys: pytest.CaptureFixture[str]):
    lines = _render(capsys, [_position(1, 1.0, "alice", "one", 90.13661), _position(2, None, "bob", "two", 89.9), _position(3, 2.0, "me", "mine", 71.42, user_id=7)])

    assert lines[0] == ["rank", "user", "project", "team", "ROC", "AUC", "remarks"]
    assert lines[2:] == [["1", "alice", "one", "(solo)", "90.14%"], ["bob", "two", "(solo)", "89.9%"], ["*", "2", "me", "mine", "(solo)", "71.42%"]]


def test_grouped_teams_with_around_and_remarks(capsys: pytest.CaptureFixture[str]):
    positions = [_position(rank, float(rank), f"user{rank}", f"p{rank}", 80.0 - rank) for rank in range(1, 6)]
    positions[4].update(duplicate=True)
    positions += [_position(6, 6.0, "leader", "l", 74.0, team="squad", leader=True), _position(7, None, "me", "mine", 73.0, user_id=7, team="squad")]

    lines = _render(capsys, positions, only_team_leader=True, top=1, around=1)

    assert lines[2:] == [
        ["1", "user1", "p1", "79.0%"],
        ["..."],
        ["5", "user5", "p5", "75.0%", "duplicate"],
        ["6", "squad", "(team)"],
        ["leader", "(leader)", "l", "74.0%"],
        ["*", "me", "mine", "73.0%"],
    ]


def test_best_outside_the_out_of_sample_phase_and_unrewarded_hidden(capsys: pytest.CaptureFixture[str]):
    lines = _render(capsys, [_position(1, 1.0, "alice", "one", 90.0), _position(2, None, "bob", "two", 89.9)], phase_type="SUBMISSION", only_rewarded=True)

    assert lines[0] == ["rank", "user", "project", "team", "best", "last", "ROC", "AUC", "remarks"]
    assert lines[2:] == [["1", "alice", "one", "(solo)", "90.0%", "90.0%"]]


def test_no_write_up_links(capsys: pytest.CaptureFixture[str]):
    _render(capsys, [_position(1, 1.0, "alice", "one", 90.0)])

    assert "example.com" not in capsys.readouterr().out


def test_skipped_positions_are_marked_before_and_after(capsys: pytest.CaptureFixture[str]):
    positions = [_position(rank, float(rank), f"user{rank}", f"p{rank}", 80.0 - rank) for rank in range(1, 8)]
    positions[3] = _position(4, 4.0, "me", "mine", 76.0, user_id=7)

    lines = _render(capsys, positions, around=1)

    assert [line[0] if len(line) == 1 else line[-4] for line in lines[2:]] == ["...", "user3", "me", "user5", "..."]


def test_around_without_own_position_explains_how_to_see_standings(capsys: pytest.CaptureFixture[str]):
    lines = _render(capsys, [_position(1, 1.0, "alice", "one", 90.0)], around=1)

    assert len(lines) == 1
    assert {"--top", "--all"}.issubset(lines[0])


def test_top_still_shows_positions_when_around_has_no_anchor(capsys: pytest.CaptureFixture[str]):
    positions = [_position(1, 1.0, "alice", "one", 90.0), _position(2, 2.0, "bob", "two", 80.0)]

    lines = _render(capsys, positions, around=1, top=1)

    assert any("alice" in line for line in lines)
    assert not any("bob" in line for line in lines)


def test_nobody_ranked_yet(capsys: pytest.CaptureFixture[str]):
    assert _render(capsys, []) == [["(nobody", "is", "ranked", "yet)"]]
