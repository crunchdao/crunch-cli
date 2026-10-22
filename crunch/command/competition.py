from typing import TYPE_CHECKING

from crunch import store
from crunch.api import Client, Competition, CompetitionFormat, CompetitionMode, CompetitionStatus, WebUrlFormatter
from crunch.command._common import reformat_datetime

if TYPE_CHECKING:
    from crunch.api import CompetitionIdentifierType


def competition_list(
    *,
    format: CompetitionFormat | None = None,
    mode: CompetitionMode | None = None,
    status: CompetitionStatus | None = None,
    continuous: bool | None = None,
    external: bool | None = None,
    featured: bool | None = None,
    organizer_name: str | None = None,
    team_based: bool | None = None,
):
    client = Client.from_env()

    competitions = list(client.competitions.iter(
        format=format,
        status=status,
        mode=mode,
        continuous=continuous,
        external=external,
        featured=featured,
        organizer_name=organizer_name,
        team_based=team_based,
    ))

    if not competitions:
        print("list: no competitions found")
        return

    had_previous = False

    def print_list(title: str, statuses: set[CompetitionStatus]):
        filtered = [competition for competition in competitions if competition.status in statuses]
        if not filtered:
            return

        nonlocal had_previous
        if had_previous:
            print()
        had_previous = True

        print(f"{title} competitions:")
        for competition in filtered:
            category = "external" if competition.external else competition.format.name.lower()
            print(f"- {competition.name} ({category})")

    print_list("ongoing", {CompetitionStatus.OPEN, CompetitionStatus.LATE_SUBMISSIONS})
    print_list("upcoming", {CompetitionStatus.PENDING})
    print_list("closed", {CompetitionStatus.SCORING, CompetitionStatus.CLOSED})

    if store.show_tips:
        print()
        print("tips:")
        print(f"  - Show the details of a competition using `crunch competition show <competition-name>`.")


def competition_show(
    *,
    competition_identifier: "CompetitionIdentifierType",
):
    client = Client.from_env()
    competition = client.competitions.get(competition_identifier)

    do_competition_show(competition=competition)


def do_competition_show(
    competition: Competition,
):
    print(f"competition:")
    print(f"  name: {competition.name}")
    print(f"  display name: {competition.display_name!r}")
    print(f"  short description: {competition.short_description!r}")
    print(f"  start date: {reformat_datetime(competition.start)}")
    print(f"  end date: {reformat_datetime(competition.end)}")
    print(f"  competition documentation: {competition.documentation_url}")
    print(f"  notebook: {competition.notebook_url}")
    print(f"  competition sponsor: {competition.hosted_by_name}")
    print(f"  prize pool: {competition.prize_pool_short_text}")

    if competition.external:
        print(f"  external: yes")
        print(f"  platform url: {competition.url or '(not available)'}")
        return  # stop here, no more useful information afterward

    print(f"  number of models allowed: {competition.project_creation_limit}")

    if competition.team_based:
        print(f"  teaming up: possible, up to {competition.max_team_size} members", "but only the leader can be ranked" if competition.only_team_leader else "")
    else:
        print(f"  teaming up: not possible, you can only participate in solo")

    print(f"  external: no")

    web_url_formatter = WebUrlFormatter.from_env()
    print(f"  hub url: {web_url_formatter.competition(competition.name)}")
