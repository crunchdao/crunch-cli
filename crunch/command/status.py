from crunch.command._common import get_project, reformat_datetime


def status(
    *,
    show_tips: bool = True,
):
    project = get_project()
    competition = project.competition

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

    if competition.team_based:
        print(f"  teaming up: possible, up to {competition.max_team_size} members", "but only the leader can be ranked" if competition.only_team_leader else "")
    else:
        print(f"  teaming up: not possible, you can only participate in solo")

    print(f"  number of models allowed: {competition.project_creation_limit}")

    print()
    print(f"project:")
    print(f"  name: {project.name}")
    print(f"  created at: {reformat_datetime(project.created_at)}")

    if show_tips:
        print()
        print(f"tips:")
        print(f"  - To manage quickstarters, use `crunch quickstarter`.")
        print(f"  - To manage submissions, use `crunch submission`.")
        print(f"  - To manage runs, use `crunch run`.")
        print(f"  - To run a local test, use `crunch test`.")
