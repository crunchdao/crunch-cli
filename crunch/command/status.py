from crunch.command._common import get_project, reformat_datetime


def status():
    project = get_project()
    competition = project.competition

    print(f"Competition Details:")
    print(f"  Name: {competition.display_name}")
    print(f"  Short description: {competition.short_description!r}")
    print(f"  Start Date: {reformat_datetime(competition.start)}")
    print(f"  End Date: {reformat_datetime(competition.end)}")
    print(f"  Competition Documentation: {competition.documentation_url}")
    print(f"  Notebook: {competition.notebook_url}")
    print(f"  Competition Sponsor: {competition.hosted_by_name}")
    print(f"  Prize Pool: {competition.prize_pool_short_text}")

    if competition.team_based:
        print(f"  Teaming up: possible, up to {competition.max_team_size} members", "but only the leader can be ranked" if competition.only_team_leader else "")
    else:
        print(f"  Teaming up: not possible, you can only participate in solo")

    print(f"  Number of models allowed: {competition.project_creation_limit}")

    print("")
    print(f"Project Details:")
    print(f"  Name: {project.name}")
    print(f"  Created At: {reformat_datetime(project.created_at)}")

    print("")
    print(f"Tips:")
    print(f"  Manage quickstarters via `crunch quickstarter`.")
    print(f"  Manage submissions via `crunch submission`.")
    print(f"  Manage runs via `crunch run`.")
    print(f"  Run a local test via `crunch test`.")
