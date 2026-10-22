from crunch import store
from crunch.command._common import get_project, reformat_datetime
from crunch.repository import Repository


def status(
    repository: Repository,
):
    project = get_project(repository)
    competition = project.competition

    from crunch.command.competition import do_competition_show

    do_competition_show(competition)

    print()
    print(f"project:")
    print(f"  name: {project.name}")
    print(f"  created at: {reformat_datetime(project.created_at)}")

    print()
    print(f"run parameters:")
    print(f"  can configure force first train: {'no' if competition.hide_force_first_train else 'yes'}")
    print(f"  can configure train frequency: {'no' if competition.hide_train_frequency else 'yes'}")

    if store.show_tips:
        print()
        print(f"tips:")
        print(f"  - To manage quickstarters, use `crunch quickstarter`.")
        print(f"  - To manage submissions, use `crunch submission`.")
        print(f"  - To manage runs, use `crunch run`.")
        print(f"  - To run a local test, use `crunch test`.")
