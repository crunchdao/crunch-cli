import graphlib
from collections import defaultdict
from dataclasses import dataclass, field

from crunch_convert import RequirementLanguage
from crunch_convert.requirements_txt import RequirementParseError, parse_from_line


@dataclass
class InstallationBatch:
    lines: list[str]
    build_isolation: bool
    needed_by: dict[str, list[str]] = field(default_factory=dict[str, list[str]])
    """For the logs: the names of the libraries that need each one of this batch."""


def plan_installation(
    requirement_lines: list[str],
    build_dependencies: dict[str, list[str]],
) -> list[InstallationBatch]:
    """
    Order the requirements that need other libraries to be built.
    The first batch contains the build dependencies, and the next ones the libraries that need them, without build isolation so that they can see them.
    Everything else is not included and must be installed afterwards.

    Names are compared in lowercase, like the whitelist does.

    :raises graphlib.CycleError: if the build dependencies are cyclic
    """

    user_lines_by_name = _group_by_name(requirement_lines)

    dependency_lines_by_name = {
        dependent_name.lower(): _group_by_name(lines)
        for dependent_name, lines in build_dependencies.items()
    }

    pending = [
        name
        for name in user_lines_by_name
        if name in dependency_lines_by_name
    ]

    graph: dict[str, set[str]] = {}
    while pending:
        name = pending.pop()
        if name in graph:
            continue

        dependency_names = set(dependency_lines_by_name.get(name, {}))
        graph[name] = dependency_names
        pending.extend(dependency_names)

    if not graph:
        return []

    sorter = graphlib.TopologicalSorter(graph)
    sorter.prepare()

    batches: list[InstallationBatch] = []
    while sorter.is_active():
        names = sorted(sorter.get_ready())

        lines: list[str] = []
        needed_by: defaultdict[str, list[str]] = defaultdict(list)
        for name in names:
            lines.extend(user_lines_by_name.get(name, []))

            for dependent_name, dependency_lines in dependency_lines_by_name.items():
                if dependent_name in graph and name in dependency_lines:
                    lines.extend(dependency_lines[name])
                    needed_by[name].append(dependent_name)

        batches.append(InstallationBatch(
            lines=_deduplicate(lines) or names,
            build_isolation=not batches,
            needed_by={
                name: sorted(dependents)
                for name, dependents in needed_by.items()
            },
        ))

        sorter.done(*names)

    return batches


def _group_by_name(lines: list[str]) -> dict[str, list[str]]:
    lines_by_name: defaultdict[str, list[str]] = defaultdict(list)

    for line in lines:
        try:
            requirement = parse_from_line(
                language=RequirementLanguage.PYTHON,
                requirement_line=line,
            )
        except RequirementParseError:
            continue

        if requirement is None:
            continue

        lines_by_name[requirement.name.lower()].append(str(requirement))

    return dict(lines_by_name)


def _deduplicate(lines: list[str]) -> list[str]:
    return list(dict.fromkeys(lines))
