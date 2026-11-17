import graphlib
from collections import defaultdict
from dataclasses import dataclass

from crunch_convert import RequirementLanguage
from crunch_convert.requirements_txt import RequirementParseError, parse_from_line


@dataclass
class InstallationBatch:
    names: list[str]
    lines: list[str]
    build_isolation: bool
    needs: dict[str, list[str]]


def plan_installation(
    requirement_lines: list[str],
    build_dependencies: dict[str, list[str]],
) -> list[InstallationBatch]:
    """
    Order the requirements that need other libraries to be built, the others must be installed afterwards.
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
        for name in names:
            lines.extend(user_lines_by_name.get(name, []))

            for dependent_name, dependency_lines in dependency_lines_by_name.items():
                if dependent_name in graph and name in dependency_lines:
                    lines.extend(dependency_lines[name])

        batches.append(InstallationBatch(
            names=names,
            lines=list(dict.fromkeys(lines)),
            build_isolation=not batches,
            needs={
                name: sorted(graph[name])
                for name in names
                if graph[name]
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
