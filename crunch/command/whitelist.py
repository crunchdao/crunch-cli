import sys
from typing import TYPE_CHECKING, Any, Sequence

import click

from crunch import store
from crunch.api import Client, GpuRequirement
from crunch.command._common import get_project
from crunch.repository import Repository
from crunch.utils import ascii_table

if TYPE_CHECKING:
    from crunch_convert import RequirementLanguage


class ProgrammingLanguageClickType(click.ParamType):  # pyright: ignore[reportMissingTypeArgument]
    name = "language"

    def get_metavar(self, param: click.Parameter, ctx: click.Context | None = None) -> str:
        from crunch_convert import RequirementLanguage

        return f"[{'|'.join([language.name.lower() for language in RequirementLanguage])}]"

    def convert(self, value: Any, param: click.Parameter | None, ctx: click.Context | None) -> "RequirementLanguage | None":
        if value is None:
            return None

        from crunch_convert import RequirementLanguage

        try:
            return RequirementLanguage(value.upper())
        except ValueError:
            pass

        self.fail(
            f"'{value}' is not a valid programming language.",
            param,
            ctx
        )


GPU_REQUIREMENT_MAPPING: dict[str, GpuRequirement | None] = {
    "unknown": None,
    "useless": GpuRequirement.OPTIONAL,
    "useful": GpuRequirement.RECOMMENDED,
    "required": GpuRequirement.REQUIRED,
}

REVERSE_GPU_REQUIREMENT_MAPPING: dict[GpuRequirement | None, str] = {
    value: key
    for key, value in GPU_REQUIREMENT_MAPPING.items()
}


class GpuRequirementClickType(click.ParamType):  # pyright: ignore[reportMissingTypeArgument]
    name = "gpu"

    def get_metavar(self, param: click.Parameter, ctx: click.Context | None = None) -> str:
        return f"[{'|'.join(GPU_REQUIREMENT_MAPPING.keys())}]"

    def convert(self, value: Any, param: click.Parameter | None, ctx: click.Context | None) -> "GpuRequirement | None":
        if value in GPU_REQUIREMENT_MAPPING:
            return GPU_REQUIREMENT_MAPPING[value]

        self.fail(
            f"'{value}' is not a valid gpu requirement.",
            param,
            ctx
        )


def whitelist_search(
    *,
    query: str,
    language: "RequirementLanguage | None",
    limit: int | None,
):
    client = Client.from_env()

    libraries = list(client.libraries.list(
        standard=False,
        name=f"*{query}*".replace("*", "%"),
        language=language,
    ))

    if not libraries:
        print("search: no results found")
        return 1

    reached_limit = limit is not None and len(libraries) > limit
    if reached_limit:
        libraries = libraries[:limit]  # TODO do it at iteration

    rows: list[Sequence[Any]] = []
    for library in libraries:
        rows.append(
            (
                library.name,
                ", ".join(library.aliases) if library.aliases else "(none)",
                REVERSE_GPU_REQUIREMENT_MAPPING[library.gpu_requirement],
                library.language.name.lower(),
            )
        )

    print("libraries:")
    ascii_table(
        headers=["name", "alias(es)", "gpu", "lang."],
        values=rows,
    )

    if reached_limit:
        print()
        print(f"pagination: only displaying the first {limit} libraries, use `--limit <n>` to show more or `--all` to show all")

    if store.show_tips:
        print()
        print(f"tips:")
        print(f"  - You can request a package to be whitelisted by using `crunch whitelist request --name <name>`.")


def whitelist_validate(
    *,
    requirements_txt_file_path: str,
    language: "RequirementLanguage",
):
    from crunch_convert.requirements_txt import CrunchHubWhitelist, RequirementParseError, parse_from_file

    with open(requirements_txt_file_path, encoding="utf-8") as fd:
        content = fd.read()

    try:
        requirements = parse_from_file(
            language=language,
            file_content=content,
        )
    except RequirementParseError as exception:
        print(exception, file=sys.stderr)
        raise click.Abort()

    whitelist = CrunchHubWhitelist(
        api_base_url=store.api_base_url,
    )

    has_one_not_whitelisted = False

    for requirement in requirements:
        library = whitelist.find_library(name=requirement.name, language=language)

        tail = "ok"
        if library is None:
            tail = "not whitelisted"
            has_one_not_whitelisted = True

        print(f"requirement: {requirement.name}: {tail}")

    if store.show_tips:
        print("")
        print("tips:")

        if has_one_not_whitelisted:
            print("  - You can request to whitelist a package by doing `crunch whitelist request --name <name>`.")
            print("  - Only packages on PyPI can be whitelisted.")

        print("  - You can search for other whitelisted packages by doing `crunch whitelist search <query>` (with * as a wildcard).")

    if has_one_not_whitelisted:
        return 1


def whitelist_request(
    repository: Repository,
    *,
    name: str | None,
    alias: str | None,
    language: "RequirementLanguage | None",
    gpu_requirement: "GpuRequirement | None",
    additional_details: str | None,
):
    competition = get_project(repository).competition

    library_request = competition.library_requests.create(
        name=name,
        alias=alias,
        language=language,
        gpu_requirement=gpu_requirement,
        additional_details=additional_details,
    )

    print(f"request: created #{library_request.id} for {library_request.name or library_request.alias} ({library_request.verdict.name.lower()})")

    if store.show_tips:
        print()
        print(f"tips:")
        print(f"  - Library requests are reviewed by the Crunch team, the user will receive the outcome via email.")
