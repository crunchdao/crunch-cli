import dataclasses
import os
import re
import typing

import redbaron
import yaml

import requirements

_FAKE_PACKAGE_NAME = "version"

_IMPORT = (
    redbaron.ImportNode,
    redbaron.FromImportNode,
)

_CODE = (
    redbaron.DefNode,
    redbaron.ClassNode
)

_COMMENT = (
    redbaron.CommentNode,
    redbaron.StringNode,
    redbaron.EndlNode,
)

_DOT = "."
_KV_DIVIDER = "---"


JUPYTER_MAGIC_COMMAND_PATTERN = r"^\s*?(!|%)"


@dataclasses.dataclass()
class EmbedFile:
    path: str
    normalized_path: str
    content: str


@dataclasses.dataclass()
class Requirement:
    name: str
    extras: typing.Optional[typing.List[str]]
    specs: typing.Optional[typing.List[str]]


def strip_packages(name: str):
    if name.startswith(_DOT):
        return None  # just in case, but should not happen

    if _DOT not in name:
        return name

    index = name.index(_DOT)
    return name[:index]


class ConverterError(ValueError):
    pass


class NotebookCellParseError(ConverterError):

    def __init__(
        self,
        message: str,
        parser_error: str,
        cell_source: str,
        cell_index: int = None,
        cell_id: str = None,
    ) -> None:
        super().__init__(message)
        self.parser_error = parser_error
        self.cell_index = cell_index
        self.cell_id = cell_id
        self.cell_source = cell_source


class RequirementVersionParseError(ConverterError):

    def __init__(self, message) -> None:
        super().__init__(message)


class InconsistantLibraryVersionError(ConverterError):

    def __init__(
        self,
        message: str,
        package_name: str,
        old: typing.List[str],
        new: typing.List[str]
    ) -> None:
        super().__init__(message)
        self.package_name = package_name
        self.old = old
        self.new = new


def _cut_crlf(input: str):
    input = input.replace("\r", "")

    if input.endswith('\n'):
        input = input[:-1]

    return input


def _extract_import_version(log: typing.Callable[[str], None], node: redbaron.Node):
    next_node = node.next
    if not isinstance(next_node, redbaron.CommentNode):
        log(f"skip version: next node not comment")
        return None

    line = re.sub(r"\s*#", "", next_node.dumps()).strip()
    if not re.match(r"^[\[><=~]", line):
        log(f"skip version: line not matching: `{line}`")
        return None

    line = f"{_FAKE_PACKAGE_NAME} {line}"

    try:
        requirement = next(requirements.parse(line), None)
        if requirement is None:
            log(f"skip version: parse returned nothing: `{line}`")
            return None
    except Exception as error:
        raise RequirementVersionParseError(
            f"version cannot be parsed: {error}"
        ) from error

    if requirement.name != _FAKE_PACKAGE_NAME:
        # package has been modified somehow
        raise RequirementVersionParseError(
            f"name must be {_FAKE_PACKAGE_NAME} and not {requirement.name}"
        )

    return (
        list(requirement.extras),
        [
            f"{operator}{semver}"
            for operator, semver in requirement.specs
        ]
    )


def _convert_import(log: typing.Callable[[str], None], node: redbaron.Node):
    paths = None
    if isinstance(node, redbaron.ImportNode):
        paths = node.modules()
    elif isinstance(node, redbaron.FromImportNode):
        paths = node.full_path_modules()

    if paths is None:
        return [], None

    version = _extract_import_version(log, node)

    names = set()
    for path in paths:
        name = strip_packages(path)
        if name:
            names.add(name)

    return names, version


def _add_to_packages(log: typing.Callable[[str], None], packages: dict, node: redbaron.Node):
    package_names, new = _convert_import(log, node)

    for package_name in package_names:
        if new is not None:
            old = packages.get(package_name)

            if old is not None and old != new:
                raise InconsistantLibraryVersionError(
                    f"inconsistant requirements for the same package",
                    package_name,
                    old,
                    new
                )

        if package_name not in packages or new is not None:
            packages[package_name] = new

        if new is not None:
            log(f"found version: {package_name}: {new}")


def _extract_code_cell(
    cell_source: typing.List[str],
    log: typing.Callable[[str], None],
    module: typing.List[str],
    packages: typing.Dict[str, typing.Tuple[typing.List[str], typing.List[str]]],
):
    source = "\n".join(
        re.sub(JUPYTER_MAGIC_COMMAND_PATTERN, r"#\1", line)
        for line in cell_source
    )

    if not len(source):
        log(f"skip since empty")
        return

    try:
        tree = redbaron.RedBaron(source)
    except Exception as error:
        log(f"failed to parse: {error}")
        raise NotebookCellParseError(
            f"notebook code cell cannot be parsed",
            str(error),
            source,
        ) from error

    parts = []
    for node in tree:
        node: redbaron.Node

        node_str = node.dumps()
        if node_str.endswith("\n"):
            node_str = node_str[:-1]

        if isinstance(node, _IMPORT):
            _add_to_packages(log, packages, node)
        elif not isinstance(node, _CODE) and not isinstance(node, _COMMENT):
            node = redbaron.RedBaron("\n".join(
                f"#{line}"
                for line in node_str.split("\n")
            ))

        parts.append(node.dumps())

    if len(tree):
        log(f"used {len(tree)} node(s)")

        module.append("\n".join(parts))
        module.append("\n")
    else:
        log(f"skip since empty")


def _extract_markdown_cell(
    cell_source: typing.List[str],
    log: typing.Callable[[str], None],
    embed_files: typing.Dict[str, EmbedFile],
):
    if not len(cell_source):
        log(f"skip since empty")
        return

    def get_full_source():
        return "\n".join(cell_source)

    iterator = iter(cell_source)

    if next(iterator) != _KV_DIVIDER:
        return

    try:
        source = []
        valid = True

        for line in iterator:
            if not line.strip():
                valid = False
                break

            if line == _KV_DIVIDER:
                break

            source.append(line)
        else:
            valid = False

        if not valid:
            return

        source = "\n".join(source)

        configuration = yaml.safe_load(source)

        if not isinstance(configuration, dict):
            raise ValueError("root must be a dict")
    except Exception as error:
        log(f"failed to parse: {error}")
        raise NotebookCellParseError(
            f"notebook markdown cell cannot be parsed",
            str(error),
            source,
        ) from error

    file_path = configuration.get("file")
    if file_path is None:
        raise NotebookCellParseError(
            f"file not specified",
            None,
            get_full_source(),
        )

    normalized_file_path = os.path.normpath(file_path).replace("\\", "/")
    lower_file_path = normalized_file_path.lower()

    previous = embed_files.get(lower_file_path)
    if previous is not None:
        raise NotebookCellParseError(
            f"file `{file_path}` specified multiple time",
            f"file `{file_path}` is conflicting with `{previous.path}`",
            get_full_source(),
        )

    content = "\n".join(iterator).strip()

    embed_files[lower_file_path] = EmbedFile(
        file_path,
        normalized_file_path,
        content,
    )

    log(f"embed {lower_file_path}: {len(content)} characters")


def extract_cells(
    cells: typing.List[typing.Any],
    print: typing.Callable[[str], None] = print,
) -> typing.Tuple[
    str,
    typing.List[EmbedFile],
    typing.List[Requirement],
]:
    packages: typing.Dict[str, typing.Tuple[typing.List[str], typing.List[str]]] = {}
    module: typing.List[str] = []
    embed_files: typing.Dict[str, EmbedFile] = {}

    for index, cell in enumerate(cells):
        cell_id = cell["metadata"].get("id") or f"cell_{index}"

        def log(message):
            print(f"convert {cell_id}: {message}")

        cell_source = cell["source"]
        if isinstance(cell_source, str):
            cell_source = cell_source.split("\n")

        cell_source = [
            _cut_crlf(line)
            for line in cell_source
        ]

        try:
            cell_type = cell["cell_type"]
            if cell_type == "code":
                _extract_code_cell(cell_source, log, module, packages)
            elif cell_type == "markdown":
                _extract_markdown_cell(cell_source, log, embed_files)
            else:
                log(f"skip since unknown type: {cell_type}")
                continue
        except NotebookCellParseError as error:
            error.cell_index = index
            error.cell_id = cell_id
            raise

    source_code = "\n".join(module)
    requirements = [
        Requirement(
            name,
            requirement[0] if requirement is not None else None,
            requirement[1] if requirement is not None else None,
        )
        for name, requirement in packages.items()
    ]

    return (
        source_code,
        list(embed_files.values()),
        requirements,
    )
