import ast
import dataclasses
import os
import py_compile
import re
import string
import typing

import libcst
import yaml

import requirements

_FAKE_PACKAGE_NAME = "version"

_DOT = "."
_KV_DIVIDER = "---"

_CRUNCH_KEEP_ON = "@crunch/keep:on"
_CRUNCH_KEEP_OFF = "@crunch/keep:off"


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


def _strip_hashes(input: str):
    return input.lstrip(string.whitespace + "#")


def _extract_import_version(log: typing.Callable[[str], None], comment_node: typing.Optional[libcst.Comment]):
    if comment_node is None:
        log(f"skip version: no comment")
        return None

    line = _strip_hashes(comment_node.value)
    if not line:
        log(f"skip version: comment empty")
        return None

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


ImportNodeType = typing.Union[libcst.Import, libcst.ImportFrom]


def _evaluate_name(node: libcst.CSTNode) -> str:
    if isinstance(node, libcst.Name):
        return node.value
    elif isinstance(node, libcst.Attribute):
        return f"{_evaluate_name(node.value)}.{node.attr.value}"
    else:
        raise Exception("Logic error!")


def _convert_import(log: typing.Callable[[str], None], import_node: ImportNodeType, comment_node: typing.Optional[libcst.Comment]):
    if isinstance(import_node, libcst.Import):
        paths = [
            _evaluate_name(alias.name)
            for alias in import_node.names
        ]
    elif isinstance(import_node, libcst.ImportFrom) and import_node.module is not None:
        paths = [_evaluate_name(import_node.module)]
    else:
        return [], None

    version = _extract_import_version(log, comment_node)

    names = set()
    for path in paths:
        name = strip_packages(path)
        if name:
            names.add(name)

    return names, version


def _add_to_packages(log: typing.Callable[[str], None], packages: dict, import_node: ImportNodeType, comment_node: typing.Optional[libcst.Comment]):
    package_names, new = _convert_import(log, import_node, comment_node)

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


_IMPORT = (
    libcst.Import,
    libcst.ImportFrom,
)

_KEEP = (
    libcst.Module,

    libcst.FunctionDef,
    libcst.ClassDef,

    libcst.Comment,
    libcst.EmptyLine,
    libcst.TrailingWhitespace,

    libcst.SimpleStatementLine,
)


class Comment(libcst.Comment):

    semicolon = False

    def _codegen_impl(self, state, default_semicolon=None) -> None:
        super()._codegen_impl(state)


class EmptyLine(libcst.EmptyLine):

    semicolon = False

    def _codegen_impl(self, state, default_semicolon=None) -> None:
        super()._codegen_impl(state)


class CommentTransformer(libcst.CSTTransformer):

    METHOD_GROUP = "group"
    METHOD_LINE = "line"

    def __init__(self, tree: libcst.Module):
        self.tree = tree

        self.import_and_comment_nodes: typing.List[typing.Tuple[ImportNodeType, typing.Optional[libcst.Comment]]] = []

        self._method_stack = []
        self._previous_import_node = None
        self._auto_comment = True

    def on_visit(self, node):
        # print("visit", type(node), "\\n".join(self._to_lines(node)))

        if isinstance(node, (libcst.Module, libcst.SimpleStatementLine)):
            self._method_stack.append(self.METHOD_GROUP)
            return True
        elif isinstance(node, libcst.BaseCompoundStatement):
            self._method_stack.append(self.METHOD_GROUP)
            return False
        else:
            self._method_stack.append(self.METHOD_LINE)
            return False

    def on_leave(self, original_node, updated_node):
        method = self._method_stack.pop()
        # print("leave", type(original_node), method)

        if isinstance(original_node, _IMPORT):
            self._previous_import_node = original_node
            return updated_node

        if self._previous_import_node is not None:
            import_node, self._previous_import_node = self._previous_import_node, None

            if isinstance(original_node, libcst.TrailingWhitespace) and original_node.comment:
                self.import_and_comment_nodes.append(
                    (import_node, original_node.comment)
                )
            else:
                self.import_and_comment_nodes.append(
                    (import_node, None)
                )

        if isinstance(original_node, libcst.EmptyLine) and original_node.comment:
            comment = _strip_hashes(original_node.comment.value)
            if comment == _CRUNCH_KEEP_ON:
                self._auto_comment = False
            elif comment == _CRUNCH_KEEP_OFF:
                self._auto_comment = True

            return updated_node

        if not self._auto_comment or isinstance(original_node, _KEEP):
            return updated_node

        nodes = []

        # control flow blocks have their comment attached to them
        if isinstance(original_node, libcst.BaseCompoundStatement) and original_node.leading_lines:
            nodes.extend(original_node.leading_lines)

            original_node = original_node.with_changes(
                leading_lines=libcst.FlattenSentinel([])
            )

        if method == self.METHOD_GROUP:
            nodes.extend(
                EmptyLine(comment=Comment(f"#{line}"))
                for line in self._to_lines(original_node)
            )

        elif method == self.METHOD_LINE:
            if isinstance(original_node, libcst.BaseSmallStatement):
                lines = self._to_lines(original_node)

                if len(lines) == 1:
                    nodes.append(Comment(f"#{lines[0]}"))
                else:
                    nodes.extend(
                        EmptyLine(comment=Comment(f"#{line}"))
                        for line in lines[:-1]
                    )
                    nodes.append(Comment(f"#{lines[-1]}"))
            else:
                nodes.extend(
                    Comment(f"#{line}")
                    for line in self._to_lines(original_node)
                )

        else:
            raise NotImplementedError(f"method: {method}")

        return libcst.FlattenSentinel(nodes)

    def _to_lines(self, node: libcst.CSTNode) -> str:
        return self.tree.code_for_node(node).splitlines()


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
        tree = libcst.parse_module(source)
    except libcst.ParserSyntaxError as error:
        log(f"failed to parse: {error.message}")

        raise NotebookCellParseError(
            f"notebook code cell cannot be parsed",
            str(error),
            source,
        ) from error

    transformer = CommentTransformer(tree)
    tree = tree.visit(transformer)

    for import_node, comment_node in transformer.import_and_comment_nodes:
        _add_to_packages(log, packages, import_node, comment_node)

    lines = tree.code.strip("\r\n").splitlines()
    if len(lines):
        log(f"used {len(lines)} line(s)")

        if len(module):
            module.append(f"\n")

        module.append("\n".join(lines))
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


def _validate(source_code: str):
    try:
        ast.parse(source_code)
    except SyntaxError as error:
        parser_error = py_compile.PyCompileError(
            error.__class__,
            error,
            "<converted_output>",
        )

        raise NotebookCellParseError(
            f"converted notebook code cell cannot be compiled",
            str(parser_error),
            source_code,
            -1,
            parser_error.file
        )


def extract_cells(
    cells: typing.List[typing.Any],
    print: typing.Callable[[str], None] = print,
    validate=True,
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

    module.append("")
    source_code = "\n".join(module)

    requirements = [
        Requirement(
            name,
            requirement[0] if requirement is not None else None,
            requirement[1] if requirement is not None else None,
        )
        for name, requirement in packages.items()
    ]

    if validate:
        _validate(source_code)

    return (
        source_code,
        list(embed_files.values()),
        requirements,
    )
