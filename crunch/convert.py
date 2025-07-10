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

_FAKE_PACKAGE_NAME = "x__fake_package_name__"
_PACKAGE_NAME_PATTERN = r"[a-zA-Z_][a-zA-Z0-9_-]*[a-zA-Z0-9]"
_LAST_VERSION = "@latest"

_DOT = "."
_KV_DIVIDER = "---"

_CRUNCH_KEEP_ON = "@crunch/keep:on"
_CRUNCH_KEEP_OFF = "@crunch/keep:off"


JUPYTER_MAGIC_COMMAND_PATTERN = r"^(\s*?)(!|%|pip3? )"


@dataclasses.dataclass()
class EmbedFile:
    path: str
    normalized_path: str
    content: str


@dataclasses.dataclass()
class ImportedRequirement:
    alias: str
    name: typing.Optional[str] = None
    extras: typing.Optional[typing.List[str]] = None
    specs: typing.Optional[typing.List[str]] = None

    @property
    def extras_and_specs(self) -> typing.List[str]:
        return (self.extras, self.specs)

    def merge(self, other: "ImportedRequirement") -> bool:
        """
        Merge requirements:
        - if name is missing or the same, then use other's name
        - if (extras and specs) are empty or the same, then use other's (extras and specs)

        Alias is ignored.
        """

        errors = []

        different_name = self.name is not None and other.name is not None and self.name != other.name
        if different_name:
            errors.append("name")

        different_extras = len(self.extras) and len(other.extras) and self.extras != other.extras
        if different_extras:
            errors.append("extras")

        different_specs = len(self.specs) and len(other.specs) and self.specs != other.specs
        if different_specs:
            errors.append("specs")

        if len(errors):
            error_count = len(errors)

            if error_count == 1:
                field = errors[0]
                be = "is" if field == "name" else "are"
                message = f"{field} {be} different"
            elif error_count == 2:
                message = f"both {errors[0]} and {errors[1]} are different"
            elif error_count == 3:
                message = f"{errors[0]}, {errors[1]} and {errors[2]} are all different"

            return False, message

        if not different_name and other.name is not None:
            self.name = other.name

        if not different_extras and not different_specs and (len(other.extras) or len(other.specs)):
            self.extras = other.extras
            self.specs = other.specs

        return True, None


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
        old: typing.Tuple[typing.Optional[str], typing.List[str], typing.List[str]],
        new: typing.Tuple[typing.Optional[str], typing.List[str], typing.List[str]],
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
    return input.strip(string.whitespace + "#")


def _extract_import_version(log: typing.Callable[[str], None], comment_node: typing.Optional[libcst.Comment]):
    if comment_node is None:
        log(f"skip version: no comment")
        return None

    line = _strip_hashes(comment_node.value)
    if not line:
        log(f"skip version: comment empty")
        return None

    match = re.match(r"^(" + _PACKAGE_NAME_PATTERN + r")?\s*([@\[><=~])", line)
    if not match:
        log(f"skip version: line not matching: `{line}`")
        return None

    user_package_name = match.group(1)
    test_package_name = user_package_name or _FAKE_PACKAGE_NAME

    version_part = line[match.start(2):]
    if version_part == _LAST_VERSION:
        return (user_package_name or None, [], [])

    line = f"{test_package_name} {version_part}"

    try:
        requirement = next(requirements.parse(line), None)
        if requirement is None:
            log(f"skip version: parse returned nothing: `{line}`")
            return None
    except Exception as error:
        raise RequirementVersionParseError(
            f"version cannot be parsed: {error}"
        ) from error

    if requirement.name != test_package_name:
        # package has been modified somehow
        raise RequirementVersionParseError(
            f"name must be `{test_package_name}` and not `{requirement.name}`"
        )

    return (
        user_package_name or None,
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


def _convert_import(
    log: typing.Callable[[str], None],
    import_node: ImportNodeType,
    comment_node: typing.Optional[libcst.Comment]
) -> typing.List[ImportedRequirement]:
    if isinstance(import_node, libcst.Import):
        paths = [
            _evaluate_name(alias.name)
            for alias in import_node.names
        ]
    elif isinstance(import_node, libcst.ImportFrom) and import_node.module is not None:
        paths = [_evaluate_name(import_node.module)]
    else:
        return []

    package_name, extras, specs = _extract_import_version(log, comment_node) or (None, [], [])

    names = set()
    for path in paths:
        name = strip_packages(path)
        if name:
            names.add(name)

    return [
        ImportedRequirement(
            alias=name,
            name=package_name,
            extras=extras,
            specs=specs,
        )
        for name in names
    ]


_EMPTY_EXTRAS_AND_SPECS = ([], [])


def _add_to_packages(
    log: typing.Callable[[str], None],
    imported_requirements: typing.Dict[str, ImportedRequirement],
    import_node: ImportNodeType,
    comment_node: typing.Optional[libcst.Comment]
):
    new_requirements = _convert_import(log, import_node, comment_node)

    for new in new_requirements:
        package_name = new.alias

        if package_name in imported_requirements:
            current = imported_requirements[package_name]

            success, message = current.merge(new)
            if not success:
                raise InconsistantLibraryVersionError(
                    f"inconsistant requirements for the same package: {message}",
                    package_name,
                    (current.name, current.extras, current.specs),
                    (new.name, new.extras, new.specs),
                )
        else:
            imported_requirements[package_name] = new

        if new.extras_and_specs != _EMPTY_EXTRAS_AND_SPECS:
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


def _jupyter_replacer(match: typing.Match[str]) -> str:
    spaces = match.group(1)
    command = match.group(2)

    if len(spaces):
        return f"{spaces}pass  #{command}"

    return f"#{command}"


def _extract_code_cell(
    cell_source: typing.List[str],
    log: typing.Callable[[str], None],
    module: typing.List[str],
    imported_requirements: typing.Dict[str, ImportedRequirement],
):
    source = "\n".join(
        re.sub(JUPYTER_MAGIC_COMMAND_PATTERN, _jupyter_replacer, line)
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
        _add_to_packages(log, imported_requirements, import_node, comment_node)

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
    typing.List[ImportedRequirement],
]:
    imported_requirements: typing.Dict[str, ImportedRequirement] = {}
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
                _extract_code_cell(cell_source, log, module, imported_requirements)
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

    if validate:
        _validate(source_code)

    return (
        source_code,
        list(embed_files.values()),
        list(imported_requirements.values()),
    )
