import typing
import warnings

from crunch_convert.notebook import ConverterError as ConverterError
from crunch_convert.notebook import EmbeddedFile as EmbedFile
from crunch_convert.notebook import ImportedRequirement as ImportedRequirement
from crunch_convert.notebook import ImportedRequirementLanguage as ImportedRequirementLanguage
from crunch_convert.notebook import InconsistantLibraryVersionError as InconsistantLibraryVersionError
from crunch_convert.notebook import NotebookCellParseError as NotebookCellParseError
from crunch_convert.notebook import RequirementVersionParseError as RequirementVersionParseError
from crunch_convert.notebook import extract_from_cells


def extract_cells(
    cells: typing.List[typing.Any],
    print: typing.Optional[typing.Callable[[str], None]] = print,
    validate: bool = True,
) -> typing.Tuple[
    str,
    typing.List[EmbedFile],
    typing.List[ImportedRequirement],
]:
    warnings.warn("Use the `crunch_convert.notebook.extract_cells` function instead.", DeprecationWarning, stacklevel=2)

    flatten = extract_from_cells(
        cells,
        print=print,
        validate=validate,
    )

    return (
        flatten.source_code,
        flatten.embedded_files,
        flatten.requirements,
    )
