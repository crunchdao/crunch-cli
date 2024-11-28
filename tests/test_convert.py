import textwrap
import typing
import unittest

from crunch.convert import (InconsistantLibraryVersionError,
                            NotebookCellParseError,
                            RequirementVersionParseError, extract_cells)


def _cell(
    id: str,
    type: typing.Literal["markdown", "code"],
    source: typing.List[str],
):
    return {
        "metadata": {
            "id": id,
        },
        "cell_type": type,
        "source": source,
    }


class SourceCodeTest(unittest.TestCase):

    def test_normal(self):
        (
            source_code,
            _,
            _,
        ) = extract_cells([
            _cell("a", "code", [
                "# Hello World",
            ]),
            _cell("b", "code", [
                "a = 42",
                "def hello(x):",
                "    return x + 1",
            ]),
            _cell("c", "code", [
                "a += 1",
                "",
                "class Model:",
                "    pass",
            ])
        ])

        content = textwrap.dedent("""
            # Hello World
            
            
            #a = 42
            def hello(x):
                return x + 1
            
            
            
            #a += 1
            
            
            class Model:
                pass
            
            
        """).lstrip()

        self.assertEqual(content, source_code)

    def test_invalid_syntax(self):
        with self.assertRaises(NotebookCellParseError) as context:
            extract_cells([
                _cell("a", "code", [
                    "invalid code",
                ]),
            ])

        self.assertEqual("notebook code cell cannot be parsed", str(context.exception))
        self.assertIsNotNone(context.exception.parser_error)


class ImportTest(unittest.TestCase):

    def test_normal(self):
        (
            _,
            _,
            requirements,
        ) = extract_cells([
            _cell("a", "code", [
                "import hello",
                "import world # == 42",
                "import extras # [big] >4.2",
            ])
        ])

        self.assertEqual([
            {
                "name": "hello",
                "extras": None,
                "specs": None,
            },
            {
                "name": "world",
                "extras": [],
                "specs": ["==42"],
            },
            {
                "name": "extras",
                "extras": ["big"],
                "specs": [">4.2"],
            }
        ], requirements)

    def test_inconsistant_version(self):
        with self.assertRaises(InconsistantLibraryVersionError):
            extract_cells([
                _cell("a", "code", [
                    "import hello # == 1",
                    "import hello # == 2",
                ])
            ])

    def test_version_parse(self):
        with self.assertRaises(RequirementVersionParseError):
            extract_cells([
                _cell("a", "code", [
                    "import hello # == aaa",
                ])
            ])


class EmbedFilesTest(unittest.TestCase):

    def test_normal(self):
        (
            source_code,
            embed_files,
            requirements,
        ) = extract_cells([
            _cell("a", "markdown", [
                "---",
                "file: a",
                "---",
                "# Hello World",
                "from a embed markdown file",
            ])
        ])

        self.assertEqual("", source_code)
        self.assertEqual({
            "a": "# Hello World\nfrom a embed markdown file"
        }, embed_files)
        self.assertEqual([], requirements)

    def test_root_not_a_dict(self):
        with self.assertRaises(NotebookCellParseError) as context:
            extract_cells([
                _cell("a", "markdown", [
                    "---",
                    "- 42",
                    "---",
                    "# Hello World",
                ])
            ])

        self.assertEqual("notebook markdown cell cannot be parsed", str(context.exception))
        self.assertEqual("root must be a dict", context.exception.parser_error)

    def test_file_not_specified(self):
        with self.assertRaises(NotebookCellParseError) as context:
            extract_cells([
                _cell("a", "markdown", [
                    "---",
                    "file: readme.md",
                    "---",
                    "# Hello World",
                ]),
                _cell("b", "markdown", [
                    "---",
                    "file: readme.md",
                    "---",
                    "# Hello World",
                ])
            ])

        self.assertEqual("file `readme.md` specified multiple time", str(context.exception))
