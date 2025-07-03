import json
import os
import tempfile
import textwrap
import typing
import unittest

from parameterized import parameterized

from crunch.command.convert import convert
from crunch.convert import (EmbedFile, ImportedRequirement,
                            InconsistantLibraryVersionError,
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

    def test_keep_commands(self):
        (
            source_code,
            _,
            _,
        ) = extract_cells([
            _cell("a", "code", [
                "# @crunch/keep:on",
                "a = 42",
                "# @crunch/keep:off",
                "b = 42",
            ]),
            _cell("b", "code", [
                "# @crunch/keep:on",
                "c = 42",
            ]),
            _cell("b", "code", [
                "d = 42",
            ]),
        ])

        content = textwrap.dedent("""
            # @crunch/keep:on
            a = 42
            # @crunch/keep:off
            #b = 42


            # @crunch/keep:on
            c = 42


            #d = 42
        """).lstrip()

        self.assertEqual(content, source_code)

    def test_pip_escape(self):
        (
            source_code,
            _,
            _,
        ) = extract_cells([
            _cell("a", "code", [
                "pip install pandas",
                "pip3 install pandas",
            ]),
        ])

        content = textwrap.dedent("""
            #pip install pandas
            #pip3 install pandas
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

    @parameterized.expand([
        (
            """
            
            """,
            None,
        ),

        (
            """
            def foo(x):
                if x > 0:
                    return x
            """,
            None,
        ),

        (
            """
            class Foo:
                def bar(x):
                    if x > 0:
                        return x
            """,
            None,
        ),

        ("del foo", "#del foo\n", ),
        ("foo = 42", "#foo = 42\n",),
        ("foo += 42", "#foo += 42\n",),
        ("foo: int = 42", "#foo: int = 42\n",),

        (
            """
            for x in range(10):
                if x > 0:
                    print(x)
            """,
            """
            #for x in range(10):
            #    if x > 0:
            #        print(x)
            """,
        ),
        (
            """
            while True:
                if x > 0:
                    print(x)
            """,
            """
            #while True:
            #    if x > 0:
            #        print(x)
            """,
        ),
        (
            """
            if x > 0:
                print(x)
            """,
            """
            #if x > 0:
            #    print(x)
            """,
        ),
        (
            """
            with open("file.txt") as f:
                print(f.read())
            """,
            """
            #with open("file.txt") as f:
            #    print(f.read())
            """,
        ),

        (
            """
            match x:
                case 42:
                    print(x)
            """,
            """
            #match x:
            #    case 42:
            #        print(x)
            """,
        ),

        ("raise ValueError('x')", "#raise ValueError('x')\n",),
        (
            """
            try:
                pass
            except ValueError as e:
                print(e)
            """,
            """
            #try:
            #    pass
            #except ValueError as e:
            #    print(e)
            """,
        ),
        (
            """
            try:
                pass
            except* ValueError as e:
                print(e)
            """,
            """
            #try:
            #    pass
            #except* ValueError as e:
            #    print(e)
            """,
        ),
        ("assert False, 'oops'", "#assert False, 'oops'\n",),

        ("import a", "import a\n",),
        ("from a import b", "from a import b\n",),

        ("global x", "#global x\n",),
        ("nonlocal x", "#nonlocal x\n",),  # technically not correct
        ("pass", "#pass\n",),
        ("break", "#break\n",),  # technically not correct
        ("continue", "#continue\n",),  # technically not correct

        ("x & y", "#x & y\n",),
        ("x - y", "#x - y\n",),
        ("-x", "#-x\n",),
        ("lambda x: ...", "#lambda x: ...\n",),
        ("x if y else z", "#x if y else z\n",),
        ("{ 'x': 'y' }", "#{ 'x': 'y' }\n",),
        (
            """
            {
                'x': 'y'
            }
            """,
            """
            #{
            #    'x': 'y'
            #}
            """,
        ),
        ("{ 'x', 'y' }", "#{ 'x', 'y' }\n",),
        (
            """
            {
                'x',
                'y'
            }
            """,
            """
            #{
            #    'x',
            #    'y'
            #}
            """,
        ),
        ("[ x for x in range(42) if x > 0 ]", "#[ x for x in range(42) if x > 0 ]\n",),
        (
            """
            [
                x
                for x in range(42)
                if x > 0
            ]
            """,
            """
            #[
            #    x
            #    for x in range(42)
            #    if x > 0
            #]
            """,
        ),
        ("{ x for x in range(42) if x > 0 }", "#{ x for x in range(42) if x > 0 }\n",),
        (
            """
            {
                x
                for x in range(42)
                if x > 0
            }
            """,
            """
            #{
            #    x
            #    for x in range(42)
            #    if x > 0
            #}
            """,
        ),
        ("{ x: x * 2 for x in range(42) if x > 0 }", "#{ x: x * 2 for x in range(42) if x > 0 }\n",),
        (
            """
            {
                x: x * 2
                for x in range(42)
                if x > 0
            }
            """,
            """
            #{
            #    x: x * 2
            #    for x in range(42)
            #    if x > 0
            #}
            """,
        ),
        ("(x for x in range(42) if x > 0)", "#(x for x in range(42) if x > 0)\n",),
        (
            """
            (
                x
                for x in range(42)
                if x > 0
            )
            """,
            """
            #(
            #    x
            #    for x in range(42)
            #    if x > 0
            #)
            """,
        ),
        ("await x", "#await x\n",),  # technically not correct
        ("yield x", "#yield x\n",),  # technically not correct
        ("yield from x", "#yield from x\n",),  # technically not correct

        ("x > y", "#x > y\n",),
        ("x(y)", "#x(y)\n",),
        ("f'hello {world!s}'", "#f'hello {world!s}'\n",),
        ("'hello ' 'world'", "#'hello ' 'world'\n",),
        ("'hello '\n'world'", "#'hello '\n#'world'\n",),

        ("x.y", "#x.y\n",),
        ("x[y]", "#x[y]\n",),
        ("x, *y = z", "#x, *y = z\n",),
        ("x", "#x\n",),
        ("[ 'x', 'y' ]", "#[ 'x', 'y' ]\n",),
        (
            """
            [
                'x',
                'y'
            ]
            """,
            """
            #[
            #    'x',
            #    'y'
            #]
            """,
        ),
        ("( 'x', 'y' )", "#( 'x', 'y' )\n",),
        (
            """
            (
                'x',
                'y'
            )
            """,
            """
            #(
            #    'x',
            #    'y'
            #)
            """,
        ),

        ("x[y:z]", "#x[y:z]\n",),

        ("x and y", "#x and y\n",),
        ("x or y", "#x or y\n",),

        ("not x", "#not x\n",),

        ("x in y", "#x in y\n",),
        ("x not in y", "#x not in y\n",),

        ("import a as b", "import a as b\n",),

        (
            """
            print(x(1
                    + y))
            print()
            """,
            """
            #print(x(1
            #        + y))
            #print()
            """,
        ),
    ])
    def test_syntax(self, cell_content, expected):
        cell_content = textwrap.dedent(cell_content).lstrip()
        expected = textwrap.dedent(expected).lstrip() if expected else cell_content

        (
            source_code,
            _,
            _,
        ) = extract_cells([
            _cell("a", "code", cell_content.splitlines()),
        ])

        self.assertEqual(expected, source_code)


class ImportedRequirementTest(unittest.TestCase):

    def test_merge_nothing(self):
        a = ImportedRequirement("a", None, [], [])
        b = ImportedRequirement("b", None, [], [])

        success, _ = a.merge(b)

        self.assertTrue(success)
        self.assertEqual(("a", None, [], []), (a.alias, a.name, a.extras, a.specs))

    def test_merge_ignore_if_set_name(self):
        a = ImportedRequirement("a", "xyz", [], [])
        b = ImportedRequirement("b", None, [], [])

        success, _ = a.merge(b)

        self.assertTrue(success)
        self.assertEqual(("a", "xyz", [], []), (a.alias, a.name, a.extras, a.specs))

    def test_merge_ignore_if_set_extras(self):
        a = ImportedRequirement("a", None, ["tiny"], [])
        b = ImportedRequirement("b", None, [], [])

        success, _ = a.merge(b)

        self.assertTrue(success)
        self.assertEqual(("a", None, ["tiny"], []), (a.alias, a.name, a.extras, a.specs))

    def test_merge_ignore_if_set_full(self):
        a = ImportedRequirement("a", "xyz", ["tiny"], ["==1"])
        b = ImportedRequirement("b", None, [], [])

        success, _ = a.merge(b)

        self.assertTrue(success)
        self.assertEqual(("a", "xyz", ["tiny"], ["==1"]), (a.alias, a.name, a.extras, a.specs))

    def test_merge_ignore_if_set_full(self):
        a = ImportedRequirement("a", None, [], ["==1"])
        b = ImportedRequirement("b", None, [], [])

        success, _ = a.merge(b)

        self.assertTrue(success)
        self.assertEqual(("a", None, [], ["==1"]), (a.alias, a.name, a.extras, a.specs))

    def test_merge_name(self):
        a = ImportedRequirement("a", None, [], [])
        b = ImportedRequirement("b", "xyz", [], [])

        success, _ = a.merge(b)

        self.assertTrue(success)
        self.assertEqual(("a", "xyz", [], []), (a.alias, a.name, a.extras, a.specs))

    def test_merge_extras(self):
        a = ImportedRequirement("a", None, [], [])
        b = ImportedRequirement("b", None, ["full"], [])

        success, _ = a.merge(b)

        self.assertTrue(success)
        self.assertEqual(("a", None, ["full"], []), (a.alias, a.name, a.extras, a.specs))

    def test_merge_specs(self):
        a = ImportedRequirement("a", None, [], [])
        b = ImportedRequirement("b", None, [], ["==1"])

        success, _ = a.merge(b)

        self.assertTrue(success)
        self.assertEqual(("a", None, [], ["==1"]), (a.alias, a.name, a.extras, a.specs))

    def test_merge_specs_and_extras(self):
        a = ImportedRequirement("a", None, [], [])
        b = ImportedRequirement("b", None, ["full"], ["==1"])

        success, _ = a.merge(b)

        self.assertTrue(success)
        self.assertEqual(("a", None, ["full"], ["==1"]), (a.alias, a.name, a.extras, a.specs))

    def test_merge_different_name(self):
        a = ImportedRequirement("a", "abc", [], [])
        b = ImportedRequirement("b", "def", ["full"], ["==1"])

        success, message = a.merge(b)

        self.assertFalse(success)
        self.assertEqual(message, "name is different")

    def test_merge_different_extras(self):
        a = ImportedRequirement("a", None, ["tiny"], [])
        b = ImportedRequirement("b", None, ["full"], [])

        success, message = a.merge(b)

        self.assertFalse(success)
        self.assertEqual(message, "extras are different")

    def test_merge_different_specs(self):
        a = ImportedRequirement("a", None, [], ["==1"])
        b = ImportedRequirement("b", None, [], ["==2"])

        success, message = a.merge(b)

        self.assertFalse(success)
        self.assertEqual(message, "specs are different")

    def test_merge_different_extras_and_specs(self):
        a = ImportedRequirement("a", None, ["tiny"], ["==1"])
        b = ImportedRequirement("b", None, ["full"], ["==2"])

        success, message = a.merge(b)

        self.assertFalse(success)
        self.assertEqual(message, "both extras and specs are different")

    def test_merge_different_full(self):
        a = ImportedRequirement("a", "abc", ["tiny"], ["==1"])
        b = ImportedRequirement("b", "def", ["full"], ["==2"])

        success, message = a.merge(b)

        self.assertFalse(success)
        self.assertEqual(message, "name, extras and specs are all different")


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
                "import named # named-python == 42",
                "import extras # [big] >4.2",
            ])
        ])

        self.assertEqual([
            ImportedRequirement("hello", None, [], []),
            ImportedRequirement("world", None, [], ["==42"]),
            ImportedRequirement("named", "named-python", [], ["==42"]),
            ImportedRequirement("extras", None, ["big"], [">4.2"]),
        ], requirements)

    def test_latest_version(self):
        (
            _,
            _,
            requirements,
        ) = extract_cells([
            _cell("a", "code", [
                "import hello # @latest",
                "import world # pandas @latest",
            ])
        ])

        self.assertEqual([
            ImportedRequirement("hello", None, [], []),
            ImportedRequirement("world", "pandas", [], []),
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

    def test_one_specific_and_one_generic(self):
        (
            _,
            _,
            requirements,
        ) = extract_cells([
            _cell("a", "code", [
                "import hello # == 1",
                "import hello",
            ])
        ])

        self.assertEqual([
            ImportedRequirement("hello", None, [], ["==1"]),
        ], requirements)

        (
            _,
            _,
            requirements,
        ) = extract_cells([
            _cell("a", "code", [
                "import hello",
                "import hello # == 1",
            ])
        ])

        self.assertEqual([
            ImportedRequirement("hello", None, [], ["==1"]),
        ], requirements)

        (
            _,
            _,
            requirements,
        ) = extract_cells([
            _cell("a", "code", [
                "import hello",
                "import hello # == 1",
                "import hello",
            ])
        ])

        self.assertEqual([
            ImportedRequirement("hello", None, [], ["==1"]),
        ], requirements)

    def test_import_in_try_except(self):
        (
            source_code,
            _,
            _,
        ) = extract_cells([
            _cell("a", "code", [
                "try:",
                "    import hello",
                "except ImportError:",
                "    !pip install hello",
                "",
                "import hello",
            ])
        ])

        content = textwrap.dedent("""
            #try:
            #    import hello
            #except ImportError:
            #    pass  #!pip install hello

            import hello
        """).lstrip()

        self.assertEqual(content, source_code)

    def test_import_with_commented(self):
        (
            source_code,
            _,
            _,
        ) = extract_cells([
            _cell("a", "code", [
                "from pandas import DataFrame #, Series",
                "import pandas # Import important tools"
            ])
        ])

        content = textwrap.dedent("""
            from pandas import DataFrame #, Series
            import pandas # Import important tools
        """).lstrip()

        self.assertEqual(content, source_code)


class EmbedFilesTest(unittest.TestCase):

    def test_normal(self):
        (
            source_code,
            embed_files,
            requirements,
        ) = extract_cells([
            _cell("a", "markdown", [
                "---",
                "file: ./a.txt",
                "---",
                "",
                "# Hello World",
                "from a embed markdown file",
            ])
        ])

        self.assertEqual("", source_code)
        self.assertEqual([EmbedFile("./a.txt", "a.txt", "# Hello World\nfrom a embed markdown file")], embed_files)
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

    def test_separator(self):
        (
            source_code,
            embed_files,
            _,
        ) = extract_cells([
            _cell("a", "markdown", [
                "---",
                "<!-- content -->",
            ])
        ])

        self.assertEqual(0, len(embed_files))
        self.assertEqual("", source_code)

        (
            source_code,
            embed_files,
            _,
        ) = extract_cells([
            _cell("a", "markdown", [
                "---",
                "",  # empty line
                "unrelated",
                "---",
            ])
        ])

        self.assertEqual(0, len(embed_files))
        self.assertEqual("", source_code)


def get_notebook_paths():
    cloned_directory = os.getenv("CLONED_COMPETITIONS_REPOSITORY_PATH")
    if not cloned_directory:
        return []

    competitions_directory = os.path.join(cloned_directory, "competitions")
    for competition_name in os.listdir(competitions_directory):
        quickstarters_directory = os.path.join(competitions_directory, competition_name, "quickstarters")

        if not os.path.isdir(quickstarters_directory):
            continue

        for quickstarter_name in os.listdir(quickstarters_directory):
            quickstarter_directory = os.path.join(quickstarters_directory, quickstarter_name)

            manifest_file = os.path.join(quickstarter_directory, "quickstarter.json")
            if not os.path.exists(manifest_file):
                continue

            with open(manifest_file, "r") as file:
                manifest = json.loads(file.read())

            if not manifest.get("notebook") or manifest.get("language") != "PYTHON":
                continue

            entrypoint_file = os.path.join(quickstarter_directory, manifest["entrypoint"])
            if not os.path.exists(entrypoint_file):
                continue

            yield entrypoint_file


class FullNotebookTest(unittest.TestCase):

    @parameterized.expand(
        [
            (notebook_path,)
            for notebook_path in get_notebook_paths()
        ],
        skip_on_empty=True
    )
    def test_convert(self, notebook_path: str):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_file = os.path.join(temp_dir, "main.py")

            convert(
                notebook_path,
                main_file,
                override=True,
            )

            self.assertNotEquals(0, os.path.getsize(main_file), "main file is empty")
