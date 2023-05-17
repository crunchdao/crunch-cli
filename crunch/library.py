import typing
import requirements
import ast
import logging
import requests

from . import utils


DOT = "."


def strip_packages(name: str):
    if name.startswith(DOT):
        return None  # just in case, but should not happen

    if DOT not in name:
        return name

    index = name.index(DOT)
    return name[:index]


def _convert_import(node: ast.AST):
    packages = set()

    if isinstance(node, ast.Import):
        for alias in node.names:
            name = strip_packages(alias.name)
            if name:
                packages.add(name)
    elif isinstance(node, ast.ImportFrom):
        name = strip_packages(node.module)
        if name:
            packages.add(name)

    return packages


def extract_from_requirements(file_path: str):
    with open(file_path, 'r') as fd:
        return {
            requirement.name
            for requirement in requirements.parse(fd)
        }


def extract_from_code_cells(cells: typing.List[typing.List[str]]):
    packages = set()

    for index, lines in enumerate(cells):
        try:
            source = utils.strip_python_special_lines(lines)

            tree = ast.parse(source)

            for node in tree.body:
                packages.update(_convert_import(node))
        except Exception as exception:
            print(f"ignoring cell #{index + 1}: {str(exception)}")

    return packages - _STANDARD_LIBRARIES


def extract_from_notebook_modules(module: typing.Any):
    cells = getattr(module, "In", [])
    cells_and_lines = [
        cell if isinstance(cell, list) else cell.split("\n")
        for cell in cells
    ]

    return extract_from_code_cells(cells_and_lines)


def find_forbidden(packages: typing.Set[str], session: requests.Session):
    libraries = session.get("/v1/libraries").json()

    whitelist = set()
    for library in libraries:
        whitelist.add(library["name"])
        whitelist.update(library["aliases"])

    return packages - whitelist


def scan(session: requests.Session, module: typing.Any = None, requirements_file: str = None):
    packages = set()

    if module:
        packages = extract_from_notebook_modules(module)
    elif requirements_file:
        packages = extract_from_requirements(requirements_file)
    
    forbidden = find_forbidden(packages, session)

    for package in forbidden:
        logging.error('forbidden library: %s', package)

    if not len(forbidden):
        logging.warn('no forbidden library found')

_STANDARD_LIBRARIES = set([
    "ensurepip",
    "turtle",
    "xdrlib",
    "sunau",
    "operator",
    "zlib",
    "struct",
    "locale",
    "filecmp",
    "ftplib",
    "traceback",
    "pathlib",
    "wave",
    "compileall",
    "getopt",
    "mailcap",
    "plistlib",
    "types",
    "binascii",
    "concurrent",
    "tty",
    "imaplib",
    "dbm",
    "site",
    "test",
    "textwrap",
    "zoneinfo",
    "logging",
    "poplib",
    "cgitb",
    "cmath",
    "os",
    "argparse",
    "webbrowser",
    "shlex",
    "stat",
    "urllib",
    "optparse",
    "runpy",
    "tabnanny",
    "chunk",
    "bdb",
    "codeop",
    "netrc",
    "marshal",
    "tokenize",
    "enum",
    "readline",
    "modulefinder",
    "signal",
    "pydoc",
    "ctypes",
    "sched",
    "getpass",
    "abc",
    "difflib",
    "glob",
    "subprocess",
    "datetime",
    "numbers",
    "socket",
    "pty",
    "colorsys",
    "pdb",
    "socketserver",
    "grp",
    "pipes",
    "spwd",
    "asyncio",
    "smtplib",
    "nntplib",
    "curses",
    "linecache",
    "msilib",
    "timeit",
    "typing",
    "crypt",
    "audioop",
    "email",
    "__main__",
    "msvcrt",
    "distutils",
    "sysconfig",
    "token",
    "shutil",
    "mailbox",
    "re",
    "quopri",
    "platform",
    "fcntl",
    "wsgiref",
    "keyword",
    "ast",
    "weakref",
    "fileinput",
    "time",
    "statistics",
    "pwd",
    "gc",
    "queue",
    "tomllib",
    "lzma",
    "heapq",
    "gzip",
    "gettext",
    "atexit",
    "aifc",
    "itertools",
    "builtins",
    "stringprep",
    "code",
    "unicodedata",
    "tempfile",
    "asyncore",
    "sys",
    "threading",
    "uuid",
    "xml",
    "fractions",
    "secrets",
    "copyreg",
    "inspect",
    "winsound",
    "sqlite3",
    "sndhdr",
    "dis",
    "pickle",
    "fnmatch",
    "array",
    "base64",
    "asynchat",
    "rlcompleter",
    "tkinter",
    "functools",
    "contextvars",
    "math",
    "decimal",
    "__future__",
    "unittest",
    "doctest",
    "codecs",
    "bz2",
    "trace",
    "cmd",
    "py_compile",
    "imp",
    "calendar",
    "graphlib",
    "_thread",
    "uu",
    "selectors",
    "mimetypes",
    "csv",
    "pkgutil",
    "ssl",
    "copy",
    "posix",
    "telnetlib",
    "string",
    "multiprocessing",
    "select",
    "pyclbr",
    "hmac",
    "random",
    "shelve",
    "pickletools",
    "html",
    "winreg",
    "termios",
    "cgi",
    "hashlib",
    "syslog",
    "importlib",
    "zipimport",
    "contextlib",
    "mmap",
    "symtable",
    "errno",
    "zipapp",
    "bisect",
    "reprlib",
    "ipaddress",
    "io",
    "ossaudiodev",
    "faulthandler",
    "json",
    "pprint",
    "imghdr",
    "venv",
    "warnings",
    "xmlrpc",
    "dataclasses",
    "collections",
    "zipfile",
    "http",
    "tracemalloc",
    "tarfile",
    "resource",
    "nis",
    "configparser",
    "smtpd",
])
