from typing import List


def convert(
    notebook_file_path: str,
    python_file_path: str,
    *,
    override: bool = False,
    write_requirements: bool = False,
    write_embedded_files: bool = False,
    no_freeze: bool = False,
):
    from crunch_convert.cli import cli

    options: List[str] = []

    if override:
        options.append("--override")

    if write_requirements:
        options.append("--write-requirements")

    if write_embedded_files:
        options.append("--write-embedded-files")

    if no_freeze:
        options.append("--no-freeze")

    cli.main(args=[
        "notebook",
        notebook_file_path,
        python_file_path,
        *options,
    ])
