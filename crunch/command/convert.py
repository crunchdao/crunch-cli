def convert(
    notebook_file_path: str,
    python_file_path: str,
    override: bool = False,
):
    from crunch_convert.cli import cli

    cli.main(args=[
        "notebook",
        *(["--override"] if override else []),
        notebook_file_path,
        python_file_path,
    ])
