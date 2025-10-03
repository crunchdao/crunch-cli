import click


@click.group(name="dev")
def group():
    pass


@group.group()
def phala():
    pass


@phala.command()
@click.option('--port', default=5123, help='Port to run the fake server on.')
@click.option('--keys-storage', "storage_directory_path", default='phala-keys', help='Path to store key pairs.', type=click.Path(file_okay=False, dir_okay=True, writable=True))
def fake_server(
    port: int,
    storage_directory_path: str,
):
    from crunch.dev.phala import run_fake_server

    run_fake_server(
        port=port,
        storage_directory_path=storage_directory_path,
    )
