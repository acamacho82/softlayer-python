"""Purge cached files from all edge nodes."""
# :license: MIT, see LICENSE for more details.

import click

import SoftLayer
from SoftLayer.CLI import environment, formatting


@click.command()
@click.argument('unique_id')
@click.argument('path')
@environment.pass_env
def cli(env, unique_id, path):
    """Creates a purge record and also initiates the purge call.

        Example:
             slcli cdn purge 9779455 /article/file.txt
    """

    manager = SoftLayer.CDNManager(env.client)
    result = manager.purge_content(unique_id, path)

    table = formatting.Table(['Date', 'Path', 'Saved', 'Status'])

    table.add_row([
        result['date'],
        result['path'],
        result['saved'],
        result['status']
    ])

    env.fout(table)
