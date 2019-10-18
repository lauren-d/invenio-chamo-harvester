# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 UCLouvain.
#
# Invenio-Chamo-Harvester is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""CLI for Chamo Harvester."""

from __future__ import absolute_import, print_function

import click
import yaml
from celery.messaging import establish_connection
from flask import current_app
from flask.cli import with_appcontext
from invenio_chamo_harvester.api import ChamoRecordHarvester
from invenio_chamo_harvester.tasks import (process_bulk_queue,
                                           queue_records_to_harvest)


def abort_if_false(ctx, param, value):
    """Abort command is value is False."""
    if not value:
        ctx.abort()


@click.group()
def chamo():
    """Fixtures management commands."""


@chamo.command("harvest")
@click.option('-s', '--size', type=int, default=1000)
@click.option('-n', '--next-id', type=int, default=0)
@click.option('-m', '--modified-since', default=None)
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('--yes-i-know', is_flag=True, callback=abort_if_false,
              expose_value=False,
              prompt='Do you really want to harvest all records?')
@with_appcontext
def harvest_chamo(size, next_id, modified_since, verbose):
    """Harvest all records."""
    click.secho('Sending records to harvesting queue ...', fg='green')
    try:
        count = queue_records_to_harvest(
            next_id=next_id,
            modified_since=modified_since,
            size=size)
        click.secho(
            'Records queued: {count}'.format(count=count),
            fg='blue'
        )
        click.secho('Execute "run" command to process the queue!',
                    fg='red')
    except Exception as e:
        click.secho(
            'Harvesting Error: {e}'.format(e=e),
            fg='red'
        )


@chamo.command("run")
@click.option('--delayed', '-d', is_flag=True,
              help='Run harvesting in background.')
@click.option('--concurrency', '-c', default=1, type=int,
              help='Number of concurrent harvesting tasks to start.')
@with_appcontext
def run(delayed, concurrency):
    """Run bulk record harvesting."""
    if delayed:
        celery_kwargs = {
            'kwargs': {
            }
        }
        click.secho(
            'Starting {0} tasks for harvesting records...'.format(concurrency),
            fg='green')
        for c in range(0, concurrency):
            process_bulk_queue.apply_async(**celery_kwargs)
    else:
        click.secho('Retrieve queued records...', fg='green')
        ChamoRecordHarvester(version_type=None).process_bulk_queue()


@chamo.group(chain=True)
def queue():
    """Manage harvester queue."""


@queue.resultcallback()
@with_appcontext
def process_actions(actions):
    """Process queue actions."""
    queue = current_app.config['CHAMO_HARVESTER_MQ_QUEUE']
    with establish_connection() as c:
        q = queue(c)
        for action in actions:
            q = action(q)


@queue.command('init')
def init_queue():
    """Initialize harvester queue."""
    def action(queue):
        queue.declare()
        click.secho('Harvester queue has been initialized.', fg='green')
        return queue
    return action


@queue.command('purge')
def purge_queue():
    """Purge indexing queue."""
    def action(queue):
        queue.purge()
        click.secho('Harvester queue has been purged.', fg='green')
        return queue
    return action


@queue.command('delete')
def delete_queue():
    """Delete indexing queue."""
    def action(queue):
        queue.delete()
        click.secho('Indexing queue has been deleted.', fg='green')
        return queue
    return action
