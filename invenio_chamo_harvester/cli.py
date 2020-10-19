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
import json
import os
import yaml
import ciso8601
from celery.messaging import establish_connection
from flask import current_app
from flask.cli import with_appcontext
from invenio_chamo_harvester.api import ChamoRecordHarvester, ChamoBibRecord
from invenio_chamo_harvester.tasks import (process_bulk_queue,
                                           queue_records_to_harvest,
                                           bulk_record)
from invenio_chamo_harvester.utils import get_max_record_pid
from invenio_jsonschemas import current_jsonschemas
from invenio_pidstore.models import PersistentIdentifier, PIDStatus,\
    RecordIdentifier
from invenio_records.api import Record
from rero_ils.modules.cli import fixtures
from rero_ils.modules.utils import get_record_class_from_schema_or_pid_type
from rero_ils.modules.documents.api import Document
from rero_ils.modules.documents.models import DocumentIdentifier
from sqlalchemy import func
from invenio_db import db


def abort_if_false(ctx, param, value):
    """Abort command is value is False."""
    if not value:
        ctx.abort()


@click.group()
def chamo():
    """Chamo harvester management commands."""


@click.group()
def export():
    """Fixtures export management commands."""


@export.command("records")
@click.option('-t', '--pid-type', 'pid_type',
              help='PID type of records to export.')
@click.option('-d', '--directory', 'directory', default='export_data/',
              help='Directory destination.')
@click.option('-v', '--verbose', is_flag=True, default=False)
@with_appcontext
def records_export(pid_type, directory,  verbose):
    """Export records."""
    record_class = get_record_class_from_schema_or_pid_type(pid_type=pid_type)
    if not record_class:
        raise AttributeError('Invalid pid type.')

    # prepare export directory
    if not os.path.exists(directory):
        os.makedirs(directory)

    records = []
    # get records from DB
    for recid in record_class.get_all_pids():
        if verbose:
            click.secho('process recid: {recid}'.format(
                recid=recid
            ), fg='green')
        records.append(record_class.get_record_by_pid(recid).dumps())

    # prepare export file
    filename = os.path.join(directory, '{name}.json'.format(
        name=record_class.provider.pid_type))

    with open(filename, 'w') as outfile:
        json.dump(records, outfile, indent=4)

    click.secho('{nb_records} records exported ({pid_type})'.format(
        nb_records=len(records),
        pid_type=pid_type
    ), fg='green')


# @export.command("records")
# @click.option('-t', '--pid-type', 'pid_type',
#               help='PID type of records to export.')
# @click.option('-d', '--directory', 'directory', default='export_data/',
#               help='Directory destination.')
# @click.option('-v', '--verbose', is_flag=True, default=False)
# @with_appcontext
# def records_export(pid_type, directory,  verbose):
#     """Export records."""
#     if not pid_type:
#         raise AttributeError('Invalid pid type.')
#
#     # prepare export directory
#     if not os.path.exists(directory):
#         os.makedirs(directory)
#
#     query = PersistentIdentifier.query.filter_by(
#         pid_type=pid_type
#     )
#     if not with_deleted:
#         query = query.filter_by(status=PIDStatus.REGISTERED)
#
#     records = []
#     # get records from DB
#     for identifier in query:
#         if verbose:
#             click.secho('process recid: {recid}'.format(
#                 recid=recid
#             ), fg='green')
#         records.append(record_class.get_record_by_pid(identifier.pid_value)
#                        .dumps())
#
#     # prepare export file
#     filename = os.path.join(directory, '{name}.json'.format(
#         name=record_class.provider.pid_type))
#
#     with open(filename, 'w') as outfile:
#         json.dump(records, outfile, indent=4)
#
#     click.secho('{nb_records} records exported ({pid_type})'.format(
#         nb_records=len(records),
#         pid_type=pid_type
#     ), fg='green')

@chamo.command("harvest")
@click.option('-s', '--size', type=int, default=1000)
@click.option('-n', '--next-id', type=int, default=1)
@click.option('-m', '--modified-since', default=None)
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('--yes-i-know', is_flag=True, callback=abort_if_false,
              expose_value=False,
              prompt='Do you really want to harvest all records?')
@click.option('-f', '--file', type=click.File('r'), default=None)
@with_appcontext
def harvest_chamo(size, next_id, modified_since, verbose, file):
    """Harvest all records."""

    try:
        count = 0
        if file:
            click.secho('Reading records file to harvesting queue ...', fg='green')
            records = []
            for pid in file:
                records.append(pid)
            ChamoRecordHarvester().bulk_to_harvest(records)
            count=len(records)
        else :
            click.secho('Sending records to harvesting queue ...', fg='green')
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
@click.option('--initial', '-i', is_flag=True,
              help='Run harvesting in background.')
@click.option('--delayed', '-d', is_flag=True,
              help='Run harvesting in background.')
@click.option('--concurrency', '-c', default=1, type=int,
              help='Number of concurrent harvesting tasks to start.')
@with_appcontext
def run(initial, delayed, concurrency):
    """Run bulk record harvesting."""
    if delayed:
        celery_kwargs = {
            'kwargs': {
                'bulk_kwargs': {'initial_load': initial}
            }
        }
        click.secho(
            'Starting {0} tasks for harvesting records...'.format(concurrency),
            fg='green')
        for c in range(0, concurrency):
            process_bulk_queue.apply_async(**celery_kwargs)
    else:
        click.secho('Retrieve queued records...', fg='green')
        ChamoRecordHarvester().process_bulk_queue(
            bulk_kwargs={'initial_load': initial})

@chamo.command("record")
@click.option('--bibid', '-i', default=0, type=int,
              help='BIBID of the record.')
@with_appcontext
def record(bibid):
    """Run transform to invenio record."""
    if bibid > 0:
        try:
            print(json.dumps(
                bulk_record(ChamoBibRecord.get_record_by_id(bibid))))
        except:
            pass

@chamo.command("document")
@click.option('--bibid', '-i', default=0, type=int,
              help='BIBID of the record.')
@with_appcontext
def document(bibid):
    """Run transform to invenio record."""
    if bibid > 0:
        record_schema = current_jsonschemas.path_to_url(
            'documents/document-v0.0.1.json')
        record = ChamoBibRecord.get_record_by_id(bibid)
        document = record.document
        document['$schema'] = record_schema
        result = []
        result.append(document)
        print(json.dumps(result))


@chamo.command("max_id")
@click.option('--with-deleted', '-d', is_flag=True,
              help='With deleted record.')
@with_appcontext
def max_id(with_deleted):
    """Get max record identifier."""
    print(get_max_record_pid('doc'))


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


@click.command('create_virtua_loans')
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@click.option('-d', '--debug', 'debug', is_flag=True, default=False)
@click.argument('infile', type=click.File('r'))
@with_appcontext
def create_virtua_loans(infile, verbose, debug):
    """Create circulation transactions from Virtua.
    infile: Json transactions file
    """
    click.secho('Create Virtua circulation transactions:', fg='green')
    data = json.load(infile)
    errors_count = {}
    to_block = []
    for patron_data in data.get('items'):
        patron_barcode   = patron_data.get('patron_barcode')
        item_barcode     = patron_data.get('item_barcode')
        user_id          = patron_data.get('user_id')
        location_id      = patron_data.get('location_id')

        due_date         = ciso8601.parse_datetime(patron_data.get('due_date'))
        checkout_date    = ciso8601.parse_datetime(patron_data.get('checkout_date'))
        organisation_pid = patron_data.get('organisation_id')

        if patron_barcode is None:
            click.secho('Patron barcode is missing!', fg='red')
        else:
            click.echo('Patron: {barcode}'.format(barcode=patron_barcode))
            requests = patron_data.get('requests', {})
            blocked = patron_data.get('blocked', False)

            create_virtua_loan(patron_barcode, item_barcode, user_id, \
                    location_id, checkout_date, due_date, organisation_pid, verbose, debug)


    for key, val in errors_count.items():
        click.secho(
            'Errors {transaction_type}: {count}'.format(
                transaction_type=key,
                count=val
            ),
            fg='red'
        )
    # click.echo(result)


def create_virtua_loan(patron_barcode, item_barcode,
                user_pid, user_location, transaction_date, due_date,
                organisation_pid, verbose=False,
                debug=False):
    """Create loans transactions."""
    try:
        item = Item.get_item_by_barcode(barcode=item_barcode,organisation_pid=organisation_pid)
        patron = Patron.get_patron_by_barcode(barcode=patron_barcode)

        click.secho("Create loan...")
        item.checkout(
            patron_pid=patron.pid,
            transaction_user_pid=user_pid,
            transaction_location_pid=user_location,
            transaction_date=transaction_date,
            document_pid=item.replace_refs()['document']['pid'],
            item_pid=item.pid,
        )
        click.secho("Update due date")

        loan = get_loan_for_item(item_pid_to_object(item.pid))
        loan_pid = loan.get('pid')
        loan = Loan.get_record_by_pid(loan_pid)
        loan['end_date'] = due_date.isoformat()
        loan.update(
            loan,
            dbcommit=True,
            reindex=True
        )
    except Exception as err:
        if verbose:
            click.secho(
                '\tException loan {err}'.format(
                        err=err
                ),
                fg='red'
            )
        if debug:
            traceback.print_exc()
        return None


@click.command('create_virtua_requests')
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@click.option('-d', '--debug', 'debug', is_flag=True, default=False)
@click.argument('infile', type=click.File('r'))
@with_appcontext
def create_virtua_requests(infile, verbose, debug):
    """Create requests from Virtua.
    infile: Json transactions file
    """
    click.secho('Create Virtua requests:', fg='green')
    data = json.load(infile)
    errors_count = {}
    to_block = []
    for patron_data in data.get('items'):
        patron_barcode   = patron_data.get('patron_barcode')
        item_barcode     = patron_data.get('item_barcode')
        location_id      = patron_data.get('location_id')
        date_placed      = ciso8601.parse_datetime(patron_data.get('date_placed'))
        organisation_pid = patron_data.get('organisation_id')

        if patron_barcode is None:
            click.secho('Patron barcode is missing!', fg='red')
        else:
            click.echo('Patron: {barcode}'.format(barcode=patron_barcode))
            create_virtua_request(patron_barcode, item_barcode, \
                    location_id, date_placed, location_id,
                    organisation_pid, verbose, debug)

    for key, val in errors_count.items():
        click.secho(
            'Errors {transaction_type}: {count}'.format(
                transaction_type=key,
                count=val
            ),
            fg='red'
        )


def create_virtua_request(patron_barcode, item_barcode,
                user_location, transaction_date, pickup_location_pid,
                organisation_pid, verbose=False,
                debug=False):
    """Create Virtua request transactions."""
    try:
        item = Item.get_item_by_barcode(barcode=item_barcode,organisation_pid=organisation_pid)
        patron = Patron.get_patron_by_barcode(patron_barcode)

        circ_policy = CircPolicy.provide_circ_policy(
            item.holding_library_pid,
            patron.patron_type_pid,
            item.holding_circulation_category_pid
        )
        if circ_policy.get('allow_requests'):
            item.request(
                patron_pid=patron.pid,
                transaction_location_pid=user_location,
                transaction_user_pid=patron.pid,
                transaction_date=transaction_date,
                pickup_location_pid=pickup_location_pid,
                document_pid=item.replace_refs()['document']['pid'],
            )
    except Exception as err:
        if verbose:
            click.secho(
                '\tException request : {err}'.format(
                    err=err
                ),
                fg='red'
            )
        if debug:
            traceback.print_exc()
        return None


fixtures.add_command(create_virtua_loans)
fixtures.add_command(create_virtua_requests)
