# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Invenio.
# Copyright (C) 2019 UCLouvain.
#
# Invenio-Chamo-Harvester is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Celery tasks to harvest records from Chamo Rest API."""

from __future__ import absolute_import, print_function

import time
import traceback
from datetime import datetime
import sys

import click
import requests
from celery import shared_task
from copy import deepcopy
from flask import current_app
from invenio_db import db
from rero_ils.modules.api import IlsRecordsIndexer
from invenio_jsonschemas import current_jsonschemas
from invenio_pidstore.models import PersistentIdentifier
from rero_ils.modules.documents.api import Document, DocumentsSearch
from rero_ils.modules.documents.models import DocumentIdentifier
from rero_ils.modules.holdings.api import Holding
from rero_ils.modules.holdings.models import HoldingIdentifier
from rero_ils.modules.items.api import Item
from rero_ils.modules.items.models import ItemIdentifier
from .utils import get_max_record_pid

from .api import ChamoRecordHarvester
from .utils import extract_records_id, map_item_type, map_locations


@shared_task(ignore_result=True)
def process_bulk_queue(bulk_kwargs=None):
    """Process bulk harvesting queue.

    :param str version_type: Elasticsearch version type.
    Note: You can start multiple versions of this task.
    """
    ChamoRecordHarvester().process_bulk_queue(bulk_kwargs)


@shared_task(ignore_result=True)
def queue_records_to_harvest(size=1000, next_id=None, modified_since=None,
                             verbose=False):
    """Queue records to harvest from Chamo Rest API."""
    uri = '{base_url}/{route}?all=true&batchSize={size}'.format(
        base_url=current_app.config['CHAMO_HARVESTER_CHAMO_BASE_URL'],
        route='bibs',
        size=size)
    if next_id:
        uri += '&next={next_id}'.format(next_id=next_id)
    if verbose:
        click.echo('Get records from {uri}'.format(uri=uri))

    try:
        count = 0
        request = requests.get(uri, auth=(
            current_app.config['CHAMO_HARVESTER_CHAMO_USER'],
            current_app.config['CHAMO_HARVESTER_CHAMO_PASSWORD']))
        data = request.json()

        next = data.get('next', {})
        while next:
            records = extract_records_id(data)
            if verbose:
                click.echo('List records :  {records}'.format(records=records))
            ChamoRecordHarvester().bulk_to_harvest(records)
            count += len(records)

            request = requests.get(next, auth=(
                current_app.config['CHAMO_HARVESTER_CHAMO_USER'],
                current_app.config['CHAMO_HARVESTER_CHAMO_PASSWORD']))
            data = request.json()
            next = data.get('next', None)
        records = extract_records_id(data)
        if verbose:
            click.echo('List records :  {records}'.format(records=records))
        ChamoRecordHarvester().bulk_to_harvest(records)
        count += len(records)
        return count
    except Exception as e:
        click.secho(
            'Harvesting API Error: {e}'.format(e=e),
            fg='red'
        )
        return 0, uri, []


@shared_task(ignore_result=True)
def harvest_record(record_uuid):
    """Index a single record.

    :param record_uuid: The record UUID.
    """
    ChamoRecordHarvester().harvest_by_id(record_uuid)


@shared_task(ignore_result=True)
def delete_record(record_uuid):
    """Delete a single record.

    :param record_uuid: The record UUID.
    """
    ChamoRecordHarvester().delete_by_id(record_uuid)


@shared_task(ignore_result=True)
def bulk_records(records, bulk_kwargs=None):
    """Records bulk creation."""
    bulk_size = current_app.config['CHAMO_HARVESTER_BULK_SIZE']
    initial_import = bulk_kwargs.pop('initial_load')
    bulk_index = bulk_kwargs.pop('bulk_index')
    current_app.logger.info('harverster bulk size : {size}'.format(
        size=bulk_size))
    n_updated = 0
    n_rejected = 0
    n_created = 0
    record_schema = current_jsonschemas.path_to_url('documents/document-v0.0.1.json')
    item_schema = current_jsonschemas.path_to_url('items/item-v0.0.1.json')
    holding_schema = current_jsonschemas.path_to_url('holdings/holding-v0.0.1.json')
    host_url = current_app.config.get('RERO_ILS_APP_URL')
    url_api = '{host}/api/{doc_type}/{pid}'
    required = ['pid', 'type', 'title', 'language']
    bulk_delete_item = []
    bulk_delete_hold = []
    record_id_iterator = []
    item_id_iterator = []
    holding_id_iterator = []
    indexer = IlsRecordsIndexer()
    start_time = datetime.now()
    for record in records:
        try:
            if record.get('frbr'):
                continue
                # raise Exception('FRBR record cannot be processed')
            # TODO: check chamo:2033646 => record is masked
            if record.get('masked'):
                continue
                # raise Exception('masked record will be not processed')
            document = record.get('document', {})
            if not all(elem in document.keys() for elem in required):
                raise Exception('missing required {f} properties for record'
                                .format(f=required))

            # TODO: create task to update records
            if not initial_import:
                # check if already in Rero-ILS
                rec = Document.get_record_by_pid(document.get('pid'))

                if rec:
                    # UPDATE DOCUMENT
                    # doc_pid = rec.get('pid')
                    # for ite_obj in Item.get_items_pid_by_document_pid(doc_pid):
                    #     try:
                    #         item_pid = ite_obj.get('value')
                    #         item = Item.get_record_by_pid(item_pid)
                    #         if item:
                    #             item.delete(force=True, dbcommit=True, delindex=True)
                    #         else :
                    #             # TODO: delete by id
                    #             pass
                    #     except Exception as e:
                    #         print('ERROR deleting item:', e)
                    #         pass

                    # update document
                    document['$schema'] = record_schema
                    current_app.logger.info('update document')
                    document = rec.replace(
                        document,
                        dbcommit=False,
                        reindex=False
                    )
                    record_id_iterator.append(document.id)
            else:
                # NEW DOCUMENT
                document['$schema'] = record_schema
                current_app.logger.info('create document')
                document = Document.create(
                    document,
                    dbcommit=False,
                    reindex=False
                )
                db.session.add(DocumentIdentifier(recid=document.get('pid')))
                record_id_iterator.append(document.id)
                uri_documents = url_api.format(host=host_url,
                                            doc_type='documents',
                                            pid=document.get('pid'))

                # HOLDINGS
                map_holdings = {}
                items = record.get('items', [])
                for holding in record.get('holdings'):
                    new_holding = deepcopy(holding)
                    new_holding['$schema'] = holding_schema

                    new_holding['document'] = {
                        '$ref': uri_documents
                    }
                    new_holding['circulation_category'] = {
                        '$ref': map_item_type(
                            str(holding.get('circulation_category')))
                    }
                    new_holding['location'] = {
                        '$ref': map_locations(str(holding.get('location')))
                    }
                    holding_map = '{location}#{cica}'.format(
                                location=holding.get('location'),
                                cica=holding.get('circulation_category'))

                    result = Holding.create(
                        new_holding,
                        dbcommit=False,
                        reindex=False
                    )

                    map_holdings.update({
                            holding_map: result.get('pid')
                        }
                    )
                    holding_id_iterator.append(result.id)

                # ITEMS
                for item in items:
                    new_item = deepcopy(item)
                    new_item['$schema'] = item_schema
                    new_item['document'] = {
                        '$ref': uri_documents
                        }
                    new_item['item_type'] = {
                        '$ref': map_item_type(str(item.get('item_type')))
                        }
                    new_item['location'] = {
                        '$ref': map_locations(str(item.get('location')))
                        }

                    holding_pid = map_holdings.get(
                        '{location}#{cica}'.format(
                            location=item.get('location'),
                            cica=item.get('item_type')))
                    if holding_pid is None:
                        click.secho('holding pid is None for record : {id} '.format(
                            id=document.pid
                        ), fg='red')
                        click.secho('holding map : {map}.'.format(
                            map=map_holdings), fg='white')
                        click.secho('item to map : {location}#{cica}'.format(
                            location=item.get('location'),
                            cica=item.get('item_type')), fg='yellow')
                    new_item['holding'] = {
                        '$ref': url_api.format(
                            host=host_url,
                            doc_type='holdings',
                            pid=holding_pid)
                        }
                    result = Item.create(
                        new_item,
                        dbcommit=False,
                        reindex=False
                    )
                    db.session.add(
                        ItemIdentifier(recid=result.get('pid')))
                    item_id_iterator.append(result.id)
                n_created += 1
        except Exception as e:
            n_rejected += 1
            traceback.print_exc()
            current_app.logger.error(
                'Error processing record [{id}] : {e}'.format(
                    id=str(record.get('_id')).strip(),
                    e=str(e)
                ), exc_info=True
            )
        # db.session.flush()
        if n_created % bulk_size == 0:
            db.session.commit()
            if bulk_index:
                # HOLDINGS
                indexer.bulk_index(holding_id_iterator, doc_type='hold')
                indexer.process_bulk_queue()
                # ITEMS
                indexer.bulk_index(item_id_iterator, doc_type='item')
                indexer.process_bulk_queue()
                # DOCUMENTS
                indexer.bulk_index(record_id_iterator, doc_type='doc')
                indexer.process_bulk_queue()

            record_id_iterator.clear()
            holding_id_iterator.clear()
            item_id_iterator.clear()

    try:
        db.session.commit()

        if bulk_index:
            indexer.bulk_index(holding_id_iterator, doc_type='hold')
            indexer.process_bulk_queue()
            indexer.bulk_index(item_id_iterator, doc_type='item')
            indexer.process_bulk_queue()
            indexer.bulk_index(record_id_iterator, doc_type='doc')
            indexer.process_bulk_queue()
    except Exception as e:
        current_app.logger.error(e)

    max_recid = get_max_record_pid('doc')
    DocumentIdentifier._set_sequence(max_recid)
    max_recid = get_max_record_pid('item')
    ItemIdentifier._set_sequence(max_recid)
    db.session.commit()
    return n_created


@shared_task(ignore_result=True)
def bulk_record(record):
    """Records creation."""
    record_schema = current_jsonschemas.path_to_url('documents/document-v0.0.1.json')
    item_schema = current_jsonschemas.path_to_url('items/item-v0.0.1.json')
    holding_schema = current_jsonschemas.path_to_url('holdings/holding-v0.0.1.json')
    host_url = current_app.config.get('RERO_ILS_APP_URL')
    url_api = '{host}/api/{doc_type}/{pid}'
    required = ['pid', 'type', 'title', 'language']
    try:
        if record.isFrbr:
            raise Exception('FRBR record cannot be processed')

        if record.isMasked:
            raise Exception('masked record will not be processed')

        document = record.document
        document['$schema'] = record_schema

        if not all(elem in document.keys() for elem in required):
            raise Exception('missing required properties in document')

        uri_documents = url_api.format(host=host_url,
                                        doc_type='documents',
                                        pid=document.get('pid'))

        map_holdings = {}
        holdings = []
        for idx, holding in enumerate(record.holdings):
            new_holding = deepcopy(holding)
            new_holding['$schema'] = holding_schema
            new_holding['document'] = {
                '$ref': uri_documents
                }
            new_holding['circulation_category'] = {
                '$ref': map_item_type(str(holding.get('circulation_category')))
                }
            new_holding['location'] = {
                '$ref': map_locations(str(holding.get('location')))
                }
            holding_map = '{location}#{cica}'.format(
                location=holding.get('location'),
                cica=holding.get('circulation_category'))

            map_holdings.update({
                    holding_map: idx
                }
            )
            holdings.append(new_holding)

        items = []
        for item in record.items:
            new_item = deepcopy(item)
            new_item['$schema'] = item_schema
            new_item['document'] = {
                '$ref': uri_documents
                }
            new_item['item_type'] = {
                '$ref': map_item_type(str(item.get('item_type')))
                }
            new_item['location'] = {
                '$ref': map_locations(str(item.get('location')))
                }
            new_item['type'] = 'standard'
            # item['type'] = 'standard' if holdings_type == 'standard' \
            #     else 'issue'
            holding_pid = map_holdings.get(
                '{location}#{cica}'.format(
                    location= item.get('location'),
                    cica= item.get('item_type')))
            if holding_pid is None:
                click.secho('holding pid is None for record : {id} '.format(
                    id=document.get('pid')
                ), fg='red')
                click.secho('holding map : {map}.'.format(
                    map=map_holdings), fg='white')
                click.secho('item to map : {location}#{cica}'.format(
                    location = item.get('location'),
                    cica=item.get('item_type')), fg='yellow')

            new_item['holding'] = {
                '$ref': url_api.format(host=host_url,
                            doc_type='holdings',
                            pid=holding_pid)
                }

            items.append(new_item)
        return {
            'document': document,
            'holdings': holdings,
            'items': items
        }
    except Exception as e:
        current_app.logger.error('Error converting record [{id}] : {e}'
            .format(
                id=str(record.data.get('_id')).strip(),
                e=str(e)))
        raise


def has_items(holding, items):
    """check if holding has items."""
    # TODO: use filter
    for item in items:
        if holding == item.get('holding'):
            return True
    return False
