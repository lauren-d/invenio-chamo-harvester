# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 UCLouvain.
#
# Invenio-Chamo-Harvester is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Utility functions for data processing."""

import requests
from flask import current_app
from invenio_pidstore.models import PersistentIdentifier
from sqlalchemy import text


def extract_records_id(data):
    """Extract a record id from REST data."""
    records = []
    uris = data.get('links', {})
    for uri in uris:
        record_id = uri.split('/')[-1]
        records.append(record_id)
    return records


def get_max_record_pid(pid_type):
    """Get max record PID."""
    return PersistentIdentifier.query.filter_by(
        pid_type=pid_type).order_by(text('pid_value desc')).first().id


def map_item_type(type):
    """Returns mapped type."""
    host_url = current_app.config.get('RERO_ILS_APP_URL')
    item_types = {
        "1": "{host}/api/item_types/1",
        "2": "{host}/api/item_types/2",
        "3": "{host}/api/item_types/3",
        "4": "{host}/api/item_types/4",
        "5": "{host}/api/item_types/5",
        "6": "{host}/api/item_types/6",
        "101": "{host}/api/item_types/7",
        "102": "{host}/api/item_types/8",
        "103": "{host}/api/item_types/9",
        "104": "{host}/api/item_types/10",
        "105": "{host}/api/item_types/11",
        "106": "{host}/api/item_types/12",
        "110": "{host}/api/item_types/13",
        "250": "{host}/api/item_types/17",
        "251": "{host}/api/item_types/18",
        "252": "{host}/api/item_types/19",
        "253": "{host}/api/item_types/20",
        "254": "{host}/api/item_types/21",
        "255": "{host}/api/item_types/22",
        "256": "{host}/api/item_types/23",
        "257": "{host}/api/item_types/24",
        "258": "{host}/api/item_types/25",
        "259": "{host}/api/item_types/26",
        "260": "{host}/api/item_types/27",
        "261": "{host}/api/item_types/28",
        "262": "{host}/api/item_types/29",
        "300": "{host}/api/item_types/30",
        "301": "{host}/api/item_types/15",
        "303": "{host}/api/item_types/31",
        "304": "{host}/api/item_types/32",
        "305": "{host}/api/item_types/33",
        "306": "{host}/api/item_types/34",
        "308": "{host}/api/item_types/35",
        "309": "{host}/api/item_types/36",
        "310": "{host}/api/item_types/37",
        "777": "{host}/api/item_types/14",
        "999": "{host}/api/item_types/16"
    }
    return item_types.get(type).format(host=host_url)


def map_locations(location):
    """Returns mapped location."""
    host_url = current_app.config.get('RERO_ILS_APP_URL')
    # location 200000 can be removed after production correction
    locations = {
        "100000": "{host}/api/locations/1",
        "100001": "{host}/api/locations/2",
        "100002": "{host}/api/locations/3",
        "100003": "{host}/api/locations/4",
        "200000": "{host}/api/locations/5",
        "200002": "{host}/api/locations/5",
        "200003": "{host}/api/locations/6",
        "200004": "{host}/api/locations/7",
        "200005": "{host}/api/locations/8",
        "200006": "{host}/api/locations/9",
        "200007": "{host}/api/locations/10",
        "200008": "{host}/api/locations/11",
        "200009": "{host}/api/locations/12",
        "200010": "{host}/api/locations/13",
        "300000": "{host}/api/locations/14",
        "300001": "{host}/api/locations/15",
        "300002": "{host}/api/locations/16",
        "300003": "{host}/api/locations/17",
        "400000": "{host}/api/locations/18",
        "400001": "{host}/api/locations/19",
        "400002": "{host}/api/locations/20",
        "400003": "{host}/api/locations/21",
        "400004": "{host}/api/locations/22",
        "400005": "{host}/api/locations/23",
        "410000": "{host}/api/locations/72",
        "500000": "{host}/api/locations/24",
        "500001": "{host}/api/locations/25",
        "500002": "{host}/api/locations/26",
        "500003": "{host}/api/locations/27",
        "500004": "{host}/api/locations/28",
        "600000": "{host}/api/locations/29",
        "600009": "{host}/api/locations/30",
        "600010": "{host}/api/locations/31",
        "600014": "{host}/api/locations/33",
        "600019": "{host}/api/locations/36",  # BST-ELIA
        "700000": "{host}/api/locations/37",
        "700009": "{host}/api/locations/38",
        "700010": "{host}/api/locations/39",
        "800000": "{host}/api/locations/41",
        "900000": "{host}/api/locations/42",
        "1020000": "{host}/api/locations/43",
        "600020": "{host}/api/locations/44",  # BST-INGI
        "1040000": "{host}/api/locations/45",
        "1050001": "{host}/api/locations/46",
        "1050002": "{host}/api/locations/47",
        "1050003": "{host}/api/locations/48",
        "1050004": "{host}/api/locations/49",
        "1060000": "{host}/api/locations/50",
        "1060002": "{host}/api/locations/52",
        "11000000": "{host}/api/locations/53",
        "20600000": "{host}/api/locations/54",
        "20700001": "{host}/api/locations/55",
        "20700002": "{host}/api/locations/56",
        "20700003": "{host}/api/locations/57",
        "20700004": "{host}/api/locations/58",
        "21000000": "{host}/api/locations/59",
        "21000001": "{host}/api/locations/59",
        "21000002": "{host}/api/locations/60",
        "21000003": "{host}/api/locations/61",
        "21000004": "{host}/api/locations/62",
        "21000006": "{host}/api/locations/63",
        "21000008": "{host}/api/locations/64",
        "21000009": "{host}/api/locations/65",
        "30100000": "{host}/api/locations/66",
        "30100001": "{host}/api/locations/67",
        "30100002": "{host}/api/locations/68",
        "30200000": "{host}/api/locations/69",
        "30300000": "{host}/api/locations/70",
        "30400000": "{host}/api/locations/71",
        "410000": "{host}/api/locations/72",
        "200011": "{host}/api/locations/73"
    }
    return locations.get(location).format(host=host_url)
