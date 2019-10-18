# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 UCLouvain.
#
# Invenio-Chamo-Harvester is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Utility functions for data processing."""

import requests


def extract_records_id(data):
    """Extract a record id from REST data."""
    records = []
    uris = data.get('links', {})
    for uri in uris:
        record_id = uri.split('/')[-1]
        records.append(record_id)
    return records


def map_item_type(type):
    """Returns mapped type."""
    item_types = {
        "1": "https://ils.rero.ch/api/item_types/1",
        "2": "https://ils.rero.ch/api/item_types/2",
        "3": "https://ils.rero.ch/api/item_types/3",
        "4": "https://ils.rero.ch/api/item_types/4",
        "5": "https://ils.rero.ch/api/item_types/5",
        "6": "https://ils.rero.ch/api/item_types/6",
        "101": "https://ils.rero.ch/api/item_types/7",
        "102": "https://ils.rero.ch/api/item_types/8",
        "103": "https://ils.rero.ch/api/item_types/9",
        "104": "https://ils.rero.ch/api/item_types/10",
        "105": "https://ils.rero.ch/api/item_types/11",
        "106": "https://ils.rero.ch/api/item_types/12",
        "110": "https://ils.rero.ch/api/item_types/13",
        "250": "https://ils.rero.ch/api/item_types/14",
        "251": "https://ils.rero.ch/api/item_types/15",
        "252": "https://ils.rero.ch/api/item_types/16",
        "253": "https://ils.rero.ch/api/item_types/17",
        "254": "https://ils.rero.ch/api/item_types/18",
        "255": "https://ils.rero.ch/api/item_types/19",
        "256": "https://ils.rero.ch/api/item_types/20",
        "257": "https://ils.rero.ch/api/item_types/21",
        "258": "https://ils.rero.ch/api/item_types/22",
        "259": "https://ils.rero.ch/api/item_types/23",
        "260": "https://ils.rero.ch/api/item_types/24",
        "261": "https://ils.rero.ch/api/item_types/25",
        "262": "https://ils.rero.ch/api/item_types/26",
        "300": "https://ils.rero.ch/api/item_types/27",
        "301": "https://ils.rero.ch/api/item_types/28",
        "303": "https://ils.rero.ch/api/item_types/29",
        "304": "https://ils.rero.ch/api/item_types/30",
        "305": "https://ils.rero.ch/api/item_types/31",
        "306": "https://ils.rero.ch/api/item_types/32",
        "308": "https://ils.rero.ch/api/item_types/33",
        "309": "https://ils.rero.ch/api/item_types/34",
        "310": "https://ils.rero.ch/api/item_types/35",
        "777": "https://ils.rero.ch/api/item_types/36",
        "999": "https://ils.rero.ch/api/item_types/37"
    }
    return item_types.get(type)


def map_locations(location):
    """Returns mapped location."""
    locations = {
        "100000": "https://ils.rero.ch/api/locations/1",
        "100001": "https://ils.rero.ch/api/locations/2",
        "100002": "https://ils.rero.ch/api/locations/3",
        "100003": "https://ils.rero.ch/api/locations/4",
        "200002": "https://ils.rero.ch/api/locations/5",
        "200003": "https://ils.rero.ch/api/locations/6",
        "200004": "https://ils.rero.ch/api/locations/7",
        "200005": "https://ils.rero.ch/api/locations/8",
        "200006": "https://ils.rero.ch/api/locations/9",
        "200007": "https://ils.rero.ch/api/locations/10",
        "200008": "https://ils.rero.ch/api/locations/11",
        "200009": "https://ils.rero.ch/api/locations/12",
        "200010": "https://ils.rero.ch/api/locations/13",
        "300000": "https://ils.rero.ch/api/locations/14",
        "300001": "https://ils.rero.ch/api/locations/15",
        "300002": "https://ils.rero.ch/api/locations/16",
        "300003": "https://ils.rero.ch/api/locations/17",
        "400000": "https://ils.rero.ch/api/locations/18",
        "400001": "https://ils.rero.ch/api/locations/19",
        "400002": "https://ils.rero.ch/api/locations/20",
        "400003": "https://ils.rero.ch/api/locations/21",
        "400004": "https://ils.rero.ch/api/locations/22",
        "400005": "https://ils.rero.ch/api/locations/23",
        "410000": "https://ils.rero.ch/api/locations/24",
        "500000": "https://ils.rero.ch/api/locations/25",
        "500001": "https://ils.rero.ch/api/locations/26",
        "500002": "https://ils.rero.ch/api/locations/27",
        "500003": "https://ils.rero.ch/api/locations/28",
        "500004": "https://ils.rero.ch/api/locations/29",
        "600000": "https://ils.rero.ch/api/locations/30",
        "600009": "https://ils.rero.ch/api/locations/31",
        "600010": "https://ils.rero.ch/api/locations/32",
        "600012": "https://ils.rero.ch/api/locations/33",
        "600013": "https://ils.rero.ch/api/locations/34",
        "600014": "https://ils.rero.ch/api/locations/35",
        "600015": "https://ils.rero.ch/api/locations/36",
        "600017": "https://ils.rero.ch/api/locations/37",
        "600018": "https://ils.rero.ch/api/locations/38",
        "600019": "https://ils.rero.ch/api/locations/39",
        "700000": "https://ils.rero.ch/api/locations/40",
        "700009": "https://ils.rero.ch/api/locations/41",
        "700010": "https://ils.rero.ch/api/locations/42",
        "700012": "https://ils.rero.ch/api/locations/43",
        "800000": "https://ils.rero.ch/api/locations/44",
        "900000": "https://ils.rero.ch/api/locations/45",
        "1010000": "https://ils.rero.ch/api/locations/46",
        "1020000": "https://ils.rero.ch/api/locations/47",
        "1030000": "https://ils.rero.ch/api/locations/48",
        "1040000": "https://ils.rero.ch/api/locations/49",
        "1050001": "https://ils.rero.ch/api/locations/50",
        "1050002": "https://ils.rero.ch/api/locations/51",
        "1050003": "https://ils.rero.ch/api/locations/52",
        "1050004": "https://ils.rero.ch/api/locations/53",
        "1060000": "https://ils.rero.ch/api/locations/54",
        "1060001": "https://ils.rero.ch/api/locations/55",
        "1060002": "https://ils.rero.ch/api/locations/56",
        "11000000": "https://ils.rero.ch/api/locations/57",
        "20600000": "https://ils.rero.ch/api/locations/58",
        "20700001": "https://ils.rero.ch/api/locations/59",
        "20700002": "https://ils.rero.ch/api/locations/60",
        "20700003": "https://ils.rero.ch/api/locations/61",
        "20700004": "https://ils.rero.ch/api/locations/62",
        "21000001": "https://ils.rero.ch/api/locations/63",
        "21000002": "https://ils.rero.ch/api/locations/64",
        "21000003": "https://ils.rero.ch/api/locations/65",
        "21000004": "https://ils.rero.ch/api/locations/66",
        "21000006": "https://ils.rero.ch/api/locations/67",
        "21000008": "https://ils.rero.ch/api/locations/68",
        "21000009": "https://ils.rero.ch/api/locations/69",
        "30100000": "https://ils.rero.ch/api/locations/70",
        "30100001": "https://ils.rero.ch/api/locations/71",
        "30100002": "https://ils.rero.ch/api/locations/72",
        "30200000": "https://ils.rero.ch/api/locations/73",
        "30300000": "https://ils.rero.ch/api/locations/74",
        "30400000": "https://ils.rero.ch/api/locations/75"
    }
    return locations.get(location)
