# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 UCLouvain.
#
# Invenio-Chamo-Harvester is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Module tests."""

from __future__ import absolute_import, print_function

from flask import Flask
from invenio_chamo_harvester import InvenioChamoHarvester


def test_version():
    """Test version import."""
    from invenio_chamo_harvester import __version__
    assert __version__


def test_init():
    """Test extension initialization."""
    app = Flask('testapp')
    ext = InvenioChamoHarvester(app)
    assert 'invenio-chamo-harvester' in app.extensions

    app = Flask('testapp')
    ext = InvenioChamoHarvester()
    assert 'invenio-chamo-harvester' not in app.extensions
    ext.init_app(app)
    assert 'invenio-chamo-harvester' in app.extensions
