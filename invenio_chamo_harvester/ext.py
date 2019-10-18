# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 UCLouvain.
#
# Invenio-Chamo-Harvester is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Flask exension for Invenio-Chamo-Harvester."""

from __future__ import absolute_import, print_function

import six
from flask import current_app
from werkzeug.utils import cached_property, import_string

from . import config


class InvenioChamoHarvester(object):
    """Invenio-Chamo-Harvester extension."""

    def __init__(self, app=None):
        """Extension initialization.

        :param app: The Flask application. (Default: ``None``)
        """
        if app:
            self.init_app(app)

    def init_app(self, app):
        """Flask application initialization.

        :param app: The Flask application.
        """
        self.init_config(app)
        app.extensions['invenio-chamo-harvester'] = self

    def init_config(self, app):
        """Initialize configuration.

        :param app: The Flask application.
        """
        for k in dir(app.config):
            if k.startswith('CHAMO_HARVESTER_'):
                app.config.setdefault(k, getattr(app.config, k))
