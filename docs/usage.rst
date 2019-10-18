..
    Copyright (C) 2019 UCLouvain.

    Invenio-Chamo-Harvester is free software; you can redistribute it
    and/or modify it under the terms of the MIT License; see LICENSE file for
    more details.


Usage
=====

Init queue :


.. code-block:: console

  $ invenio chamo queue init


Purge queue :

.. code-block:: console

  $ invenio chamo queue purge

Bulk record in queue :

.. code-block:: console

  $ invenio chamo harvest --size 1000 --yes-i-know

  available options :
    -n, --next-id         : start at specific id.
    -m, --modified-since  : all id modified after date (YYYY-MM-dd'T'HH:mm:ssZ).
    -s, --size            : size of batch.
    --yes-i-know          : confirm to start harvesting.
    -v, --verbose         : display more informations.

Run queue :

.. code-block:: console

  $ invenio chamo run

  available options :
    -c, --concurrency : number of concurrent harvesting tasks to start.
    -d, --delayed     : run harvesting in background.
