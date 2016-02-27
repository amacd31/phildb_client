PhilDBClient
============

The PhilDB timeseries database client aims to provide a
`PhilDB database API
<https://phildb.readthedocs.org/en/latest/api/phildb.html#module-phildb.database>`_
compatible object. At this stage mostly the read related methods have been implemented.
None of the write related methods have been implemented.
Any methods yet to be implemented in the client will raise a NotImplementedError if called.

Example usage
-------------

.. code::

    from phildb_client import PhilDBClient
    db = PhilDBClient('http://localhost:8889')

    db.ts_list()

Notes
-----
This client is experimental and depends on the experimental `PhilDB server
<https://github.com/amacd31/phildb_server>`_.
By default the client reads data from the server in the `msgpack
<https://msgpack.org>`_ format.
The data served by the server and read by the client is using the
`experimental Pandas msgpack implementation
<http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_msgpack.html>`_,
as such it is dependant on the version of Pandas used by the server and the client being compatible.
There is a known issue with Pandas < v0.17 (which has been made a dependency for the server),
the client however can use an older version of Pandas and use JSON as the transport format instead of msgpack if required.
For example:
.. code::

    from phildb_client import PhilDBClient
    db = PhilDBClient('http://localhost:8889', 'json')
