.. _Cache Context:

Cache Context
=============

Often we would like to serve a single request which requires
repeated reads to a single object Perhaps pricing a book has
underlying instruments accessing the same IRCurve.
KYDB already does cacheing so that we hit the database
only once. However what if we point to some live market data
and want to recalculate the book price each time we call
``Book.Price()``?

Well we could:

#. Call ``db.refresh()``, but that would clear caches that we want
   to keep. For example if we know no new trades where done,
   then why reload all the book’s position on each tick?

#. Call ``db.load('MyMarketData', refresh=True)``.
   But now we have to keep track of what market data needs
   invalidating.

This is where the ``cache_context()`` comes handy. See the example below:


::

    db = kydb.connect('dynamodb://tradingdb')
    book = db['/Books/MyBook']

    with db.cache_context():

        positions = book.Positions()

        while keep_pricing():

            with db.cache_context():
                for qty, inst in positions.items():
                    print((qt, inst, inst.Price()))

            wait_for_next_marketdata_tick()

            # At this point all market data cache disappears

    # At this point all position cache disappears