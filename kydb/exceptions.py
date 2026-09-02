class KydbException(Exception):
    pass


class DbObjException(KydbException):
    pass


class IndexNotSupported(KydbException):
    """ Raised by ``folder()`` / ``recent()`` recency queries on a backend
    (or index name) that has no server-side ordering to serve them.

    Backends without a native ordering index must raise this rather than
    silently falling back to a full scan-and-sort. An opt-in
    ``allow_scan=True`` client-side fallback is planned separately.
    """
    pass
