class KydbException(Exception):
    pass


class DbObjException(KydbException):
    pass


class IndexNotSupported(KydbException):
    """ Raised when an index cannot serve what was asked of it.

    On the **read** side -- ``folder()`` / ``recent()`` queries on a
    backend (or index name) with no server-side ordering to serve them.
    Backends without a native ordering index must raise this rather than
    silently falling back to a full scan-and-sort. Supported backends offer
    an explicit ``allow_scan=True`` client-side fallback.

    On the **write** side -- ``set(key, value, index={...})`` on a
    backend that cannot store caller-supplied index values (Files and
    S3: see ``BaseDB.supports_user_index``). The write raises rather
    than accepting the values and dropping them, because a silent drop
    is the one failure that reaches production undetected: every write
    succeeds and only the query, later and elsewhere, comes back empty.
    """
    pass
