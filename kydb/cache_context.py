from contextlib import contextmanager
import copy


@contextmanager
def cache_context(db):
    """The Cache Context

    See :ref:`Cache Context`

    """
    orig_cache = db._cache
    db._cache = copy.copy(orig_cache)

    try:
        yield db
    finally:
        db._cache = orig_cache
