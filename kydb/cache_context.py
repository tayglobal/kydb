from contextlib import contextmanager
import copy


@contextmanager
def cache_context(db):
    # Code to acquire resource, e.g.:
    
    orig_cache = db._cache
    db._cache = copy.copy(orig_cache)
    
    try:
        yield db
    finally:
        db._cache = orig_cache
