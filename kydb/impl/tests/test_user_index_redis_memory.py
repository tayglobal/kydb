""" User-supplied index values on Redis and Memory, plus the Files/S3
refusal.

The gym question from ``user_index_plan.md`` §1.1 -- *who is booked into
today's classes?* -- is the shape every test here takes: the booking is
written on one day for a class on another, so an implementation that
quietly ordered by ``mtime`` would fail rather than merely look odd.

Redis and Memory are covered by the same parametrised bodies wherever
the contract is shared, because it *is* shared: one native sorted set,
one client-side scan, identical answers. The backend-specific tests
below them cover what only that backend can get wrong -- for Redis,
index entries outliving their object and the byte-for-byte key names an
existing database depends on.
"""
import os
from tempfile import gettempdir

import pytest

import kydb
from kydb.exceptions import IndexNotSupported


def _db_types():
    env_val = os.environ.get('KYDB_TEST_DB_TYPES')
    if env_val:
        return [x.strip() for x in env_val.split(',') if x.strip()]
    return ['memory', 's3', 'redis', 'dynamodb', 'files', 'union']


ALL_DB_TYPES = _db_types()

DB_URLS = {
    'memory': 'memory://user_index_memory',
    'redis': 'redis://{}:6379'.format(
        os.environ.get('KINYU_UNITTEST_REDIS_HOST', 'localhost')),
    'files': 'files:/' + gettempdir() + '/kydb_tests',
    's3': 's3://' + os.environ.get('KINYU_UNITTEST_S3_BUCKET', 'kydb-test'),
}

# Both backends that store user index values, exercised identically.
INDEXED_DB_TYPES = ['memory', 'redis']


def _require(db_type: str):
    if db_type not in ALL_DB_TYPES:
        pytest.skip(f'{db_type} not in KYDB_TEST_DB_TYPES')


def get_db(db_type: str):
    _require(db_type)
    return kydb.connect(DB_URLS[db_type])


def query(db, folder: str):
    """ A folder query on either backend.

    ``allow_scan=True`` is what MemoryDB requires to opt into its O(n)
    sort and is a documented no-op on Redis, so one call serves both.
    """
    return db.folder(folder, allow_scan=True)


def names(q):
    return list(q)


# The roster: three students, three distinct class dates, written in an
# order that matches none of them, plus a walk-in with no class booked.
BOOKINGS = [
    ('bob', 20260906),
    ('anna', 20260905),
    ('cara', 20260907),
]


def seed(db, folder: str):
    for student, class_date in BOOKINGS:
        db.set(folder + student, {'student': student},
               index={'class_date': class_date})
    # No class_date at all: present in the folder, absent from the index.
    db[folder + 'walkin'] = {'student': 'walkin'}


def cleanup(db, folder: str):
    try:
        db.rm_tree(folder)
    except KeyError:
        pass


@pytest.fixture(params=INDEXED_DB_TYPES)
def indexed_db(request):
    """ A db that stores user index values, with an empty folder to use.

    Yields ``(db, folder)``. The folder is named after the test so the
    Redis instance -- which, unlike Memory, persists across tests in the
    session -- cannot leak rows from one case into another.
    """
    db = get_db(request.param)
    folder = '/unittests/user_index/{}_{}/'.format(
        request.param, request.node.name.replace('[', '_').replace(']', ''))
    cleanup(db, folder)
    try:
        yield db, folder
    finally:
        cleanup(db, folder)


# --- the round trip ---------------------------------------------------

def test_write_and_query_round_trip(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    assert names(query(db, folder).by('class_date').asc()) == \
        ['anna', 'bob', 'cara']


def test_items_reads_the_objects_back(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    got = list(query(db, folder).by('class_date').asc().items())
    assert got == [
        ('anna', {'student': 'anna'}),
        ('bob', {'student': 'bob'}),
        ('cara', {'student': 'cara'}),
    ]


def test_ordering_both_ways(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    q = query(db, folder).by('class_date')
    assert names(q.asc()) == ['anna', 'bob', 'cara']
    assert names(q.desc()) == ['cara', 'bob', 'anna']


def test_the_ordering_is_the_class_date_not_the_booking_time(indexed_db):
    """ The bookings are written in the order bob, anna, cara -- which is
    neither the ascending nor the descending class order, so an
    implementation that fell back to ``mtime`` cannot pass by accident.
    """
    db, folder = indexed_db
    seed(db, folder)
    assert names(query(db, folder).by('class_date').asc()) != \
        names(query(db, folder).by('mtime').asc())


# --- sparseness: no epoch tail ---------------------------------------

@pytest.mark.parametrize('direction', ['asc', 'desc'])
def test_an_object_with_no_index_value_never_appears(indexed_db, direction):
    """ ``user_index_plan.md`` §5: a missing user index value is not
    "older than everything", it is "not in this ordering at all".
    """
    db, folder = indexed_db
    seed(db, folder)
    q = getattr(query(db, folder).by('class_date'), direction)()
    assert 'walkin' not in names(q)
    # ...but it is genuinely there, so the query really did exclude it
    # rather than never having seen it.
    assert 'walkin' in db.ls(folder)
    assert 'walkin' in names(query(db, folder).by('mtime'))


def test_no_epoch_tail_even_with_an_unbounded_query(indexed_db):
    # since=None is the case where the mtime index *does* read the
    # folder for un-indexed objects. A user index must not.
    db, folder = indexed_db
    seed(db, folder)
    entries = list(query(db, folder).by('class_date').asc().entries())
    assert [e.key for e in entries] == ['anna', 'bob', 'cara']
    assert all(e.index_value >= 20260905 for e in entries)


def test_an_index_nobody_wrote_is_empty_not_an_error(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    assert names(query(db, folder).by('never_written')) == []


# --- since / until / between -----------------------------------------

def test_since_is_an_inclusive_lower_bound(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    q = query(db, folder).by('class_date').asc()
    assert names(q.since(20260906)) == ['bob', 'cara']


def test_until_is_an_inclusive_upper_bound(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    q = query(db, folder).by('class_date').asc()
    assert names(q.until(20260906)) == ['anna', 'bob']


def test_the_day_exact_query(indexed_db):
    """ ``.since(d).until(d)`` -- the question the front desk asks. """
    db, folder = indexed_db
    seed(db, folder)
    q = query(db, folder).by('class_date')
    assert names(q.since(20260906).until(20260906)) == ['bob']
    assert names(q.since(20260904).until(20260904)) == []


def test_a_closed_range_spanning_two_days(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    q = query(db, folder).by('class_date').asc()
    assert names(q.since(20260905).until(20260906)) == ['anna', 'bob']


def test_an_inverted_range_yields_nothing(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    q = query(db, folder).by('class_date')
    assert names(q.since(20260907).until(20260905)) == []


def test_bounds_apply_descending_too(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    q = query(db, folder).by('class_date').desc()
    assert names(q.since(20260905).until(20260906)) == ['bob', 'anna']


# --- limit ------------------------------------------------------------

def test_limit_caps_the_result(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    q = query(db, folder).by('class_date')
    assert names(q.asc().limit(2)) == ['anna', 'bob']
    assert names(q.desc().limit(1)) == ['cara']


def test_limit_composes_with_the_bounds(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    q = query(db, folder).by('class_date').asc().since(20260906).limit(1)
    assert names(q) == ['bob']


def test_limit_does_not_pad_with_unindexed_objects(indexed_db):
    """ A limit larger than the index must not be topped up from the
    folder -- there is no epoch tail to top it up from.
    """
    db, folder = indexed_db
    seed(db, folder)
    assert names(query(db, folder).by('class_date').asc().limit(10)) == \
        ['anna', 'bob', 'cara']


# --- entries() --------------------------------------------------------

def test_entries_report_the_index_value_and_a_real_mtime(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    entries = list(query(db, folder).by('class_date').asc().entries())

    assert [e.key for e in entries] == ['anna', 'bob', 'cara']
    assert [e.index_value for e in entries] == [20260905, 20260906, 20260907]

    for entry in entries:
        # A nanosecond timestamp of the write, not the business value:
        # ordering by class_date must not make entries() lie about when
        # the object was actually written.
        assert entry.mtime > 10 ** 18
        assert entry.mtime != entry.index_value
        assert entry.ctime <= entry.mtime

    # Written bob, anna, cara -- so mtime order is not class_date order.
    assert [e.mtime for e in entries] != sorted(e.mtime for e in entries)


def test_an_mtime_query_still_reports_index_value_as_the_mtime(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    for entry in query(db, folder).by('mtime').entries():
        assert entry.index_value == entry.mtime


# --- preserve on rewrite, and the explicit clear ----------------------

def test_a_rewrite_with_no_index_preserves_the_value(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    db.set(folder + 'anna', {'student': 'anna', 'paid': True})

    q = query(db, folder).by('class_date')
    assert names(q.since(20260905).until(20260905)) == ['anna']
    assert db.read(folder + 'anna', reload=True) == \
        {'student': 'anna', 'paid': True}


def test_setitem_preserves_the_value_too(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    db[folder + 'anna'] = {'student': 'anna', 'v': 2}
    assert names(query(db, folder).by('class_date').since(20260905)
                 .until(20260905)) == ['anna']


def test_an_explicit_none_clears_the_value(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    db.set(folder + 'anna', {'student': 'anna'},
           index={'class_date': None})

    q = query(db, folder).by('class_date')
    assert names(q.asc()) == ['bob', 'cara']
    # Cleared from the index, still a perfectly good object.
    assert db.read(folder + 'anna', reload=True) == {'student': 'anna'}
    assert 'anna' in db.ls(folder)
    assert 'anna' in names(query(db, folder).by('mtime'))


def test_a_cleared_value_can_be_written_again(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    db.set(folder + 'anna', {'student': 'anna'}, index={'class_date': None})
    db.set(folder + 'anna', {'student': 'anna'},
           index={'class_date': 20260907})
    assert names(query(db, folder).by('class_date').since(20260907)
                 .until(20260907).asc()) == ['anna', 'cara']


def test_rebooking_moves_the_student_between_days(indexed_db):
    """ The move that path-encoded dates cannot do atomically: one
    ``set(index=...)``, and the student leaves one roster and joins
    another.
    """
    db, folder = indexed_db
    seed(db, folder)
    db.set(folder + 'anna', {'student': 'anna'},
           index={'class_date': 20260907})

    q = query(db, folder).by('class_date')
    assert names(q.since(20260905).until(20260905)) == []
    assert sorted(names(q.since(20260907).until(20260907))) == \
        ['anna', 'cara']


# --- several indexes on one object ------------------------------------

def test_two_indexes_on_the_same_object_are_independent(indexed_db):
    db, folder = indexed_db
    db.set(folder + 'anna', 1, index={'class_date': 20260905, 'rank': 3})
    db.set(folder + 'bob', 2, index={'class_date': 20260906, 'rank': 1})
    db.set(folder + 'cara', 3, index={'rank': 2})

    assert names(query(db, folder).by('class_date').asc()) == ['anna', 'bob']
    assert names(query(db, folder).by('rank').asc()) == \
        ['bob', 'cara', 'anna']


def test_clearing_one_index_leaves_the_other(indexed_db):
    db, folder = indexed_db
    db.set(folder + 'anna', 1, index={'class_date': 20260905, 'rank': 3})
    db.set(folder + 'anna', 1, index={'rank': None})

    assert names(query(db, folder).by('class_date')) == ['anna']
    assert names(query(db, folder).by('rank')) == []


def test_negative_and_zero_values_order_correctly(indexed_db):
    db, folder = indexed_db
    db.set(folder + 'a', 1, index={'rank': -5})
    db.set(folder + 'b', 2, index={'rank': 0})
    db.set(folder + 'c', 3, index={'rank': 7})

    q = query(db, folder).by('rank')
    assert names(q.asc()) == ['a', 'b', 'c']
    # Zero is a value like any other, not "missing".
    assert names(q.since(0).until(0)) == ['b']


# --- delete cleanup ---------------------------------------------------

def test_an_index_entry_does_not_outlive_its_object(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    db.delete(folder + 'anna')

    q = query(db, folder).by('class_date')
    assert names(q.asc()) == ['bob', 'cara']
    assert names(q.since(20260905).until(20260905)) == []


def test_deleting_and_rewriting_does_not_resurrect_the_old_value(indexed_db):
    db, folder = indexed_db
    seed(db, folder)
    db.delete(folder + 'anna')
    db[folder + 'anna'] = {'student': 'anna'}
    # Rewritten with no index: it must not come back at its old date.
    assert names(query(db, folder).by('class_date').asc()) == ['bob', 'cara']


# --- Redis internals --------------------------------------------------

def _redis_folder(db, folder: str) -> str:
    return db._ensure_slashes(db._get_full_path(folder))[:-1]


def test_redis_mtime_key_names_are_unchanged():
    """ Existing databases must not be invalidated.

    The `mtime` keys are asserted against their literal spelling rather
    than against the helpers, so a refactor that routes them through the
    parameterised user-index helpers still cannot move them.
    """
    from kydb.impl.redis import RedisDB

    folder = '/unittests/user_index/keys'
    assert RedisDB._mtime_key(folder) == 'kydb:mtime-index:' + folder
    assert RedisDB._mtime_val_key(folder) == 'kydb:mtime-values:' + folder
    assert RedisDB._ctime_key(folder) == 'kydb:ctime-index:' + folder

    # The user-index helpers are the same shape with the name filled in
    # -- which is exactly what makes `mtime`'s spelling above safe.
    assert RedisDB._index_key(folder, 'class_date') == \
        'kydb:class_date-index:' + folder
    assert RedisDB._index_val_key(folder, 'class_date') == \
        'kydb:class_date-values:' + folder
    assert RedisDB._index_key(folder, 'mtime') == RedisDB._mtime_key(folder)

    # Every index key stays out of the object namespace: object paths
    # always start with '/', these never do.
    for key in (RedisDB._index_key(folder, 'class_date'),
                RedisDB._index_val_key(folder, 'class_date'),
                RedisDB._index_names_key(folder)):
        assert key.startswith('kydb:')
        assert not key.startswith('/')


def test_redis_index_entries_are_removed_on_delete():
    db = get_db('redis')
    folder = '/unittests/user_index/redis_delete/'
    try:
        seed(db, folder)
        full = _redis_folder(db, folder)
        zset = db._index_key(full, 'class_date')
        values = db._index_val_key(full, 'class_date')

        assert db.connection.zscore(zset, 'anna') == 20260905
        assert db.connection.hget(values, 'anna') is not None

        db.delete(folder + 'anna')

        assert db.connection.zscore(zset, 'anna') is None
        assert db.connection.hget(values, 'anna') is None
        assert db.connection.zcard(zset) == 2
    finally:
        cleanup(db, folder)


def test_redis_rm_tree_leaves_no_index_entries():
    db = get_db('redis')
    folder = '/unittests/user_index/redis_rmtree/'
    seed(db, folder)
    full = _redis_folder(db, folder)
    db.rm_tree(folder)

    assert db.connection.zcard(db._index_key(full, 'class_date')) == 0
    assert db.connection.hlen(db._index_val_key(full, 'class_date')) == 0


def test_redis_records_the_index_names_of_a_folder():
    db = get_db('redis')
    folder = '/unittests/user_index/redis_names/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': 20260905, 'rank': 2})
        full = _redis_folder(db, folder)
        assert db._index_names(full) == ['class_date', 'rank']

        # An untouched folder has no names, and its delete path is
        # therefore exactly the one it always was.
        other = _redis_folder(db, '/unittests/user_index/redis_names_none/')
        assert db._index_names(other) == []
    finally:
        cleanup(db, folder)


def test_redis_clearing_a_value_removes_it_from_both_structures():
    db = get_db('redis')
    folder = '/unittests/user_index/redis_clear/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': 20260905})
        db.set(folder + 'anna', 1, index={'class_date': None})

        full = _redis_folder(db, folder)
        assert db.connection.zscore(
            db._index_key(full, 'class_date'), 'anna') is None
        assert db.connection.hget(
            db._index_val_key(full, 'class_date'), 'anna') is None
    finally:
        cleanup(db, folder)


def _count_round_trips(db):
    """ Count Redis round trips, a pipeline execute counting as one. """
    calls = []
    conn = db.connection
    original_execute_command = conn.execute_command
    original_pipeline = conn.pipeline

    def counting_execute_command(*args, **kwargs):
        calls.append(args[0] if args else None)
        return original_execute_command(*args, **kwargs)

    def counting_pipeline(*args, **kwargs):
        pipe = original_pipeline(*args, **kwargs)
        original_pipe_execute = pipe.execute

        def counting_pipe_execute(*a, **kw):
            calls.append('PIPELINE')
            return original_pipe_execute(*a, **kw)

        pipe.execute = counting_pipe_execute
        return pipe

    conn.execute_command = counting_execute_command
    conn.pipeline = counting_pipeline
    return calls, lambda: (
        setattr(conn, 'execute_command', original_execute_command),
        setattr(conn, 'pipeline', original_pipeline))


def test_redis_an_indexed_write_is_still_one_round_trip():
    db = get_db('redis')
    folder = '/unittests/user_index/redis_trips/'
    try:
        # Warm the folder so mkdir_raw's own writes are not counted.
        db[folder + 'warm'] = 1

        calls, restore = _count_round_trips(db)
        try:
            db.set(folder + 'anna', 1,
                   index={'class_date': 20260905, 'rank': 2})
        finally:
            restore()
        assert calls.count('PIPELINE') == 1
    finally:
        cleanup(db, folder)


def test_redis_folder_hash_is_untouched_by_index_writes():
    """ The user index is an index, not a directory listing: the object
    appears in ls() exactly once whether or not it carries a value.
    """
    db = get_db('redis')
    folder = '/unittests/user_index/redis_ls/'
    try:
        seed(db, folder)
        assert sorted(db.ls(folder)) == ['anna', 'bob', 'cara', 'walkin']
    finally:
        cleanup(db, folder)


# --- Memory internals -------------------------------------------------

def test_memory_raw_entries_carry_the_index_value():
    db = get_db('memory')
    folder = '/unittests/user_index/memory_rows/'
    seed(db, folder)

    rows = sorted(db._folder_time_entries(folder, 'class_date'))
    assert [(name, index_value) for name, _m, _c, index_value in rows] == \
        [('anna', 20260905), ('bob', 20260906), ('cara', 20260907)]

    # mtime rows are unchanged in meaning: the ordering value is the
    # timestamp, and the walk-in is present because it has one.
    mtime_rows = sorted(db._folder_time_entries(folder))
    assert [name for name, _m, _c, _v in mtime_rows] == \
        ['anna', 'bob', 'cara', 'walkin']
    assert all(mtime == index_value
               for _n, mtime, _c, index_value in mtime_rows)


def test_memory_delete_removes_the_index_values():
    db = get_db('memory')
    folder = '/unittests/user_index/memory_delete/'
    seed(db, folder)
    db.delete(folder + 'anna')
    assert [row[0] for row in db._folder_time_entries(folder, 'class_date')] \
        == ['bob', 'cara']


def test_memory_still_requires_allow_scan_for_a_user_index():
    db = get_db('memory')
    with pytest.raises(IndexNotSupported):
        db.folder('/unittests/user_index/memory_scan/').by('class_date')


# --- Files and S3: loudly unsupported ---------------------------------

@pytest.fixture(params=['files', 's3'])
def unindexed_db(request):
    """ A backend with nowhere to put a caller-supplied value.

    No implementation change was needed for either: the default
    ``BaseDB.supports_user_index = False`` is what makes them raise, and
    these tests pin that down so a later backend cannot become silently
    lossy by forgetting a guard.
    """
    db = get_db(request.param)
    folder = '/unittests/user_index/{}/'.format(request.param)
    try:
        yield db, folder
    finally:
        cleanup(db, folder)


def test_set_with_an_index_raises(unindexed_db):
    db, folder = unindexed_db
    with pytest.raises(IndexNotSupported) as excinfo:
        db.set(folder + 'anna', 1, index={'class_date': 20260905})
    assert 'class_date' in str(excinfo.value)


def test_a_rejected_indexed_write_leaves_no_object(unindexed_db):
    db, folder = unindexed_db
    with pytest.raises(IndexNotSupported):
        db.set(folder + 'anna', 1, index={'class_date': 20260905})
    assert not db.exists(folder + 'anna')


def test_supports_user_index_is_false(unindexed_db):
    db, _folder = unindexed_db
    assert db.supports_user_index is False


def test_querying_a_user_index_raises(unindexed_db):
    db, folder = unindexed_db
    db[folder + 'anna'] = 1
    with pytest.raises(IndexNotSupported):
        list(db.folder(folder, allow_scan=True).by('class_date').entries())


def test_an_unindexed_write_still_works(unindexed_db):
    db, folder = unindexed_db
    db.set(folder + 'anna', 1)
    db.set(folder + 'bob', 2, index=None)
    db.set(folder + 'cara', 3, index={})
    assert db.read(folder + 'anna', reload=True) == 1
    assert names(db.folder(folder, allow_scan=True).by('mtime')) != []
