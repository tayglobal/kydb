""" Shared query-builder contract: the pieces every backend implements
against.

Nothing here touches a real ordering index -- the backend-specific
round trips live in ``kydb/impl/tests``. These cover the parts that must
behave identically everywhere: the immutability of the builder, the
inclusive ``since``/``until`` bounds, ``Entry``'s value semantics, and
the write-side validation that runs before any backend is reached.
"""
from tempfile import gettempdir

import pytest

import kydb
from kydb.exceptions import IndexNotSupported
from kydb.folder_meta import FolderMetaMixin
from kydb.query import Entry, FolderQuery, ScanFolderQuery


@pytest.fixture
def db():
    return kydb.connect('memory://test_query')


@pytest.fixture
def unindexed_db():
    """ A backend that cannot store caller-supplied index values.

    Files (like S3) reads ``mtime`` from the substrate and has nowhere
    to put a user index value, so it keeps
    ``supports_user_index = False`` -- which is what these tests need to
    exercise. MemoryDB was the stand-in while no backend implemented the
    feature; now that it stores index values, only Files and S3 are
    honest examples of the unsupported case.
    """
    return kydb.connect('files:/' + gettempdir() + '/kydb_test_query')


@pytest.fixture
def base_query(db):
    return db.folder('/my-folder', allow_scan=True).by('mtime')


class RowsFolderQuery(ScanFolderQuery):
    """ A ``ScanFolderQuery`` over a fixed list of rows.

    Stands in for a backend that has been generalised to report a user
    index value, so the Stage 1 filtering/sorting contract can be tested
    before any backend implements it. ``rows`` is passed through
    ``_clone`` like every other builder field.
    """

    _SUPPORTED_INDEXES = ('mtime', 'class_date')

    def __init__(self, *args, rows=(), **kwargs):
        super().__init__(*args, **kwargs)
        self._rows = rows

    def _clone(self, **overrides):
        clone = super()._clone(**overrides)
        clone._rows = self._rows
        return clone

    def _raw_entries(self):
        yield from self._rows


def make_query(db, rows, index_name='class_date'):
    return RowsFolderQuery(db, '/my-folder', index_name=index_name,
                           allow_scan=True, rows=rows)


# --- Entry ------------------------------------------------------------

def test_entry_index_value_defaults_to_none():
    entry = Entry('foo', 10, 5)
    assert entry.index_value is None
    assert (entry.key, entry.mtime, entry.ctime) == ('foo', 10, 5)


def test_entry_equality_includes_index_value():
    assert Entry('foo', 10, 5, 99) == Entry('foo', 10, 5, 99)
    assert Entry('foo', 10, 5, 99) != Entry('foo', 10, 5, 100)
    assert Entry('foo', 10, 5, 99) != Entry('foo', 10, 5)
    assert Entry('foo', 10, 5) == Entry('foo', 10, 5)


def test_entry_equality_with_non_entry():
    assert Entry('foo', 10, 5, 99) != ('foo', 10, 5, 99)
    assert Entry('foo', 10, 5, 99).__eq__(object()) is NotImplemented


def test_entry_hash_includes_index_value():
    assert hash(Entry('foo', 10, 5, 99)) == hash(Entry('foo', 10, 5, 99))
    assert len({Entry('foo', 10, 5, 99), Entry('foo', 10, 5, 100)}) == 2
    assert len({Entry('foo', 10, 5, 99), Entry('foo', 10, 5, 99)}) == 1


def test_entry_repr_shows_index_value():
    assert repr(Entry('foo', 10, 5, 99)) == \
        "Entry(key='foo', mtime=10, ctime=5, index_value=99)"


def test_entry_index_value_is_separate_from_mtime():
    # A result ordered by class_date still carries a genuine mtime --
    # conflating them would make entries() lie about when the object
    # was written.
    entry = Entry('anna', mtime=1700, ctime=1600, index_value=20260905)
    assert entry.mtime == 1700
    assert entry.index_value == 20260905


# --- until(): cloning and immutability --------------------------------

def test_until_returns_a_new_query(base_query):
    query = base_query.until(30)
    assert query is not base_query
    assert isinstance(query, FolderQuery)


def test_until_does_not_mutate_the_receiver(base_query):
    base_query.until(30)
    assert base_query._until_ts is None


def test_until_records_the_bound(base_query):
    assert base_query.until(30)._until_ts == 30


def test_until_survives_further_chaining(base_query):
    query = base_query.until(30).since(10).desc().limit(3).by('class_date')
    assert query._until_ts == 30
    assert query._since_ts == 10
    assert query._ascending is False
    assert query._limit_n == 3
    assert query._index_name == 'class_date'


def test_until_is_carried_through_clone(base_query):
    # _clone must round-trip until_ts, or a later chained call would
    # silently drop the upper bound.
    query = base_query.until(30)
    assert query.asc()._until_ts == 30
    assert query.desc()._until_ts == 30
    assert query.limit(1)._until_ts == 30
    assert query.by('class_date')._until_ts == 30
    assert query.since(1)._until_ts == 30


def test_a_half_built_query_can_be_branched(base_query):
    base = base_query.since(10)
    one_day = base.until(10)
    open_ended = base.desc()
    assert base._until_ts is None
    assert one_day._until_ts == 10
    assert open_ended._until_ts is None
    assert one_day._ascending is True


# --- Bounds -----------------------------------------------------------

ROWS = [
    ('anna', 1000, 900, 20260904),
    ('bob', 1100, 1000, 20260905),
    ('cara', 1200, 1100, 20260906),
]


def names(query):
    return list(query)


def test_both_bounds_are_inclusive(db):
    query = make_query(db, ROWS).since(20260904).until(20260906)
    assert names(query) == ['anna', 'bob', 'cara']


def test_since_and_until_on_the_same_value_is_one_exact_day(db):
    query = make_query(db, ROWS).since(20260905).until(20260905)
    assert names(query) == ['bob']


def test_until_alone_is_an_open_lower_bound(db):
    assert names(make_query(db, ROWS).until(20260905)) == ['anna', 'bob']


def test_since_alone_is_an_open_upper_bound(db):
    assert names(make_query(db, ROWS).since(20260905)) == ['bob', 'cara']


def test_inverted_range_yields_nothing(db):
    # An empty result is the honest answer to an empty range: the
    # caller asked for values both >= 5 and <= 3.
    assert names(make_query(db, ROWS).since(20260906).until(20260904)) == []


def test_bounds_apply_to_the_index_value_not_the_mtime(db):
    # The mtimes here (1000-1200) are far below any class_date bound,
    # so filtering on r[1] would drop everything.
    query = make_query(db, ROWS).since(20260904).until(20260906)
    assert len(list(query.entries())) == 3


def test_sorting_is_on_the_index_value(db):
    # Written out of order on purpose: sorting on mtime would give the
    # insertion order back and pass by accident.
    rows = [
        ('cara', 1000, 1000, 20260906),
        ('anna', 1100, 1100, 20260904),
        ('bob', 1200, 1200, 20260905),
    ]
    assert names(make_query(db, rows).asc()) == ['anna', 'bob', 'cara']
    assert names(make_query(db, rows).desc()) == ['cara', 'bob', 'anna']


def test_limit_applies_after_ordering(db):
    assert names(make_query(db, ROWS).desc().limit(2)) == ['cara', 'bob']


def test_entries_report_the_index_value_and_a_real_mtime(db):
    entries = list(make_query(db, ROWS).asc().entries())
    assert [e.index_value for e in entries] == [20260904, 20260905, 20260906]
    assert [e.mtime for e in entries] == [1000, 1100, 1200]
    assert [e.ctime for e in entries] == [900, 1000, 1100]


# --- Sparseness: no epoch tail ----------------------------------------

def test_objects_with_no_index_value_are_dropped(db):
    # Not defaulted to 0 and yielded at the tail the way a missing
    # mtime is: an object with no class_date is not a signup for an
    # infinitely distant past class, it is not a signup at all.
    rows = list(ROWS) + [('dave', 1300, 1300, None)]
    assert names(make_query(db, rows).asc()) == ['anna', 'bob', 'cara']
    assert names(make_query(db, rows).desc()) == ['cara', 'bob', 'anna']


def test_unindexed_objects_are_dropped_with_no_bounds_at_all(db):
    rows = [('dave', 1300, 1300, None)]
    assert names(make_query(db, rows)) == []


def test_three_element_rows_still_work_for_mtime(db):
    # Files and S3 report (name, mtime, ctime) and have nowhere to
    # store a user index value; for an mtime query the mtime is the
    # index value.
    rows = [('anna', 1000, 900), ('bob', 1200, 1100)]
    query = make_query(db, rows, index_name='mtime')
    assert names(query.desc()) == ['bob', 'anna']
    assert [e.index_value for e in query.asc().entries()] == [1000, 1200]


def test_three_element_rows_are_absent_from_a_user_index(db):
    rows = [('anna', 1000, 900), ('bob', 1200, 1100)]
    assert names(make_query(db, rows, index_name='class_date')) == []


def test_scan_query_rejects_an_unsupported_index(unindexed_db):
    query = unindexed_db.folder('/my-folder', allow_scan=True).by('class_date')
    with pytest.raises(IndexNotSupported):
        list(query.entries())


# --- set(index=...) validation ----------------------------------------

@pytest.fixture
def indexable_db():
    """ A db that has opted into storing user index values, so
    validation is reached and is not short-circuited by the
    capability check.
    """
    db = kydb.connect('memory://test_query_indexable')
    unindexed_db.supports_user_index = True
    unindexed_db.set_raw = lambda key, value, index=None: None
    return db


@pytest.mark.parametrize('name', [
    '1class_date',      # must not start with a digit
    '_class_date',      # must not start with an underscore
    'class-date',       # hyphen is not allowed
    'class date',       # nor a space
    'class.date',       # nor a dot
    'class#date',
    '',
])
def test_bad_index_name_raises_value_error(indexable_db, name):
    with pytest.raises(ValueError) as excinfo:
        indexable_db.set('/my-folder/anna', 123, index={name: 20260905})
    assert repr(name) in str(excinfo.value)


@pytest.mark.parametrize(
    'name', ['path', 'folder', 'contents', 'mtime', 'ctime'])
def test_reserved_index_name_raises_value_error(indexable_db, name):
    with pytest.raises(ValueError) as excinfo:
        indexable_db.set('/my-folder/anna', 123, index={name: 20260905})
    assert repr(name) in str(excinfo.value)
    assert 'reserved' in str(excinfo.value)


def test_non_string_index_name_raises_value_error(indexable_db):
    with pytest.raises(ValueError):
        indexable_db.set('/my-folder/anna', 123, index={7: 20260905})


@pytest.mark.parametrize('value, type_name', [
    (True, 'bool'),          # bool is an int subclass -- rejected anyway
    (False, 'bool'),
    (20260905.0, 'float'),
    ('20260905', 'str'),
    (b'20260905', 'bytes'),
    ([20260905], 'list'),
])
def test_bad_index_value_raises_type_error(indexable_db, value, type_name):
    with pytest.raises(TypeError) as excinfo:
        indexable_db.set('/my-folder/anna', 123,
                         index={'class_date': value})
    message = str(excinfo.value)
    assert "'class_date'" in message
    assert type_name in message


def test_index_must_be_a_mapping(indexable_db):
    with pytest.raises(TypeError):
        indexable_db.set('/my-folder/anna', 123, index=[('class_date', 1)])


def test_none_index_value_is_accepted_as_a_clear(indexable_db):
    indexable_db.set('/my-folder/anna', 123, index={'class_date': None})


def test_negative_and_zero_index_values_are_accepted(indexable_db):
    indexable_db.set('/my-folder/anna', 123, index={'rank': 0})
    indexable_db.set('/my-folder/anna', 123, index={'rank': -1})


def test_a_bad_index_is_rejected_before_anything_is_written(db):
    key = '/my-folder/anna'
    with pytest.raises(TypeError):
        db.set(key, 123, index={'class_date': 'not-an-int'})
    assert not db.exists(key)


def test_validation_runs_before_the_capability_check(unindexed_db):
    # A malformed index is wrong on every backend, so it must be
    # reported as malformed rather than as "unsupported here" -- which
    # would send the caller looking in the wrong place.
    assert unindexed_db.supports_user_index is False
    with pytest.raises(ValueError):
        unindexed_db.set(
            '/my-folder/anna', 123, index={'class-date': 20260905})
    with pytest.raises(TypeError):
        unindexed_db.set('/my-folder/anna', 123, index={'class_date': True})


# --- set(index=...) on a backend that cannot store it -----------------

def test_index_on_an_unsupporting_backend_raises(unindexed_db):
    # Silently dropping the value is the one bug that reaches
    # production undetected: every write succeeds and only the query,
    # later and elsewhere, comes back empty.
    assert unindexed_db.supports_user_index is False
    with pytest.raises(IndexNotSupported) as excinfo:
        unindexed_db.set(
            '/my-folder/anna', 123, index={'class_date': 20260905})
    assert 'class_date' in str(excinfo.value)


def test_rejected_index_write_leaves_no_object_behind(unindexed_db):
    key = '/my-folder/anna'
    with pytest.raises(IndexNotSupported):
        unindexed_db.set(key, 123, index={'class_date': 20260905})
    assert not unindexed_db.exists(key)


def test_writes_with_no_index_are_unaffected(db):
    key = '/my-folder/anna'
    db.set(key, 123)
    db.set('/my-folder/bob', 234, index=None)
    db.set('/my-folder/cara', 345, index={})
    assert db.read(key, reload=True) == 123
    assert db.read('/my-folder/bob', reload=True) == 234
    assert db.read('/my-folder/cara', reload=True) == 345


def test_setitem_is_unchanged(db):
    db['/my-folder/anna'] = 123
    assert db.read('/my-folder/anna', reload=True) == 123


def test_index_is_keyword_only(db):
    with pytest.raises(TypeError):
        db.set('/my-folder/anna', 123, False, {'class_date': 20260905})


# --- FolderMetaMixin forwarding ---------------------------------------

class IndexingDummyDb(FolderMetaMixin, kydb.BaseDB):
    """ Minimal backend that has opted into user index values, to pin
    down what ``FolderMetaMixin.set_raw`` forwards.
    """

    supports_user_index = True

    def __init__(self, url):
        super().__init__(url)
        self.cache = {}
        self.writes = []

    def get_raw(self, key):
        return self.cache[key]

    def folder_meta_set_raw(self, key, value, index=None):
        self.cache[key] = value
        self.writes.append((key, index))


def test_set_raw_forwards_index_to_folder_meta_set_raw():
    db = IndexingDummyDb('memory://test_query_forward')
    db.set('/my-folder/anna', 123, index={'class_date': 20260905})
    assert ('/my-folder/anna', {'class_date': 20260905}) in db.writes


def test_folder_meta_markers_never_carry_index_values():
    # Directories are not objects: keeping them out of every index is
    # the same sparseness that keeps them out of the mtime index.
    db = IndexingDummyDb('memory://test_query_markers')
    db.set('/my-folder/anna', 123, index={'class_date': 20260905})
    markers = [(key, index) for key, index in db.writes
               if FolderMetaMixin._is_folder_meta(key.rsplit('/', 1)[-1])]
    assert markers
    assert all(index is None for _, index in markers)


def test_a_write_with_no_index_passes_no_index_argument():
    # Backends not yet generalised keep a two-argument
    # folder_meta_set_raw, so the kwarg must only appear when there is
    # something to pass.
    class LegacyDb(IndexingDummyDb):
        def folder_meta_set_raw(self, key, value):
            self.cache[key] = value
            self.writes.append((key, None))

    db = LegacyDb('memory://test_query_legacy')
    db.set('/my-folder/anna', 123)
    assert db.read('/my-folder/anna', reload=True) == 123
