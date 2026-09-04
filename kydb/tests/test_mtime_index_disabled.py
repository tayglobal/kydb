""" ``mtime-index: false`` gates ``mtime``, and only ``mtime``.

The setting says "do not maintain *my* timestamp index"
(``additional_index_plan.md`` §13). A user index value is not a
timestamp the backend stamped -- it is a business key the caller
supplied, stored on the object -- so switching the recency index off
must not take it down too.

It used to. ``folder()`` checked the flag eagerly and raised before
``by()`` was ever chained onto the query it returns, so a db with the
flag set recorded ``class_date`` values correctly and then refused to
query them back: the data was right and unreachable. The check now runs
where the query runs, which is the first point that knows *which* index
was asked for.

That timing change is invisible to existing callers, because
``recent()`` was already lazy -- ``list(db.recent(f))`` raises exactly
where it always did.
"""
import os

import pytest

import kydb
from kydb.exceptions import IndexNotSupported


def _db_types():
    env_val = os.environ.get('KYDB_TEST_DB_TYPES')
    if env_val:
        return [x.strip() for x in env_val.split(',') if x.strip()]
    return ['memory', 's3', 'redis', 'dynamodb', 'files', 'union']


ALL_DB_TYPES = _db_types()

#: The backends that maintain the mtime index themselves *and* can store
#: user index values -- the only ones where the two can disagree. Files
#: and S3 read mtime from the substrate and ignore the setting entirely.
INDEXED_DB_TYPES = ['memory', 'redis', 'dynamodb']

DB_HOSTS = {
    'memory': 'memory://mtime_off',
    'redis': 'redis://{}:6379'.format(
        os.environ.get('KINYU_UNITTEST_REDIS_HOST', 'localhost')),
    'dynamodb': 'dynamodb://' + os.environ.get(
        'KINYU_UNITTEST_DYNAMODB', 'kydb-test-table'),
}

SATURDAY = 20260905
SUNDAY = 20260906


@pytest.fixture(params=INDEXED_DB_TYPES)
def off_db(request, local_backends):
    """ A db with ``mtime-index: false``, and an empty ``/signups``.

    The flag is set by assigning ``_config`` directly, as the existing
    compatibility tests do, and restored in a ``finally`` -- db
    instances are cached by URL in ``kydb.api._db_cache``, so a leaked
    config would follow the object into every later test.
    """
    db_type = request.param
    if db_type not in ALL_DB_TYPES:
        pytest.skip(f'{db_type} not in KYDB_TEST_DB_TYPES')

    case = request.node.name.replace('[', '_').replace(']', '')
    db = kydb.connect(f'{DB_HOSTS[db_type]}/unittests/mtime_off/{case}')

    def clear():
        try:
            db.rm_tree('/signups')
        except KeyError:
            pass

    clear()
    db._config = {'mtime-index': False}
    try:
        yield db
    finally:
        db._config = None
        clear()


def book(db):
    """ Two students on Saturday, one on Sunday, one walk-in. """
    db.set('/signups/anna', {'s': 'anna'}, index={'class_date': SATURDAY})
    db.set('/signups/marcus', {'s': 'marcus'}, index={'class_date': SATURDAY})
    db.set('/signups/priya', {'s': 'priya'}, index={'class_date': SUNDAY})
    db['/signups/hugo'] = {'s': 'hugo'}


def roster(db, day):
    return sorted(db.folder('/signups', allow_scan=True)
                  .by('class_date').since(day).until(day))


# --- what the flag must NOT take down ---------------------------------

def test_the_flag_is_actually_off(off_db):
    assert off_db.mtime_index_enabled is False


def test_a_business_key_is_still_writable(off_db):
    book(off_db)
    assert off_db.read('/signups/anna', reload=True) == {'s': 'anna'}


def test_a_business_key_is_still_queryable(off_db):
    """ The regression this whole change exists for. """
    book(off_db)
    assert roster(off_db, SATURDAY) == ['anna', 'marcus']
    assert roster(off_db, SUNDAY) == ['priya']


def test_the_unindexed_object_is_still_absent(off_db):
    # Sparseness does not depend on the mtime index either way.
    book(off_db)
    assert 'hugo' not in roster(off_db, SATURDAY) + roster(off_db, SUNDAY)
    assert 'hugo' in off_db.ls('/signups')


def test_ordering_by_the_business_key_still_works(off_db):
    """ Assert the ordering, not the tie-break.

    ``anna`` and ``marcus`` share a ``class_date``, and objects tying on
    the index value have no defined order relative to each other -- a
    stable client-side sort keeps them in scan order while a server-side
    ``desc()`` reverses them. So the day sequence is what is asserted
    here, and the tie only as set membership.
    """
    book(off_db)
    query = off_db.folder('/signups', allow_scan=True).by('class_date')

    asc = list(query.asc().entries())
    assert [e.index_value for e in asc] == [SATURDAY, SATURDAY, SUNDAY]
    assert {e.key for e in asc[:2]} == {'anna', 'marcus'}
    assert asc[2].key == 'priya'

    desc = list(query.desc().entries())
    assert [e.index_value for e in desc] == [SUNDAY, SATURDAY, SATURDAY]
    assert desc[0].key == 'priya'
    assert {e.key for e in desc[1:]} == {'anna', 'marcus'}


def test_a_row_reports_no_write_time_rather_than_a_fake_one(off_db):
    """ 0 is the epoch tail's "write time unknown", and is the honest
    answer: nothing recorded one. The business key must not be passed
    off as a timestamp.
    """
    book(off_db)
    entries = list(off_db.folder('/signups', allow_scan=True)
                   .by('class_date').asc().entries())
    assert [e.index_value for e in entries] == [SATURDAY, SATURDAY, SUNDAY]
    assert [e.mtime for e in entries] == [0, 0, 0]


# --- what the flag must still take down -------------------------------

@pytest.mark.parametrize('call', [
    lambda db: list(db.recent('/signups', allow_scan=True)),
    lambda db: list(db.folder('/signups', allow_scan=True).by('mtime')),
    lambda db: list(db.folder('/signups', allow_scan=True)
                    .by('mtime').entries()),
    lambda db: list(db.folder('/signups', allow_scan=True)
                    .by('mtime').items()),
], ids=['recent', 'iter', 'entries', 'items'])
def test_every_mtime_entry_point_still_raises(off_db, call):
    book(off_db)
    with pytest.raises(IndexNotSupported) as excinfo:
        call(off_db)
    assert 'mtime-index' in str(excinfo.value)


def test_reindex_still_raises_eagerly(off_db):
    """ ``reindex`` maintains the mtime index rather than reading it, so
    its guard stays where it was -- there is no index name to wait for.
    """
    with pytest.raises(IndexNotSupported):
        off_db.reindex('/signups')


# --- the mechanism ----------------------------------------------------

def test_folder_itself_no_longer_raises(off_db):
    """ The check moved to where the query runs.

    ``folder()`` cannot know which index the caller wants -- ``by()`` is
    chained onto the object it returns -- so checking there was what
    closed ``by('class_date')`` along with ``by('mtime')``.
    """
    query = off_db.folder('/signups', allow_scan=True)
    assert query is not None
    # Still refuses on iteration, because the default index is mtime.
    with pytest.raises(IndexNotSupported):
        list(query)


def test_the_flag_does_not_leak_into_a_normal_db(off_db):
    """ A second db without the setting is unaffected -- the gate is
    per-db config, not global state.
    """
    book(off_db)
    normal = kydb.connect('memory://mtime_on_control')
    try:
        normal['/signups/zoe'] = {'s': 'zoe'}
        assert normal.mtime_index_enabled is True
        assert list(normal.recent('/signups', allow_scan=True)) == ['zoe']
    finally:
        normal.rm_tree('/signups')


# --- the union ---------------------------------------------------------

def test_union_skips_a_member_whose_mtime_index_is_off():
    """ Per-member partial capability, the rule a union already used for
    a member with no index at all.
    """
    off = kydb.connect('memory://union_off_member')
    on = kydb.connect('memory://union_on_member')
    union = kydb.connect(
        'memory://union_off_member;memory://union_on_member')
    folder = '/unittests/union_mtime_off/'
    try:
        off._config = {'mtime-index': False}
        off[folder + 'from_off'] = 1
        on[folder + 'from_on'] = 2

        assert list(union.recent(folder, allow_scan=True)) == ['from_on']
    finally:
        off._config = None
        off.rm_tree(folder)
        on.rm_tree(folder)


def test_union_raises_when_every_member_has_it_off():
    """ Skipping every member would silently return an empty result,
    which reads as "nothing was written" rather than "nothing is
    recording timestamps".
    """
    db1 = kydb.connect('memory://union_all_off_1')
    db2 = kydb.connect('memory://union_all_off_2')
    union = kydb.connect(
        'memory://union_all_off_1;memory://union_all_off_2')
    folder = '/unittests/union_all_off/'
    try:
        db1._config = {'mtime-index': False}
        db2._config = {'mtime-index': False}
        db1[folder + 'one'] = 1

        with pytest.raises(IndexNotSupported) as excinfo:
            list(union.recent(folder, allow_scan=True))
        assert 'mtime' in str(excinfo.value)
    finally:
        db1._config = None
        db2._config = None
        db1.rm_tree(folder)
