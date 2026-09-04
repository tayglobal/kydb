""" User index values through the wrapper databases.

``UnionDB`` and ``CacheDB`` are not backends -- they own no storage and
maintain no index. What they own is a *routing* decision, and each has
exactly one to get right:

* ``UnionDB`` merges the members' already-sorted results. The merge key
  must be the value they sorted by (``Entry.index_value``), not
  ``mtime``, or three correctly-ordered rosters arrive as one unordered
  one. Every bound has to reach the members too, since the union filters
  nothing itself.
* ``CacheDB`` records index values on ``persist_db`` only -- the db its
  ``folder()``/``list_dir()`` already read from. The cache holds
  whatever happened to be read, so an index built there could never
  answer a folder-wide question.

The last section covers ``DbObj``, whose write path pickles the object
itself and therefore has to carry the index values separately from
``BaseDB._serialise``.
"""
import pytest

import kydb
from kydb.exceptions import IndexNotSupported
from kydb.tests.test_objdb import DBOBJ_CONFIG

SATURDAY = 20260905
SUNDAY = 20260906
MONDAY = 20260907


def roster(db, day: int):
    return sorted(db.folder('/signups', allow_scan=True)
                  .by('class_date').since(day).until(day))


def by_class_date(db):
    return db.folder('/signups', allow_scan=True).by('class_date')


# --- UnionDB -----------------------------------------------------------

def test_union_set_forwards_the_index_to_the_front_db():
    """ Writing through a union writes to the front db, index and all.

    The union keeps no index of its own: the value lives on the object,
    in the db that holds it, and the query reads it back from there.
    """
    db = kydb.connect('memory://uidx_fwd_1;memory://uidx_fwd_2')
    front, back = db.dbs

    db.set('/signups/anna', {'student': 'anna'},
           index={'class_date': SATURDAY})

    assert roster(front, SATURDAY) == ['anna']
    assert list(back.folder('/signups', allow_scan=True)
                .by('class_date')) == []
    assert roster(db, SATURDAY) == ['anna']


def test_union_merges_on_the_class_date_not_the_booking_time():
    """ The merge key is what the members sorted by.

    Both members are written in an order that is not the class order,
    and the bookings interleave across the two dbs -- so merging on
    ``mtime`` would produce a result that is sorted within each member
    and unsorted across them.
    """
    db = kydb.connect('memory://uidx_merge_1;memory://uidx_merge_2')
    front, back = db.dbs

    # Written newest-class-first, alternating between the two dbs.
    front.set('/signups/cara', {'s': 'cara'}, index={'class_date': MONDAY})
    back.set('/signups/bob', {'s': 'bob'}, index={'class_date': SUNDAY})
    front.set('/signups/anna', {'s': 'anna'}, index={'class_date': SATURDAY})
    back.set('/signups/dave', {'s': 'dave'}, index={'class_date': SATURDAY})

    entries = list(by_class_date(db).asc().entries())

    assert [e.index_value for e in entries] == \
        [SATURDAY, SATURDAY, SUNDAY, MONDAY]
    assert [e.key for e in entries] == ['anna', 'dave', 'bob', 'cara']

    descending = list(by_class_date(db).desc().entries())
    assert [e.index_value for e in descending] == \
        [MONDAY, SUNDAY, SATURDAY, SATURDAY]


def test_union_propagates_until_to_every_member():
    """ A bound the members do not apply is a bound not applied at all.

    ``since(d).until(d)`` would quietly widen to ``since(d)`` if
    ``until`` stopped at the union.
    """
    db = kydb.connect('memory://uidx_until_1;memory://uidx_until_2')
    front, back = db.dbs

    front.set('/signups/anna', {'s': 'anna'}, index={'class_date': SATURDAY})
    front.set('/signups/cara', {'s': 'cara'}, index={'class_date': MONDAY})
    back.set('/signups/bob', {'s': 'bob'}, index={'class_date': SUNDAY})
    back.set('/signups/dave', {'s': 'dave'}, index={'class_date': MONDAY})

    assert roster(db, SATURDAY) == ['anna']
    assert roster(db, SUNDAY) == ['bob']
    assert roster(db, MONDAY) == ['cara', 'dave']
    assert sorted(by_class_date(db).since(SATURDAY).until(SUNDAY)) == \
        ['anna', 'bob']


def test_union_front_db_wins_on_a_user_index():
    """ A name in both members is served by the front db -- with the
    front db's index value, not whichever sorted first. """
    db = kydb.connect('memory://uidx_dedup_1;memory://uidx_dedup_2')
    front, back = db.dbs

    back.set('/signups/anna', {'s': 'anna', 'from': 'back'},
             index={'class_date': MONDAY})
    front.set('/signups/anna', {'s': 'anna', 'from': 'front'},
              index={'class_date': SATURDAY})

    entries = list(by_class_date(db).asc().entries())
    assert [(e.key, e.index_value) for e in entries] == \
        [('anna', SATURDAY)]
    assert dict(by_class_date(db).asc().items())['anna']['from'] == 'front'


def test_union_dedup_happens_after_each_member_applies_its_bounds():
    """ A documented limitation, not a new one.

    Front-db-wins is resolved among the entries the members *return*,
    and each member applies the bounds itself. So a bounded query that
    excludes the front db's copy of a name lets a shadowed back-db copy
    through -- here the Monday roster lists anna, even though
    ``db['/signups/anna']`` is the front db's Saturday booking.

    Resolving ownership first would cost a full folder listing per
    member on every union query, including a cheap ``recent(limit=10)``,
    and the same hole is reachable with ``by('mtime').since(...)`` on a
    union whose back db holds a newer copy -- it predates user indexes.
    It is recorded in ``user_index_plan.md`` §11 rather than papered
    over here; the honest fix is per-member ownership resolution, which
    is its own change.
    """
    db = kydb.connect('memory://uidx_dedup_b1;memory://uidx_dedup_b2')
    front, back = db.dbs

    back.set('/signups/anna', {'s': 'anna', 'from': 'back'},
             index={'class_date': MONDAY})
    front.set('/signups/anna', {'s': 'anna', 'from': 'front'},
              index={'class_date': SATURDAY})

    assert roster(db, SATURDAY) == ['anna']
    assert roster(db, MONDAY) == ['anna']
    assert db['/signups/anna']['from'] == 'front'


def test_union_uses_only_the_members_that_can_hold_index_values(tmp_path):
    """ Partial support, the same rule ``by('mtime')`` already follows.

    Files has nowhere to store a caller-supplied value, so it is skipped
    for ``by('class_date')`` rather than raising -- while still serving
    ``by('mtime')`` as it always did.
    """
    files_url = 'files:/' + str(tmp_path / 'uidx_partial')
    db = kydb.connect('memory://uidx_partial_mem;' + files_url)
    mem, files = db.dbs

    mem.set('/signups/anna', {'s': 'anna'}, index={'class_date': SATURDAY})
    files['/signups/bob'] = {'s': 'bob'}

    assert roster(db, SATURDAY) == ['anna']
    assert list(by_class_date(db).asc()) == ['anna']

    # bob is in the folder and in the mtime ordering, just on no roster.
    assert set(db.ls('/signups')) == {'anna', 'bob'}
    assert set(db.folder('/signups', allow_scan=True).by('mtime')) == \
        {'anna', 'bob'}


def test_union_with_no_indexable_member_raises(tmp_path):
    """ Nothing in the union could ever have stored the value, so the
    honest answer is to say so rather than return an empty roster. """
    a = 'files:/' + str(tmp_path / 'uidx_none_a')
    b = 'files:/' + str(tmp_path / 'uidx_none_b')
    db = kydb.connect(a + ';' + b)
    db.dbs[0]['/signups/anna'] = {'s': 'anna'}

    with pytest.raises(IndexNotSupported) as excinfo:
        list(by_class_date(db).asc())

    assert 'class_date' in str(excinfo.value)

    # mtime is unaffected -- Files serves that from the filesystem.
    assert list(db.folder('/signups', allow_scan=True).by('mtime')) == \
        ['anna']


def test_union_set_with_an_index_raises_when_the_front_db_cannot(tmp_path):
    """ The write goes to the front db, so the front db's answer is the
    union's answer -- and a dropped index value must never be silent. """
    files_url = 'files:/' + str(tmp_path / 'uidx_front_files')
    db = kydb.connect(files_url + ';memory://uidx_front_mem')

    with pytest.raises(IndexNotSupported):
        db.set('/signups/anna', {'s': 'anna'},
               index={'class_date': SATURDAY})

    assert not db.exists('/signups/anna')


def test_union_recent_is_unaffected():
    """ ``recent()`` still merges on write time. """
    db = kydb.connect('memory://uidx_recent_1;memory://uidx_recent_2')
    front, back = db.dbs

    front.set('/signups/cara', {'s': 'cara'}, index={'class_date': MONDAY})
    back.set('/signups/bob', {'s': 'bob'}, index={'class_date': SUNDAY})
    front.set('/signups/anna', {'s': 'anna'}, index={'class_date': SATURDAY})

    assert list(db.recent('/signups', allow_scan=True)) == \
        ['anna', 'bob', 'cara']


# --- CacheDB -----------------------------------------------------------

def _cache_db(name: str):
    return kydb.connect(f'memory://{name}_cache|memory://{name}_persist')


def test_cache_set_records_the_index_on_persist_db_only():
    db = _cache_db('uidx_cache')

    db.set('/signups/anna', {'s': 'anna'}, index={'class_date': SATURDAY})

    assert roster(db.persist_db, SATURDAY) == ['anna']
    assert list(db.cache_db.folder('/signups', allow_scan=True)
                .by('class_date')) == []

    # The object itself is in both, as it always was.
    assert db.cache_db['/signups/anna'] == {'s': 'anna'}
    assert db.persist_db['/signups/anna'] == {'s': 'anna'}


def test_cache_folder_query_answers_from_persist_db():
    db = _cache_db('uidx_cache_q')

    db.set('/signups/anna', {'s': 'anna'}, index={'class_date': SATURDAY})
    db.set('/signups/bob', {'s': 'bob'}, index={'class_date': MONDAY})

    assert roster(db, SATURDAY) == ['anna']
    assert roster(db, MONDAY) == ['bob']
    assert dict(by_class_date(db).asc().items())['anna'] == {'s': 'anna'}


def test_cache_set_without_an_index_still_writes_both():
    db = _cache_db('uidx_cache_plain')

    db.set('/signups/anna', {'s': 'anna'})

    assert db.cache_db['/signups/anna'] == {'s': 'anna'}
    assert db.persist_db['/signups/anna'] == {'s': 'anna'}
    assert list(by_class_date(db).asc()) == []


def test_cache_set_rejects_an_index_the_persist_db_cannot_store(tmp_path):
    files_url = 'files:/' + str(tmp_path / 'uidx_cache_files')
    db = kydb.connect('memory://uidx_cache_files_cache|' + files_url)

    with pytest.raises(IndexNotSupported):
        db.set('/signups/anna', {'s': 'anna'},
               index={'class_date': SATURDAY})

    # persist_db is written first, so its refusal leaves nothing cached
    # to hide the failure behind.
    assert not db.cache_db.exists('/signups/anna')
    assert not db.persist_db.exists('/signups/anna')


# --- DbObj -------------------------------------------------------------

def _objdb(url: str):
    db = kydb.connect(url)
    db.upload_objdb_config(DBOBJ_CONFIG)
    return db


def test_a_dbobj_can_carry_an_index_value():
    """ ``write_dbobj`` pickles the object itself rather than going
    through ``_serialise``, so the index has to be threaded through that
    path explicitly -- otherwise an indexed DbObj write would succeed and
    silently land on no roster. """
    db = _objdb('memory://uidx_dbobj')

    greeter = db.new('Greeter', '/signups/anna')
    greeter.name.setvalue('Anna')
    db.set(greeter.key, greeter, index={'class_date': SATURDAY})

    assert roster(db, SATURDAY) == ['anna']

    back = db.read('/signups/anna', reload=True)
    assert back.name() == 'Anna'
    assert back.greet() == 'Hello Anna'


def test_a_dbobj_rewrite_preserves_its_index_value():
    db = _objdb('memory://uidx_dbobj_rewrite')

    greeter = db.new('Greeter', '/signups/anna')
    db.set(greeter.key, greeter, index={'class_date': SATURDAY})

    greeter.name.setvalue('Anna')
    greeter.write()                      # plain __setitem__, no index=

    assert roster(db, SATURDAY) == ['anna']
    assert db.read('/signups/anna', reload=True).name() == 'Anna'


def test_a_dbobj_index_value_can_be_cleared():
    db = _objdb('memory://uidx_dbobj_clear')

    greeter = db.new('Greeter', '/signups/anna')
    db.set(greeter.key, greeter, index={'class_date': SATURDAY})
    db.set(greeter.key, greeter, index={'class_date': None})

    assert roster(db, SATURDAY) == []
    assert 'anna' in db.ls('/signups')


def test_a_dbobj_index_value_goes_to_persist_db_through_a_cache():
    """ ``write_dbobj`` calls ``obj.db.set_raw`` -- and for a CacheDB
    that is the wrapper's own ``set_raw``, which must route the index to
    persist_db exactly as ``set`` does. """
    db = _objdb('memory://uidx_dbobj_cache|memory://uidx_dbobj_persist')

    greeter = db.new('Greeter', '/signups/anna')
    greeter.name.setvalue('Anna')
    db.set(greeter.key, greeter, index={'class_date': SATURDAY})

    assert roster(db.persist_db, SATURDAY) == ['anna']
    assert list(db.cache_db.folder('/signups', allow_scan=True)
                .by('class_date')) == []
    assert db.read('/signups/anna').name() == 'Anna'


def test_a_dbobj_index_is_still_refused_where_it_cannot_be_stored(tmp_path):
    db = _objdb('files:/' + str(tmp_path / 'uidx_dbobj_files'))

    greeter = db.new('Greeter', '/signups/anna')
    with pytest.raises(IndexNotSupported):
        db.set(greeter.key, greeter, index={'class_date': SATURDAY})

    assert not db.exists('/signups/anna')
