"""DynamoDB user-supplied index values (``user_index_plan.md``).

Everything here runs against Moto -- a real table is not required, and
every behaviour asserted is documented DynamoDB semantics rather than an
emulator quirk (plan section 8). The fixture table in ``conftest.py``
carries ``folder-class_date-index``, the one GSI these queries need.
"""
import os
import time

import boto3
import pytest

import kydb
from kydb.exceptions import IndexNotSupported


DB_URL = 'dynamodb://' + os.environ.get(
    'KINYU_UNITTEST_DYNAMODB', 'kydb-test-table')


def _require_dynamodb():
    env_val = os.environ.get('KYDB_TEST_DB_TYPES')
    types = [x.strip() for x in env_val.split(',')] if env_val else \
        ['memory', 's3', 'redis', 'dynamodb', 'files', 'union']
    if 'dynamodb' not in types:
        pytest.skip('dynamodb not in KYDB_TEST_DB_TYPES')


def get_db():
    _require_dynamodb()
    return kydb.connect(DB_URL + '/')


DAY1 = 20260905
DAY2 = 20260906
DAY3 = 20260907


def _counting(db):
    """Instrument ``query``/``get_item`` on the table.

    Returns ``(query_calls, get_item_calls, restore)``. Mirrors
    ``test_dynamodb_entries_does_not_refetch_per_item`` in
    ``test_impl.py``: the point of a projected index is that reading a
    result costs one query and zero per-item reads.
    """
    query_calls = []
    get_item_calls = []
    original_query = db.table.query
    original_get_item = db.table.get_item

    def counting_query(**kwargs):
        query_calls.append(kwargs)
        return original_query(**kwargs)

    def counting_get_item(**kwargs):
        get_item_calls.append(kwargs)
        return original_get_item(**kwargs)

    db.table.query = counting_query
    db.table.get_item = counting_get_item

    def restore():
        del db.table.query
        del db.table.get_item

    return query_calls, get_item_calls, restore


# --- write + read round trip ------------------------------------------

def test_write_and_query_round_trip():
    """The gym query: who is booked into a given day's classes."""
    db = get_db()
    folder = '/unittests/uidx_round_trip/'
    try:
        db.set(folder + 'anna', {'cls': 'hiit'}, index={'class_date': DAY1})
        db.set(folder + 'bob', {'cls': 'yoga'}, index={'class_date': DAY2})

        q = db.folder(folder).by('class_date')
        assert list(q.asc()) == ['anna', 'bob']
        assert dict(q.since(DAY1).until(DAY1).items()) == \
            {'anna': {'cls': 'hiit'}}
    finally:
        db.rm_tree(folder)


def test_index_value_is_stored_on_the_item():
    """The value is a plain attribute on the object's own item -- which
    is what makes the write a single atomic update_item.
    """
    db = get_db()
    folder = '/unittests/uidx_attribute/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        item = db.table.get_item(
            Key={'path': db._get_full_path(folder + 'anna')})['Item']
        assert int(item['class_date']) == DAY1
        # ...and it did not disturb anything else about the write.
        assert db.read(folder + 'anna', reload=True) == 1
        assert 'mtime' in item and 'ctime' in item
    finally:
        db.rm_tree(folder)


def test_folder_meta_records_carry_no_index_value():
    """Directories stay out of every index, exactly as they stay out of
    the mtime index.
    """
    db = get_db()
    folder = '/unittests/uidx_no_dir/sub/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        assert list(db.folder(folder).by('class_date')) == ['anna']

        parent = db._get_full_path('/unittests/uidx_no_dir/')
        meta = db.table.get_item(
            Key={'path': parent + '.folder-sub'})['Item']
        assert 'class_date' not in meta
    finally:
        db.rm_tree('/unittests/uidx_no_dir/')


# --- sparseness: no epoch tail (plan section 5) ------------------------

def test_user_index_is_sparse_desc():
    """An object with no value for the queried index never appears."""
    db = get_db()
    folder = '/unittests/uidx_sparse_desc/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        db[folder + 'walkin'] = 2          # no class_date at all

        query_calls, get_item_calls, restore = _counting(db)
        try:
            names = list(db.folder(folder).by('class_date').desc())
        finally:
            restore()

        assert names == ['anna']
        # Crucially: no folder-index fallback read. The mtime query does
        # one at the end of an unlimited desc() to find its epoch tail;
        # a user index has no tail, so it must not.
        assert [c['IndexName'] for c in query_calls] == \
            ['folder-class_date-index']
        assert get_item_calls == []
    finally:
        db.rm_tree(folder)


def test_user_index_is_sparse_asc():
    """asc() is where the mtime index pays its epoch-tail cost. A user
    index must skip it in this direction too -- the epoch tail would
    otherwise be emitted *first*, ahead of every real row.
    """
    db = get_db()
    folder = '/unittests/uidx_sparse_asc/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        db[folder + 'walkin'] = 2

        query_calls, _get_item_calls, restore = _counting(db)
        try:
            names = list(db.folder(folder).by('class_date').asc())
        finally:
            restore()

        assert names == ['anna']
        assert [c['IndexName'] for c in query_calls] == \
            ['folder-class_date-index']
    finally:
        db.rm_tree(folder)


def test_includes_epoch_is_false_for_a_user_index():
    """The decision itself, asserted directly: it is a correctness
    property of the query, not an artefact of a particular result set.
    """
    db = get_db()
    q = db.folder('/unittests/uidx_epoch_flag/')
    assert q.by('mtime')._includes_epoch() is True
    assert q.by('mtime').asc()._includes_epoch() is True
    assert q.by('class_date')._includes_epoch() is False
    assert q.by('class_date').asc()._includes_epoch() is False
    assert q.by('class_date').desc()._includes_epoch() is False
    assert q.by('class_date').since(0)._includes_epoch() is False


def test_mtime_query_still_sees_an_object_written_with_an_index():
    """Writing an index value must not take an object out of the recency
    index -- the two orderings are independent.
    """
    db = get_db()
    folder = '/unittests/uidx_mtime_unaffected/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        db[folder + 'walkin'] = 2
        assert sorted(db.folder(folder).by('mtime')) == ['anna', 'walkin']
    finally:
        db.rm_tree(folder)


# --- bounds -------------------------------------------------------------

def test_since_until_and_between():
    db = get_db()
    folder = '/unittests/uidx_bounds/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        db.set(folder + 'bob', 2, index={'class_date': DAY2})
        db.set(folder + 'cid', 3, index={'class_date': DAY3})

        q = db.folder(folder).by('class_date').asc()
        assert list(q) == ['anna', 'bob', 'cid']
        assert list(q.since(DAY2)) == ['bob', 'cid']
        assert list(q.until(DAY2)) == ['anna', 'bob']
        assert list(q.since(DAY2).until(DAY3)) == ['bob', 'cid']
        # The day-exact query: one between(d, d).
        assert list(q.since(DAY2).until(DAY2)) == ['bob']
        # An inverted range is empty rather than an error.
        assert list(q.since(DAY3).until(DAY1)) == []
    finally:
        db.rm_tree(folder)


def test_ordering_both_directions():
    """Written out of business order, and on a different day from the
    class -- so an mtime implementation would order these differently.
    """
    db = get_db()
    folder = '/unittests/uidx_ordering/'
    try:
        db.set(folder + 'cid', 3, index={'class_date': DAY3})
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        db.set(folder + 'bob', 2, index={'class_date': DAY2})

        q = db.folder(folder).by('class_date')
        assert list(q.asc()) == ['anna', 'bob', 'cid']
        assert list(q.desc()) == ['cid', 'bob', 'anna']
        # ...whereas mtime reports the order they were written in.
        assert list(db.folder(folder).by('mtime').asc()) == \
            ['cid', 'anna', 'bob']
    finally:
        db.rm_tree(folder)


def test_until_on_mtime_closes_the_range():
    """until() is shared, not user-index-only: on mtime it expresses
    "what changed *before* t", which since() alone could not.
    """
    db = get_db()
    folder = '/unittests/uidx_mtime_until/'
    try:
        db[folder + 'first'] = 1
        time.sleep(0.002)
        cutoff = time.time_ns()
        time.sleep(0.002)
        db[folder + 'second'] = 2

        q = db.folder(folder).by('mtime').asc()
        assert list(q.since(1).until(cutoff)) == ['first']
        assert list(q.since(cutoff)) == ['second']
    finally:
        db.rm_tree(folder)


# --- preserve on rewrite / explicit clear (plan section 4.4) ------------

def test_rewrite_without_index_preserves_the_value():
    db = get_db()
    folder = '/unittests/uidx_preserve/'
    try:
        db.set(folder + 'anna', {'v': 1}, index={'class_date': DAY1})
        db.set(folder + 'anna', {'v': 2})            # no index= at all
        db[folder + 'anna'] = {'v': 3}               # nor via __setitem__

        assert db.read(folder + 'anna', reload=True) == {'v': 3}
        assert list(db.folder(folder).by('class_date')
                    .since(DAY1).until(DAY1)) == ['anna']
    finally:
        db.rm_tree(folder)


def test_rewrite_with_a_new_value_moves_the_object():
    """Rebooking is one atomic item update, not delete-then-set."""
    db = get_db()
    folder = '/unittests/uidx_rebook/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        db.set(folder + 'anna', 1, index={'class_date': DAY2})

        q = db.folder(folder).by('class_date')
        assert list(q.since(DAY1).until(DAY1)) == []
        assert list(q.since(DAY2).until(DAY2)) == ['anna']
    finally:
        db.rm_tree(folder)


def test_none_clears_the_value_and_drops_the_row():
    db = get_db()
    folder = '/unittests/uidx_clear/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        db.set(folder + 'anna', 1, index={'class_date': None})

        item = db.table.get_item(
            Key={'path': db._get_full_path(folder + 'anna')})['Item']
        assert 'class_date' not in item
        assert list(db.folder(folder).by('class_date')) == []
        # The object itself is untouched, and still in the mtime index.
        assert db.read(folder + 'anna', reload=True) == 1
        assert list(db.folder(folder).by('mtime')) == ['anna']
    finally:
        db.rm_tree(folder)


def test_clearing_one_index_leaves_another_alone():
    db = get_db()
    folder = '/unittests/uidx_clear_one/'
    try:
        db.set(folder + 'anna', 1,
               index={'class_date': DAY1, 'rank': 7})
        db.set(folder + 'anna', 1,
               index={'class_date': None})

        item = db.table.get_item(
            Key={'path': db._get_full_path(folder + 'anna')})['Item']
        assert 'class_date' not in item
        assert int(item['rank']) == 7
    finally:
        db.rm_tree(folder)


def test_reserved_word_index_name_is_written_via_a_placeholder():
    """``status`` is a DynamoDB reserved word. It is a perfectly ordinary
    business index name, so the expression must use
    ExpressionAttributeNames rather than the bare name.
    """
    db = get_db()
    folder = '/unittests/uidx_reserved_word/'
    try:
        db.set(folder + 'anna', 1, index={'status': 3})
        item = db.table.get_item(
            Key={'path': db._get_full_path(folder + 'anna')})['Item']
        assert int(item['status']) == 3

        db.set(folder + 'anna', 1, index={'status': None})
        item = db.table.get_item(
            Key={'path': db._get_full_path(folder + 'anna')})['Item']
        assert 'status' not in item
    finally:
        db.rm_tree(folder)


# --- entries() ----------------------------------------------------------

def test_entries_expose_index_value_and_a_real_mtime():
    """A result ordered by class_date still carries a genuine mtime --
    conflating the two would make entries() lie about when the object
    was written.
    """
    db = get_db()
    folder = '/unittests/uidx_entries/'
    try:
        before = time.time_ns()
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        db.set(folder + 'bob', 2, index={'class_date': DAY2})
        after = time.time_ns()

        entries = list(db.folder(folder).by('class_date').asc().entries())
        assert [e.key for e in entries] == ['anna', 'bob']
        assert [e.index_value for e in entries] == [DAY1, DAY2]
        for e in entries:
            assert before <= e.mtime <= after
            assert e.ctime == e.mtime
            # Every DynamoDB Number arrives as a Decimal and must be cast.
            assert isinstance(e.index_value, int)
            assert isinstance(e.mtime, int)
            assert isinstance(e.ctime, int)
            assert not isinstance(e.index_value, bool)
    finally:
        db.rm_tree(folder)


def test_mtime_entries_still_report_index_value_as_mtime():
    db = get_db()
    folder = '/unittests/uidx_mtime_entries/'
    try:
        db[folder + 'anna'] = 1
        entry, = db.folder(folder).by('mtime').entries()
        assert entry.index_value == entry.mtime
    finally:
        db.rm_tree(folder)


def test_entries_does_not_refetch_per_item():
    """One query, zero per-item reads -- and, unlike the mtime case, not
    even the single folder-index tail read, because a user index has no
    epoch tail to discover.
    """
    db = get_db()
    folder = '/unittests/uidx_no_refetch/'
    try:
        for i in range(5):
            db.set(folder + f'obj{i}', i, index={'class_date': DAY1 + i})

        query_calls, get_item_calls, restore = _counting(db)
        try:
            entries = list(
                db.folder(folder).by('class_date').desc().entries())
        finally:
            restore()

        assert [e.key for e in entries] == [
            'obj4', 'obj3', 'obj2', 'obj1', 'obj0']
        assert get_item_calls == []
        assert len(query_calls) == 1
        assert query_calls[0]['IndexName'] == 'folder-class_date-index'
    finally:
        db.rm_tree(folder)


def test_limit_pages_lazily():
    db = get_db()
    folder = '/unittests/uidx_limit/'
    try:
        for i in range(5):
            db.set(folder + f'obj{i}', i, index={'class_date': DAY1 + i})

        query_calls, get_item_calls, restore = _counting(db)
        try:
            names = list(db.folder(folder).by('class_date').desc().limit(2))
        finally:
            restore()

        assert names == ['obj4', 'obj3']
        assert get_item_calls == []
        assert len(query_calls) == 1
        assert query_calls[0]['Limit'] == 2
        assert query_calls[0]['ScanIndexForward'] is False
    finally:
        db.rm_tree(folder)


# --- error paths --------------------------------------------------------

def test_reserved_and_malformed_index_names_are_rejected_on_read():
    db = get_db()
    q = db.folder('/unittests/uidx_bad_name/')
    with pytest.raises(IndexNotSupported):
        list(q.by('contents'))
    with pytest.raises(IndexNotSupported):
        list(q.by('2bad'))
    with pytest.raises(IndexNotSupported):
        list(q.by('has-a-dash'))


def test_allow_scan_cannot_serve_a_user_index():
    """folder-index projects only mtime/ctime, so a scan has nothing to
    sort a user index by. Rather than escalating allow_scan to O(n)
    per-item reads, this says what to add.
    """
    db = get_db()
    folder = '/unittests/uidx_allow_scan/'
    try:
        db.set(folder + 'anna', 1, index={'class_date': DAY1})
        # Force the scan fallback: it is only ever taken when the table
        # has no folder-time-index, which the fixture table does have.
        db._DynamoDB__has_time_index = False
        try:
            with pytest.raises(IndexNotSupported) as excinfo:
                list(db.folder(folder, allow_scan=True).by('class_date'))
        finally:
            db._DynamoDB__has_time_index = True

        msg = str(excinfo.value)
        assert 'folder-index' in msg
        assert 'folder-class_date-index' in msg
        # ...and allow_scan still serves the mtime query it exists for.
        assert list(db.folder(folder, allow_scan=True).by('mtime')) == ['anna']
    finally:
        db.rm_tree(folder)


def test_reindex_refuses_a_user_index():
    db = get_db()
    with pytest.raises(IndexNotSupported) as excinfo:
        db.reindex('/unittests/uidx_reindex/', 'class_date')

    msg = str(excinfo.value)
    assert 'class_date' in msg
    assert 'folder-class_date-index' in msg


# --- a table without the user index GSI ---------------------------------

def _table_without_user_index():
    """A table with folder-index and folder-time-index but no user index
    GSI -- i.e. every table in existence before someone adds one.
    """
    name = 'kydb-test-no-uidx-{}-{}'.format(os.getpid(), time.time_ns())
    client = boto3.client('dynamodb')
    client.create_table(
        TableName=name,
        KeySchema=[{'AttributeName': 'path', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'path', 'AttributeType': 'S'},
            {'AttributeName': 'folder', 'AttributeType': 'S'},
            {'AttributeName': 'mtime', 'AttributeType': 'N'},
        ],
        BillingMode='PAY_PER_REQUEST',
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'folder-index',
                'KeySchema': [
                    {'AttributeName': 'folder', 'KeyType': 'HASH'}],
                'Projection': {
                    'ProjectionType': 'INCLUDE',
                    'NonKeyAttributes': ['mtime', 'ctime'],
                },
            },
            {
                'IndexName': 'folder-time-index',
                'KeySchema': [
                    {'AttributeName': 'folder', 'KeyType': 'HASH'},
                    {'AttributeName': 'mtime', 'KeyType': 'RANGE'},
                ],
                'Projection': {
                    'ProjectionType': 'INCLUDE',
                    'NonKeyAttributes': ['ctime'],
                },
            },
        ],
    )
    client.get_waiter('table_exists').wait(
        TableName=name, WaiterConfig={'Delay': 1, 'MaxAttempts': 60})
    kydb.api._db_cache.clear()
    return name, client


def test_writes_succeed_without_the_gsi_and_the_query_says_what_to_add():
    """Writes never need the GSI -- update_item just sets an attribute --
    so values accumulate correctly before the index exists and are all
    there when it is added. The only symptom is at query time, and it
    must be kydb's own IndexNotSupported rather than a boto3
    ValidationException.
    """
    _require_dynamodb()
    name, client = _table_without_user_index()
    try:
        db = kydb.connect('dynamodb://' + name)
        folder = '/unittests/uidx_missing_gsi/'
        db.set(folder + 'anna', 1, index={'class_date': DAY1})

        item = db.table.get_item(
            Key={'path': db._get_full_path(folder + 'anna')})['Item']
        assert int(item['class_date']) == DAY1
        # The recency index is unaffected: it is present, so it works.
        assert list(db.folder(folder).by('mtime')) == ['anna']

        with pytest.raises(IndexNotSupported) as excinfo:
            list(db.folder(folder).by('class_date'))

        msg = str(excinfo.value)
        assert 'folder-class_date-index' in msg
        assert name in msg
        # The message must carry the full key schema, or the reader has
        # to go and look it up before they can act on it.
        assert 'folder HASH' in msg
        assert 'class_date RANGE (Number)' in msg
        assert "INCLUDE ['mtime', 'ctime']" in msg
    finally:
        kydb.api._db_cache.clear()
        client.delete_table(TableName=name)
