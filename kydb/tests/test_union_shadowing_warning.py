""" The union shadowing limitation is logged, not fixed.

``UnionFolderQuery`` merges each member's already-filtered entries and
resolves front-db-wins among what they return. A key held by two members
can therefore appear in a bounded result even when the front db's own
copy -- the authoritative one, the one ``db[key]`` returns -- falls
outside the bounds.

Fixing it costs a read per surviving row at best and a full folder
listing per member at worst, on every union query including a
``recent(limit=10)`` that is currently cheap. The decision is to keep
the query fast and tell the caller, so these tests pin the telling: the
warning fires exactly when the shape can bite, stays quiet when it
cannot, and does not repeat itself into a polling loop.
"""
import logging

import pytest

import kydb

SATURDAY = 20260905
MONDAY = 20260907

FOLDER = '/signups/'


@pytest.fixture
def union_with_disagreeing_copies(request):
    """ Two memory dbs holding ``anna`` on different days.

    The front db's copy is the authoritative one: ``union['/signups/anna']``
    returns it, because ``read`` is ``first_success_db_func``.
    """
    case = request.node.name
    front = kydb.connect(f'memory://shadow_front_{case}')
    back = kydb.connect(f'memory://shadow_back_{case}')
    union = kydb.connect(
        f'memory://shadow_front_{case};memory://shadow_back_{case}')

    front.set(FOLDER + 'anna', {'s': 'anna', 'day': SATURDAY},
              index={'class_date': SATURDAY})
    back.set(FOLDER + 'anna', {'s': 'anna', 'day': MONDAY},
             index={'class_date': MONDAY})
    back.set(FOLDER + 'yuki', {'s': 'yuki', 'day': MONDAY},
             index={'class_date': MONDAY})

    try:
        yield union
    finally:
        for db in (front, back):
            try:
                db.rm_tree(FOLDER)
            except KeyError:
                pass


def roster(union, day):
    return sorted(union.folder(FOLDER, allow_scan=True)
                  .by('class_date').since(day).until(day))


# --- the limitation itself, asserted so it cannot change silently ------

def test_the_authoritative_copy_is_the_front_dbs(
        union_with_disagreeing_copies):
    union = union_with_disagreeing_copies
    assert union[FOLDER + 'anna']['day'] == SATURDAY


def test_a_shadowed_copy_still_leaks_into_a_bounded_roster(
        union_with_disagreeing_copies):
    """ The documented wrong answer.

    ``anna`` is on Monday's roster even though her authoritative booking
    is Saturday, because the front db's Saturday copy was filtered out
    before it could suppress the back db's Monday one. Asserted rather
    than left implicit: if someone closes this hole, this test should
    fail and be deleted on purpose.
    """
    union = union_with_disagreeing_copies
    assert roster(union, SATURDAY) == ['anna']
    assert roster(union, MONDAY) == ['anna', 'yuki']


def test_an_unbounded_query_is_not_affected(union_with_disagreeing_copies):
    """ With no bounds every member returns every row, so front-db-wins
    is complete and ``anna`` appears once, on her real day.
    """
    union = union_with_disagreeing_copies
    entries = list(union.folder(FOLDER, allow_scan=True)
                   .by('class_date').asc().entries())
    by_key = {e.key: e.index_value for e in entries}
    assert by_key == {'anna': SATURDAY, 'yuki': MONDAY}


# --- the warning -------------------------------------------------------

def test_a_bounded_union_query_warns(union_with_disagreeing_copies, caplog):
    union = union_with_disagreeing_copies
    with caplog.at_level(logging.WARNING, logger='kydb.union'):
        roster(union, MONDAY)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert FOLDER in message
    assert 'class_date' in message
    # It must point at the documentation rather than read as a transient
    # error someone should retry through.
    assert 'user_index_plan.md' in message


def test_the_warning_names_the_actual_hazard(
        union_with_disagreeing_copies, caplog):
    with caplog.at_level(logging.WARNING, logger='kydb.union'):
        roster(union_with_disagreeing_copies, MONDAY)

    message = caplog.records[0].getMessage()
    assert 'front' in message and 'bounds' in message
    assert 'db[key]' in message


def test_an_unbounded_query_does_not_warn(
        union_with_disagreeing_copies, caplog):
    """ ``recent(limit=10)`` is the common union query and is correct --
    warning on it would be crying wolf.
    """
    union = union_with_disagreeing_copies
    with caplog.at_level(logging.WARNING, logger='kydb.union'):
        list(union.folder(FOLDER, allow_scan=True).by('class_date').asc())
        list(union.recent(FOLDER, limit=10, allow_scan=True))

    assert [r for r in caplog.records if r.levelno == logging.WARNING] == []


def test_a_single_member_union_does_not_warn(caplog):
    """ One member cannot shadow anything. """
    kydb.connect('memory://shadow_solo')
    union = kydb.connect('memory://shadow_solo')
    union.set(FOLDER + 'anna', {'s': 'anna'}, index={'class_date': SATURDAY})
    try:
        with caplog.at_level(logging.WARNING, logger='kydb.union'):
            roster(union, SATURDAY)
        assert [r for r in caplog.records
                if r.levelno == logging.WARNING] == []
    finally:
        union.rm_tree(FOLDER)


def test_the_warning_does_not_repeat_for_the_same_query_shape(
        union_with_disagreeing_copies, caplog):
    """ Once per (folder, index) per union, so a polling loop is not
    buried in duplicates.
    """
    union = union_with_disagreeing_copies
    with caplog.at_level(logging.WARNING, logger='kydb.union'):
        for _ in range(5):
            roster(union, MONDAY)
            roster(union, SATURDAY)

    assert len([r for r in caplog.records
                if r.levelno == logging.WARNING]) == 1


def test_mtime_bounds_warn_too(union_with_disagreeing_copies, caplog):
    """ The hole predates user indexes -- ``by('mtime').since()`` reaches
    it whenever a back db holds a newer copy -- so the warning is not
    specific to a business key.
    """
    union = union_with_disagreeing_copies
    with caplog.at_level(logging.WARNING, logger='kydb.union'):
        list(union.folder(FOLDER, allow_scan=True).by('mtime').since(1))

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert 'mtime' in warnings[0].getMessage()


def test_every_union_index_query_is_logged_at_debug(
        union_with_disagreeing_copies, caplog):
    """ The unbounded case is still worth a trace when someone is
    debugging a union result they did not expect.
    """
    union = union_with_disagreeing_copies
    with caplog.at_level(logging.DEBUG, logger='kydb.union'):
        list(union.folder(FOLDER, allow_scan=True).by('class_date').asc())

    debugs = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert len(debugs) == 1
    message = debugs[0].getMessage()
    assert 'class_date' in message and 'members=2' in message


def test_kydb_configures_no_handlers():
    """ A library must not decide where its logs go. """
    assert logging.getLogger('kydb.union').handlers == []
    assert logging.getLogger('kydb').handlers == []
