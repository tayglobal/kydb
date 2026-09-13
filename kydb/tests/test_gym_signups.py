""" The gym use case, end to end -- the acceptance test for user indexes.

``user_index_plan.md`` §1.1 states the problem in one sentence: a gym
books students into classes, a signup written on Tuesday is for
Saturday's 18:30 HIIT class, and the question the front desk asks every
morning is *"who is booked into today's classes?"*.

``mtime`` cannot answer it. It records Tuesday. This file books a real
week of classes -- bookings written **out of order**, and all of them on
a different day from the class itself -- and then asks both questions of
the same folder:

* ``by('class_date')`` -- when the class *is*: today's roster.
* ``recent()`` / ``by('mtime')`` -- when the booking *happened*: who
  signed up most recently.

The two must not agree, and every test here is written so that an
implementation which quietly ordered by ``mtime`` would **fail** rather
than merely look odd.

It runs against every backend that can store user index values -- Memory,
Redis and DynamoDB -- because the whole point of the design is that the
answers are identical on all three.
"""
import os

import pytest

import kydb


def _db_types():
    env_val = os.environ.get('KYDB_TEST_DB_TYPES')
    if env_val:
        return [x.strip() for x in env_val.split(',') if x.strip()]
    return ['memory', 's3', 'redis', 'dynamodb', 'files', 'union']


ALL_DB_TYPES = _db_types()

#: Every backend with ``supports_user_index = True``.
INDEXED_DB_TYPES = ['memory', 'redis', 'dynamodb']

DB_HOSTS = {
    'memory': 'memory://gym',
    'redis': 'redis://{}:6379'.format(
        os.environ.get('KINYU_UNITTEST_REDIS_HOST', 'localhost')),
    'dynamodb': 'dynamodb://' + os.environ.get(
        'KINYU_UNITTEST_DYNAMODB', 'kydb-test-table'),
}


# --- the week's classes ------------------------------------------------
#
# The front desk is taking bookings on Tuesday 1 September for the coming
# weekend. Every class_date below is days away from the day the booking
# is written, and the write order matches none of them.

SATURDAY = 20260905      # HIIT, 18:30
SUNDAY = 20260906        # Yoga, 09:00
MONDAY = 20260907        # Strength, 07:00

BOOKED_ON = 20260901     # the Tuesday every one of these was taken

#: (student, class_date), in the order the front desk took them. Note
#: that this order is neither sorted by class_date nor grouped by it: it
#: is the order the phone rang, which is exactly what ``mtime`` records
#: and exactly what a roster must not be ordered by.
BOOKINGS = [
    ('priya', MONDAY),
    ('anna', SATURDAY),
    ('daniel', SUNDAY),
    ('marcus', SATURDAY),
    ('yuki', MONDAY),
    ('sofia', SUNDAY),
]

#: Booked no class at all -- a drop-in who bought a day pass at the desk.
#: Present in the folder, absent from every roster.
WALK_IN = 'hugo'


def booking(student: str, class_date: int) -> dict:
    return {
        'student': student,
        'class_date': class_date,
        'booked_on': BOOKED_ON,
        'paid': True,
    }


@pytest.fixture(params=INDEXED_DB_TYPES)
def gym(request, local_backends):
    """ A db that stores user index values, with an empty ``/signups``.

    The db's *base path* carries the isolation (one per backend per
    test), so the keys inside a test read the way an application would
    really write them -- ``/signups/anna``, not
    ``/unittests/gym_redis_test_x/signups/anna``.
    """
    db_type = request.param
    if db_type not in ALL_DB_TYPES:
        pytest.skip(f'{db_type} not in KYDB_TEST_DB_TYPES')

    case = request.node.name.replace('[', '_').replace(']', '')
    db = kydb.connect(f'{DB_HOSTS[db_type]}/unittests/gym/{case}')

    def clear():
        try:
            db.rm_tree('/signups')
        except KeyError:
            pass

    clear()
    try:
        yield db
    finally:
        clear()


def take_the_weeks_bookings(db):
    """ Write every booking, in the order the phone rang. """
    for student, class_date in BOOKINGS:
        db.set(f'/signups/{student}', booking(student, class_date),
               index={'class_date': class_date})

    db['/signups/' + WALK_IN] = {
        'student': WALK_IN, 'booked_on': BOOKED_ON, 'day_pass': True}


def roster(db, day: int):
    """ Who is booked into ``day``'s classes -- one indexed query.

    ``allow_scan=True`` is what MemoryDB requires to opt into its
    client-side sort; Redis and DynamoDB serve this natively and ignore
    it, so one call serves all three.
    """
    return sorted(db.folder('/signups', allow_scan=True)
                  .by('class_date').since(day).until(day))


def signups(db, **chain):
    """ The whole ``/signups`` folder ordered by ``class_date``. """
    query = db.folder('/signups', allow_scan=True).by('class_date')
    if chain.get('descending'):
        query = query.desc()
    return query


# --- the question the front desk asks every morning --------------------

def test_todays_roster_is_one_indexed_query(gym):
    """ ``since(d).until(d)`` -- both bounds inclusive -- is the day. """
    db = gym
    take_the_weeks_bookings(db)

    assert roster(db, SATURDAY) == ['anna', 'marcus']
    assert roster(db, SUNDAY) == ['daniel', 'sofia']
    assert roster(db, MONDAY) == ['priya', 'yuki']


def test_the_weekend_is_a_closed_range(gym):
    """ Two days, one query: ``since(sat).until(sun)``. """
    db = gym
    take_the_weeks_bookings(db)

    weekend = sorted(db.folder('/signups', allow_scan=True)
                     .by('class_date').since(SATURDAY).until(SUNDAY))
    assert weekend == ['anna', 'daniel', 'marcus', 'sofia']


def test_a_day_with_no_classes_booked_is_empty_not_an_error(gym):
    db = gym
    take_the_weeks_bookings(db)

    assert roster(db, 20260908) == []


# --- the assertion an mtime implementation cannot pass ------------------

def test_the_roster_is_ordered_by_class_date_not_by_booking_time(gym):
    """ The heart of the feature.

    Every booking was written on the same Tuesday, in an order that is
    neither the class order nor grouped by class. So the two orderings
    of the *same folder* are genuinely different sequences, and a
    backend that served ``by('class_date')`` from its ``mtime`` index
    would fail here rather than return something subtly wrong.
    """
    db = gym
    take_the_weeks_bookings(db)

    class_dates_of = {student: day for student, day in BOOKINGS}

    by_class_date = [e.key for e in signups(db).asc().entries()]
    by_booking_time = [
        name for name in db.folder('/signups', allow_scan=True)
        .by('mtime').asc() if name != WALK_IN]

    # Ordered by class_date, the days come out sorted...
    assert [class_dates_of[n] for n in by_class_date] == \
        [SATURDAY, SATURDAY, SUNDAY, SUNDAY, MONDAY, MONDAY]

    # ...and ordered by booking time they emphatically do not.
    assert [class_dates_of[n] for n in by_booking_time] == \
        [day for _student, day in BOOKINGS]
    assert by_booking_time == [student for student, _day in BOOKINGS]

    # Which is the whole point: the two orderings disagree.
    assert by_class_date != by_booking_time


def test_an_entry_carries_both_the_class_date_and_a_real_booking_time(gym):
    """ ``index_value`` and ``mtime`` are separate slots on purpose.

    A roster row still knows when the booking was actually taken, so
    ``entries()`` is not made to lie about one to report the other.
    """
    db = gym
    take_the_weeks_bookings(db)

    entries = list(signups(db).asc().entries())
    class_dates_of = {student: day for student, day in BOOKINGS}

    assert [e.index_value for e in entries] == \
        sorted(class_dates_of[e.key] for e in entries)

    for entry in entries:
        assert entry.index_value == class_dates_of[entry.key]
        # A real nanosecond write time from Tuesday, nothing like the
        # 8-digit business date it is ordered by.
        assert entry.mtime > 10 ** 15
        assert entry.ctime > 0

    # And the booking times run in the order the phone rang, which the
    # class_date ordering has scrambled.
    mtimes = {e.key: e.mtime for e in entries}
    assert [mtimes[student] for student, _day in BOOKINGS] == \
        sorted(mtimes.values())


def test_ordering_runs_both_ways(gym):
    db = gym
    take_the_weeks_bookings(db)

    ascending = [e.index_value for e in signups(db).asc().entries()]
    descending = [e.index_value
                  for e in signups(db, descending=True).entries()]

    assert ascending == sorted(ascending)
    assert descending == sorted(ascending, reverse=True)


# --- strictly sparse: the walk-in is on no roster ----------------------

@pytest.mark.parametrize('direction', ['asc', 'desc'])
def test_a_student_with_no_class_date_is_never_on_a_roster(gym, direction):
    """ No epoch tail. ``hugo`` bought a day pass and booked no class.

    He is not a signup for an infinitely distant past class; he is not a
    signup at all, so he appears in no ``class_date`` query in either
    direction -- bounded or unbounded -- even though he is plainly in
    the folder and has a perfectly real ``mtime``.
    """
    db = gym
    take_the_weeks_bookings(db)

    query = signups(db)
    query = query.asc() if direction == 'asc' else query.desc()

    assert WALK_IN not in list(query)
    assert WALK_IN not in list(query.since(0))
    assert WALK_IN not in list(query.until(99999999))
    assert WALK_IN not in roster(db, SATURDAY)

    # He is in the folder, and recent() -- which asks when a thing was
    # written, not what it means -- does find him.
    assert WALK_IN in db.ls('/signups')
    assert WALK_IN in list(db.recent('/signups', allow_scan=True))


def test_a_limit_is_not_padded_with_unbooked_students(gym):
    """ ``limit(6)`` over six booked students and one walk-in returns
    six -- the walk-in cannot make up the numbers. """
    db = gym
    take_the_weeks_bookings(db)

    names = list(signups(db).asc().limit(6))
    assert len(names) == 6
    assert WALK_IN not in names


# --- rebooking: one write, and the rosters move ------------------------

def test_rebooking_moves_a_student_between_days(gym):
    """ Marcus cannot make Saturday and moves to Monday.

    One ``set(index=...)`` -- the same write the application was making
    anyway -- and he leaves one roster and joins the other. Under the
    path-encoded schema this was a non-atomic delete-then-set, and the
    object's identity changed with its class date; here the key
    ``/signups/marcus`` never moves.
    """
    db = gym
    take_the_weeks_bookings(db)

    assert roster(db, SATURDAY) == ['anna', 'marcus']
    assert roster(db, MONDAY) == ['priya', 'yuki']

    moved = db['/signups/marcus']
    moved['class_date'] = MONDAY
    db.set('/signups/marcus', moved, index={'class_date': MONDAY})

    assert roster(db, SATURDAY) == ['anna']
    assert roster(db, MONDAY) == ['marcus', 'priya', 'yuki']

    # One object, one key, still readable and now saying Monday.
    assert db.read('/signups/marcus', reload=True)['class_date'] == MONDAY
    assert sorted(db.ls('/signups')) == sorted(
        [student for student, _day in BOOKINGS] + [WALK_IN])


def test_editing_a_booking_without_mentioning_the_index_keeps_it(gym):
    """ The desk marks Anna as paid-by-card. She stays on Saturday.

    Preserve-on-rewrite: an index the write does not mention is left
    alone. The alternative -- an unmentioned index is cleared -- would
    mean every incidental rewrite anywhere in the application silently
    drops the booking off the roster, which is the failure mode hardest
    to notice and hardest to explain.
    """
    db = gym
    take_the_weeks_bookings(db)

    edited = db['/signups/anna']
    edited['payment'] = 'card'
    db['/signups/anna'] = edited          # no index= at all

    assert roster(db, SATURDAY) == ['anna', 'marcus']
    assert db.read('/signups/anna', reload=True)['payment'] == 'card'


def test_cancelling_takes_a_student_off_every_roster(gym):
    """ Sofia cancels but keeps her membership record.

    ``index={'class_date': None}`` is the explicit clear -- she is still
    an object in ``/signups``, and still turns up in ``recent()``, but
    she is on no roster.
    """
    db = gym
    take_the_weeks_bookings(db)

    cancelled = db['/signups/sofia']
    cancelled['class_date'] = None
    db.set('/signups/sofia', cancelled, index={'class_date': None})

    assert roster(db, SUNDAY) == ['daniel']
    assert 'sofia' not in list(signups(db).asc())
    assert 'sofia' in db.ls('/signups')
    assert 'sofia' in list(db.recent('/signups', allow_scan=True))


def test_a_cancelled_student_can_rebook(gym):
    db = gym
    take_the_weeks_bookings(db)

    db.set('/signups/sofia', booking('sofia', SUNDAY),
           index={'class_date': None})
    assert roster(db, SUNDAY) == ['daniel']

    db.set('/signups/sofia', booking('sofia', MONDAY),
           index={'class_date': MONDAY})
    assert roster(db, MONDAY) == ['priya', 'sofia', 'yuki']


def test_a_deleted_booking_leaves_no_ghost_on_the_roster(gym):
    """ An index entry outliving its object would come back as a name
    with nothing behind it, indistinguishable from a real booking. """
    db = gym
    take_the_weeks_bookings(db)

    db.delete('/signups/daniel')

    assert roster(db, SUNDAY) == ['sofia']
    assert 'daniel' not in list(signups(db).asc())


# --- and mtime still answers its own question --------------------------

def test_recent_still_answers_who_booked_most_recently(gym):
    """ ``recent()`` is untouched by the business index.

    It reports the order the bookings were *taken* -- newest first --
    which is the reverse of the write order and bears no relation to the
    class dates. The walk-in is in it, because he was written like
    anything else.
    """
    db = gym
    take_the_weeks_bookings(db)

    written_in_order = [student for student, _day in BOOKINGS] + [WALK_IN]
    assert list(db.recent('/signups', allow_scan=True)) == \
        list(reversed(written_in_order))

    # The three most recent signups, regardless of which class they are
    # for -- and they are for three different days.
    last_three = list(db.recent('/signups', limit=3, allow_scan=True))
    assert last_three == [WALK_IN, 'sofia', 'yuki']

    # Which is emphatically not the last three by class date.
    assert last_three != list(signups(db, descending=True).limit(3))


def test_the_two_questions_do_not_compete(gym):
    """ One folder, two orderings, both correct at the same time. """
    db = gym
    take_the_weeks_bookings(db)

    # "Who is in today's 18:30 HIIT class?"
    assert roster(db, SATURDAY) == ['anna', 'marcus']

    # "Who signed up in the last few minutes?" -- a different pair.
    assert list(db.recent('/signups', limit=2, allow_scan=True)) == \
        [WALK_IN, 'sofia']


def test_items_reads_the_bookings_back_in_roster_order(gym):
    """ The front desk wants the records, not just the names. """
    db = gym
    take_the_weeks_bookings(db)

    saturday = list(db.folder('/signups', allow_scan=True)
                    .by('class_date').since(SATURDAY).until(SATURDAY)
                    .items())

    assert sorted(name for name, _rec in saturday) == ['anna', 'marcus']
    for name, record in saturday:
        assert record['student'] == name
        assert record['class_date'] == SATURDAY
        assert record['booked_on'] == BOOKED_ON
