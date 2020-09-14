import kydb


def test_cache_context():
    db = kydb.connect('memory://unittests')
    db['global'] = 'always_here'

    assert db._cache == {'global': 'always_here'}
    with db.cache_context():
        db['foo'] = 123
        assert db._cache == {'global': 'always_here', 'foo': 123}

        with db.cache_context():
            db['bar'] = 456
            assert db._cache == {
                'global': 'always_here',
                'foo': 123,
                'bar': 456}

        assert db._cache == {'global': 'always_here', 'foo': 123}

    assert db._cache == {'global': 'always_here'}
