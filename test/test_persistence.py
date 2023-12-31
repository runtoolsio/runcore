from runtoolsio.runcore import persistence


def test_load_sqlite():
    assert persistence.load_persistence('sqlite')
