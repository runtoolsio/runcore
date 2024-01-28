from runtoolsio.runcore import db


def test_load_sqlite():
    assert db.load_database_module('sqlite')
