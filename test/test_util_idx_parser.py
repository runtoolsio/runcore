from runtools.runcore.util import IndexParser


def test_parsing():
    sut = IndexParser({'timestamp': 0, 'level': 1, 'message': slice(2, None)})

    result = sut("2024-03-19T10:30:00 INFO User login successful")

    assert result['timestamp'] == '2024-03-19T10:30:00'
    assert result['level'] == 'INFO'
    assert result['message'] == 'User login successful'


def test_strip():
    sut = IndexParser({'timestamp': 0, 'level': 1, 'message': slice(2, None)}, strip_chars="[]")

    result = sut("[2024-03-19T10:30:00] INFO [User login successful]")

    assert result['timestamp'] == '2024-03-19T10:30:00'
    assert result['level'] == 'INFO'
    assert result['message'] == 'User login successful'
