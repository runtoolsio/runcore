import pytest

from runtools.runcore.job import InstanceID


class TestConstruction:

    def test_defaults(self):
        iid = InstanceID('job1')
        assert iid.job_id == 'job1'
        assert iid.run_id  # auto-generated
        assert iid.ordinal == 1

    def test_explicit_ordinal(self):
        iid = InstanceID('j', 'r', 3)
        assert iid.ordinal == 3

    def test_ordinal_zero_rejected(self):
        with pytest.raises(ValueError, match="ordinal"):
            InstanceID('j', 'r', 0)

    def test_negative_ordinal_rejected(self):
        with pytest.raises(ValueError, match="ordinal"):
            InstanceID('j', 'r', -1)

    def test_empty_job_id_rejected(self):
        with pytest.raises(ValueError, match="job_id"):
            InstanceID('')

    def test_forbidden_chars_rejected(self):
        with pytest.raises(ValueError):
            InstanceID('j@b')


class TestLogicalKey:

    def test_logical_key(self):
        iid = InstanceID('j', 'r', 2)
        assert iid.logical_key == ('j', 'r')

    def test_same_logical_key_different_ordinals(self):
        a = InstanceID('j', 'r', 1)
        b = InstanceID('j', 'r', 2)
        assert a.logical_key == b.logical_key
        assert a != b


class TestStrAndParse:

    def test_str_ordinal_1_omitted(self):
        assert str(InstanceID('j', 'r', 1)) == 'j@r'

    def test_str_ordinal_shown(self):
        assert str(InstanceID('j', 'r', 2)) == 'j@r:2'

    def test_parse_without_ordinal(self):
        iid = InstanceID.parse('j@r')
        assert iid == InstanceID('j', 'r', 1)

    def test_parse_with_ordinal(self):
        iid = InstanceID.parse('j@r:3')
        assert iid == InstanceID('j', 'r', 3)

    def test_round_trip_ordinal_1(self):
        original = InstanceID('job', 'run', 1)
        assert InstanceID.parse(str(original)) == original

    def test_round_trip_ordinal_gt_1(self):
        original = InstanceID('job', 'run', 5)
        assert InstanceID.parse(str(original)) == original

    def test_parse_empty_raises(self):
        with pytest.raises(ValueError):
            InstanceID.parse('')

    def test_parse_no_at_raises(self):
        with pytest.raises(ValueError):
            InstanceID.parse('nope')

    def test_parse_invalid_ordinal_raises(self):
        with pytest.raises(ValueError, match="ordinal"):
            InstanceID.parse('j@r:abc')


class TestSerialize:

    def test_ordinal_1_omitted(self):
        assert InstanceID('j', 'r', 1).serialize() == {"job_id": "j", "run_id": "r"}

    def test_ordinal_included_when_gt_1(self):
        assert InstanceID('j', 'r', 2).serialize() == {"job_id": "j", "run_id": "r", "ordinal": 2}

    def test_round_trip(self):
        for ordinal in (1, 2, 10):
            original = InstanceID('j', 'r', ordinal)
            assert InstanceID.deserialize(original.serialize()) == original

    def test_deserialize_missing_ordinal(self):
        iid = InstanceID.deserialize({"job_id": "j", "run_id": "r"})
        assert iid.ordinal == 1


class TestEquality:

    def test_equal(self):
        assert InstanceID('j', 'r', 1) == InstanceID('j', 'r', 1)

    def test_different_ordinal(self):
        assert InstanceID('j', 'r', 1) != InstanceID('j', 'r', 2)

    def test_different_run_id(self):
        assert InstanceID('j', 'r1') != InstanceID('j', 'r2')

    def test_hash_same(self):
        assert hash(InstanceID('j', 'r', 1)) == hash(InstanceID('j', 'r', 1))

    def test_hash_different_ordinal(self):
        assert hash(InstanceID('j', 'r', 1)) != hash(InstanceID('j', 'r', 2))
