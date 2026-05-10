"""Tests for tag normalization, validation, and JobInstanceMetadata integration."""
import pytest

from runtools.runcore.job import (
    InstanceID,
    JobInstanceMetadata,
    normalize_tag,
    normalize_tags,
    validate_tag,
)


# --- normalize_tag ---

@pytest.mark.parametrize("raw,expected", [
    ("assistant", "assistant"),
    ("  assistant  ", "assistant"),
    ("Assistant", "assistant"),
    ("#assistant", "assistant"),
    ("# assistant", "assistant"),
    ("  #assistant  ", "assistant"),
    ("env/prod", "env/prod"),
    ("v1.2.3", "v1.2.3"),
    ("PR-23", "pr-23"),
])
def test_normalize_tag_round_trips(raw, expected):
    assert normalize_tag(raw) == expected


def test_normalize_tag_is_idempotent():
    once = normalize_tag("  #FooBar  ")
    twice = normalize_tag(once)
    assert once == twice == "foobar"


@pytest.mark.parametrize("bad", [None, 42, ["assistant"], object()])
def test_normalize_tag_rejects_non_string(bad):
    with pytest.raises(ValueError, match="must be a string"):
        normalize_tag(bad)


# --- validate_tag ---

@pytest.mark.parametrize("tag", [
    "a",                  # single char
    "ab",                 # two char
    "assistant",
    "env/prod",
    "v1.2.3",
    "tag-with-dashes",
    "tag_with_underscores",
    "ns/sub.path-x_y",
    "0",                  # leading digit OK
    "a" * 64,             # exactly max length
])
def test_validate_tag_accepts(tag):
    validate_tag(tag)


@pytest.mark.parametrize("tag,reason", [
    ("",            "empty"),
    ("a" * 65,      "too long"),
    ("Assistant",   "uppercase"),
    ("has space",   "whitespace"),
    ("-leading",    "leading punctuation"),
    (".leading",    "leading punctuation"),
    ("trailing-",   "trailing punctuation"),
    ("trailing.",   "trailing punctuation"),
    ("trailing/",   "trailing punctuation"),
    ("trailing_",   "trailing punctuation"),
    ("with#hash",   "embedded special"),
    ("with!bang",   "embedded special"),
    ("with@at",     "embedded special"),
    ("café",        "non-ASCII"),
])
def test_validate_tag_rejects(tag, reason):
    with pytest.raises(ValueError):
        validate_tag(tag)


# --- normalize_tags (plural) ---

def test_normalize_tags_dedupes_preserving_order():
    assert normalize_tags(["foo", "bar", "foo", "baz", "bar"]) == ("foo", "bar", "baz")


def test_normalize_tags_dedupes_after_normalization():
    """Tags that differ only in pre-normalization form collapse to one."""
    assert normalize_tags(["#Foo", "foo", "  FOO  "]) == ("foo",)


def test_normalize_tags_returns_frozen_tuple():
    result = normalize_tags(["a", "b"])
    assert isinstance(result, tuple)


def test_normalize_tags_propagates_invalid():
    with pytest.raises(ValueError):
        normalize_tags(["valid", "INVALID with space"])


def test_normalize_tags_empty_iterable():
    assert normalize_tags([]) == ()


def test_normalize_tags_rejects_bare_string():
    """A bare str is iterable as characters — reject up-front with a clear error
    so callers don't get a confusing per-character validation failure.
    """
    with pytest.raises(ValueError, match="expects an iterable of strings"):
        normalize_tags("#assistant")


# --- JobInstanceMetadata integration ---

def test_metadata_normalizes_tags_on_construction():
    md = JobInstanceMetadata(
        instance_id=InstanceID("j", "r", 1),
        tags=("#Assistant", "ENV/prod", "assistant"),  # mixed cases + dupe
    )
    assert md.tags == ("assistant", "env/prod")


def test_metadata_round_trips_tags_through_serialize():
    md = JobInstanceMetadata(
        instance_id=InstanceID("j", "r", 1),
        tags=("a", "b"),
    )
    restored = JobInstanceMetadata.deserialize(md.serialize())
    assert restored.tags == ("a", "b")


def test_metadata_omits_tags_from_serialization_when_empty():
    md = JobInstanceMetadata(instance_id=InstanceID("j", "r", 1))
    assert "tags" not in md.serialize()


def test_metadata_rejects_invalid_tag():
    with pytest.raises(ValueError):
        JobInstanceMetadata(
            instance_id=InstanceID("j", "r", 1),
            tags=("valid", "BAD TAG"),
        )
