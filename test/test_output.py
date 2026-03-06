import json
from pathlib import Path

import pytest

from runtools.runcore.output import (
    SourceIndex, SourceIndexBuilder, OutputLine, read_jsonl_file,
    _read_jsonl_indexed, _read_jsonl_filtered,
)


# --- SourceIndexBuilder ---

class TestSourceIndexBuilder:

    def test_single_source_produces_one_span(self):
        builder = SourceIndexBuilder()
        builder.track("EXEC", 100)
        builder.track("EXEC", 200)
        index = builder.build()
        assert index.sources == {"EXEC": [[0, 300]]}
        assert index.jsonl_size == 300

    def test_alternating_sources_produce_separate_spans(self):
        builder = SourceIndexBuilder()
        builder.track("EXEC", 50)
        builder.track("SETUP", 30)
        builder.track("EXEC", 40)
        index = builder.build()
        assert index.sources == {"EXEC": [[0, 50], [80, 40]], "SETUP": [[50, 30]]}

    def test_none_source_not_tracked(self):
        builder = SourceIndexBuilder()
        builder.track(None, 10)
        builder.track("EXEC", 20)
        builder.track(None, 15)
        index = builder.build()
        assert index.sources == {"EXEC": [[10, 20]]}
        assert index.jsonl_size == 45

    def test_none_between_same_source_flushes_span(self):
        builder = SourceIndexBuilder()
        builder.track("EXEC", 10)
        builder.track(None, 5)
        builder.track("EXEC", 10)
        index = builder.build()
        assert index.sources == {"EXEC": [[0, 10], [15, 10]]}

    def test_no_sources_returns_none(self):
        builder = SourceIndexBuilder()
        assert builder.build() is None

    def test_only_none_sources_returns_none(self):
        builder = SourceIndexBuilder()
        builder.track(None, 100)
        builder.track(None, 200)
        assert builder.build() is None


# --- SourceIndex ---

class TestSourceIndex:

    def test_path_for(self):
        assert SourceIndex.path_for(Path("/tmp/out.jsonl")) == Path("/tmp/out.jsonl.idx")

    def test_spans_for_returns_sorted_by_offset(self):
        index = SourceIndex({"A": [[100, 50], [0, 30]], "B": [[50, 50]]}, 200)
        spans = index.spans_for({"A", "B"})
        offsets = [s[0] for s in spans]
        assert offsets == sorted(offsets)

    def test_spans_for_missing_source_returns_empty(self):
        index = SourceIndex({"A": [[0, 10]]}, 10)
        assert index.spans_for({"MISSING"}) == []

    def test_save_and_load_roundtrip(self, tmp_path):
        jsonl_path = tmp_path / "test.jsonl"
        jsonl_path.write_bytes(b"x" * 42)

        original = SourceIndex({"EXEC": [[0, 20], [30, 12]]}, 42)
        original.save(jsonl_path)

        loaded = SourceIndex.load(jsonl_path)
        assert loaded is not None
        assert loaded.sources == original.sources
        assert loaded.jsonl_size == 42

    def test_load_returns_none_when_missing(self, tmp_path):
        assert SourceIndex.load(tmp_path / "nonexistent.jsonl") is None

    def test_load_returns_none_when_stale(self, tmp_path):
        jsonl_path = tmp_path / "test.jsonl"
        jsonl_path.write_bytes(b"x" * 42)

        index = SourceIndex({"A": [[0, 42]]}, 42)
        index.save(jsonl_path)

        # Modify the JSONL file so size no longer matches
        jsonl_path.write_bytes(b"x" * 100)

        assert SourceIndex.load(jsonl_path) is None

    def test_load_returns_none_on_corrupt_json(self, tmp_path):
        jsonl_path = tmp_path / "test.jsonl"
        jsonl_path.write_bytes(b"x" * 10)
        SourceIndex.path_for(jsonl_path).write_text("not json")

        assert SourceIndex.load(jsonl_path) is None

    def test_load_returns_none_on_wrong_version(self, tmp_path):
        jsonl_path = tmp_path / "test.jsonl"
        jsonl_path.write_bytes(b"x" * 10)
        SourceIndex.path_for(jsonl_path).write_text(
            json.dumps({"version": 999, "jsonl_size": 10, "sources": {}})
        )

        assert SourceIndex.load(jsonl_path) is None


# --- Indexed and filtered reads ---

def _write_jsonl(path, lines):
    """Write OutputLine instances as JSONL."""
    with open(path, "wb") as f:
        builder = SourceIndexBuilder()
        for line in lines:
            raw = (json.dumps(line.serialize(), ensure_ascii=False) + "\n").encode("utf-8")
            builder.track(line.source, len(raw))
            f.write(raw)
    if index := builder.build():
        index.save(path)


class TestIndexedRead:

    @pytest.fixture
    def jsonl_with_index(self, tmp_path):
        path = tmp_path / "test.jsonl"
        lines = [
            OutputLine("setup line", 1, source="SETUP"),
            OutputLine("exec line 1", 2, source="EXEC"),
            OutputLine("exec line 2", 3, source="EXEC"),
            OutputLine("teardown", 4, source="TEARDOWN"),
        ]
        _write_jsonl(path, lines)
        return path

    def test_indexed_read_filters_by_source(self, jsonl_with_index):
        result = _read_jsonl_indexed(jsonl_with_index, {"EXEC"})
        assert len(result) == 2
        assert all(line.source == "EXEC" for line in result)

    def test_indexed_read_multiple_sources(self, jsonl_with_index):
        result = _read_jsonl_indexed(jsonl_with_index, {"SETUP", "TEARDOWN"})
        assert {line.source for line in result} == {"SETUP", "TEARDOWN"}

    def test_indexed_read_missing_source_returns_empty(self, jsonl_with_index):
        assert _read_jsonl_indexed(jsonl_with_index, {"NONEXISTENT"}) == []

    def test_filtered_read_matches_indexed_read(self, jsonl_with_index):
        indexed = _read_jsonl_indexed(jsonl_with_index, {"EXEC"})
        filtered = _read_jsonl_filtered(jsonl_with_index, {"EXEC"})
        assert [line.ordinal for line in indexed] == [line.ordinal for line in filtered]

    def test_falls_back_to_filtered_when_no_index(self, tmp_path):
        path = tmp_path / "no_index.jsonl"
        lines = [
            OutputLine("a", 1, source="A"),
            OutputLine("b", 2, source="B"),
        ]
        with open(path, "w") as f:
            for line in lines:
                f.write(json.dumps(line.serialize()) + "\n")

        result = _read_jsonl_indexed(path, {"A"})
        assert len(result) == 1
        assert result[0].source == "A"


class TestReadJsonlFile:

    def test_returns_all_lines_sorted_by_ordinal(self, tmp_path):
        path = tmp_path / "all.jsonl"
        lines = [
            OutputLine("second", 2, source="X"),
            OutputLine("first", 1, source="Y"),
        ]
        with open(path, "w") as f:
            for line in lines:
                f.write(json.dumps(line.serialize()) + "\n")

        result = read_jsonl_file(str(path))
        assert [line.ordinal for line in result] == [1, 2]
