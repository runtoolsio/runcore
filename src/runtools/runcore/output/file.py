"""File-backed output backend — JSONL storage with source indexing.

Read-side module contract:
    create_backend(env_id, config) -> FileOutputBackend
"""

import json
import logging
import os
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from runtools.runcore import paths
from runtools.runcore.output import OutputBackend, OutputLine, OutputReadError, OutputStorageConfig

log = logging.getLogger(__name__)


class FileOutputStorageConfig(BaseModel):
    """File backend config — validated internally from the generic OutputStorageConfig extras."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    dir: Optional[Path] = Field(default=None, description="Base output directory; XDG default if None")


def _resolve_base_dir(env_id: str, config: OutputStorageConfig, *, create: bool = False) -> Path:
    """Validate file-specific config and resolve the base directory."""
    file_cfg = FileOutputStorageConfig.model_validate(config.model_extra or {})
    return Path(file_cfg.dir).expanduser() if file_cfg.dir else paths.output_dir(env_id, create=create)


_TAIL_CHUNK_SIZE = 64 * 1024


class SourceIndex:
    """Immutable source index mapping source IDs to byte-offset spans in a JSONL file."""

    VERSION = 1

    def __init__(self, sources: dict[str, list[list[int]]], jsonl_size: int):
        self.sources = sources
        self.jsonl_size = jsonl_size

    @staticmethod
    def path_for(jsonl_path: Path) -> Path:
        return Path(str(jsonl_path) + ".idx")

    @classmethod
    def load(cls, jsonl_path: Path) -> 'SourceIndex | None':
        """Load and validate a source index. Returns None if missing, corrupt, or stale."""
        try:
            with open(cls.path_for(jsonl_path), encoding="utf-8") as f:
                data = json.load(f)
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None
        if data.get("version") != cls.VERSION:
            return None
        try:
            if jsonl_path.stat().st_size != data.get("jsonl_size"):
                return None
        except OSError:
            return None
        jsonl_size = data.get("jsonl_size")
        if not isinstance(jsonl_size, int):
            return None
        return cls(data.get("sources", {}), jsonl_size)

    def save(self, jsonl_path: Path):
        """Atomically write the index file."""
        index_path = self.path_for(jsonl_path)
        tmp_path = Path(str(index_path) + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"version": self.VERSION, "jsonl_size": self.jsonl_size, "sources": self.sources},
                      f, separators=(",", ":"))
        os.replace(tmp_path, index_path)

    def spans_for(self, sources: set[str]) -> list[list[int]]:
        """Collect spans for requested sources, sorted by byte offset for correct line order."""
        spans = []
        for source in sources:
            if source in self.sources:
                spans.extend(self.sources[source])
        spans.sort(key=lambda s: s[0])
        return spans


class SourceIndexBuilder:
    """Accumulates byte-offset spans per source during writing."""

    def __init__(self):
        self._byte_offset = 0
        self._source_spans: dict[str, list[list[int]]] = {}
        self._current_source: str | None = None
        self._current_span_start = 0
        self._current_span_length = 0

    def track(self, source: str | None, byte_len: int):
        if source != self._current_source:
            self._flush_span()
            self._current_source = source
            self._current_span_start = self._byte_offset
            self._current_span_length = 0

        if source is not None:
            self._current_span_length += byte_len

        self._byte_offset += byte_len

    def build(self) -> SourceIndex | None:
        """Flush and produce the immutable index. Returns None if no sources were tracked."""
        self._flush_span()
        if not self._source_spans:
            return None
        return SourceIndex(self._source_spans, self._byte_offset)

    def _flush_span(self):
        if self._current_source is not None and self._current_span_length > 0:
            spans = self._source_spans.setdefault(self._current_source, [])
            spans.append([self._current_span_start, self._current_span_length])


class FileOutputBackend(OutputBackend):
    """File-backed output backend. Reads JSONL files from ``{base_dir}/{job_id}/{run_id}__{ordinal}.jsonl``."""

    type = "file"

    def __init__(self, base_dir: Path):
        self._base_dir = base_dir

    def _output_path(self, instance_id) -> Path:
        return self._base_dir / instance_id.job_id / f"{instance_id.run_id}__{instance_id.ordinal}.jsonl"

    def read_output(self, instance_id, sources: set[str] | None = None,
                    max_lines: int = 0) -> List[OutputLine]:
        path = self._output_path(instance_id)
        try:
            if max_lines > 0:
                if sources is not None:
                    return _read_jsonl_indexed_tail(path, sources, max_lines)
                return _read_jsonl_tail(path, max_lines)
            if sources is not None:
                return _read_jsonl_indexed(path, sources)
            return read_jsonl_file(str(path))
        except FileNotFoundError:
            return []
        except (json.JSONDecodeError, KeyError, OSError) as e:
            raise OutputReadError(str(path), e) from e

    def delete_output(self, *instance_ids) -> None:
        for iid in instance_ids:
            path = self._output_path(iid)
            path.unlink(missing_ok=True)
            SourceIndex.path_for(path).unlink(missing_ok=True)
            try:
                path.parent.rmdir()
            except OSError:
                pass


def create_backend(env_id: str, config: OutputStorageConfig) -> FileOutputBackend:
    """Module-level factory (read side) — part of the output backend module contract."""
    return FileOutputBackend(_resolve_base_dir(env_id, config))


# ---------------------------------------------------------------------------
# JSONL read helpers
# ---------------------------------------------------------------------------

def read_jsonl_file(file_path: str) -> List[OutputLine]:
    """Read output lines from a JSON Lines file."""
    lines = []
    with open(file_path, encoding="utf-8") as f:
        for raw_line in f:
            raw_line = raw_line.strip()
            if raw_line:
                lines.append(OutputLine.deserialize(json.loads(raw_line)))
    lines.sort(key=lambda ol: ol.ordinal)
    return lines


def _read_jsonl_tail(path: Path, max_lines: int) -> List[OutputLine]:
    if max_lines <= 0:
        return []
    file_size = path.stat().st_size
    if file_size == 0:
        return []
    lines = _read_tail_bytes(path, 0, file_size, max_lines)
    lines.reverse()
    return lines


def _read_jsonl_indexed_tail(jsonl_path: Path, sources: set[str],
                             max_lines: int) -> List[OutputLine]:
    if max_lines <= 0:
        return []

    index = SourceIndex.load(jsonl_path)
    if index is None:
        all_filtered = _read_jsonl_filtered(jsonl_path, sources)
        return all_filtered[-max_lines:]

    spans = index.spans_for(sources)
    if not spans:
        return []

    try:
        lines = []
        for offset, length in reversed(spans):
            chunk_lines = _read_tail_bytes(jsonl_path, offset, length, max_lines - len(lines))
            lines.extend(chunk_lines)
            if len(lines) >= max_lines:
                break
        lines = lines[:max_lines]
        lines.reverse()
        return lines
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        log.warning("Indexed tail read failed, falling back to full scan",
                    extra={"path": str(jsonl_path)}, exc_info=True)
        all_filtered = _read_jsonl_filtered(jsonl_path, sources)
        return all_filtered[-max_lines:]


def _read_tail_bytes(path: Path, start: int, length: int, max_lines: int) -> List[OutputLine]:
    """Read the last max_lines from byte range [start, start+length). Returns reverse-chronological."""
    lines = []
    end = start + length

    with open(path, "rb") as f:
        read_end = end
        leftover = b""

        while read_end > start and len(lines) < max_lines:
            chunk_start = max(start, read_end - _TAIL_CHUNK_SIZE)
            f.seek(chunk_start)
            chunk = f.read(read_end - chunk_start) + leftover

            raw_lines = chunk.split(b"\n")

            if chunk_start > start:
                leftover = raw_lines[0]
                raw_lines = raw_lines[1:]
            else:
                leftover = b""

            for raw_line in reversed(raw_lines):
                if len(lines) >= max_lines:
                    break
                raw_line = raw_line.strip()
                if raw_line:
                    lines.append(OutputLine.deserialize(json.loads(raw_line)))

            read_end = chunk_start

        if leftover and len(lines) < max_lines:
            leftover = leftover.strip()
            if leftover:
                lines.append(OutputLine.deserialize(json.loads(leftover)))

    return lines


def _read_jsonl_indexed(jsonl_path: Path, sources: set[str]) -> List[OutputLine]:
    index = SourceIndex.load(jsonl_path)
    if index is None:
        return _read_jsonl_filtered(jsonl_path, sources)

    spans = index.spans_for(sources)
    if not spans:
        return []

    try:
        lines = []
        with open(jsonl_path, "rb") as f:
            for offset, length in spans:
                f.seek(offset)
                chunk = f.read(length).decode("utf-8")
                for raw_line in chunk.splitlines():
                    if raw_line:
                        lines.append(OutputLine.deserialize(json.loads(raw_line)))
        return lines
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        log.warning("Indexed read failed, falling back to full scan",
                    extra={"path": str(jsonl_path)}, exc_info=True)
        return _read_jsonl_filtered(jsonl_path, sources)


def _read_jsonl_filtered(path: Path, sources: set[str]) -> List[OutputLine]:
    """Fallback: read entire file and filter by source."""
    lines = []
    with open(path, encoding="utf-8") as f:
        for raw_line in f:
            if raw_line := raw_line.strip():
                ol = OutputLine.deserialize(json.loads(raw_line))
                if ol.source in sources:
                    lines.append(ol)
    return lines
