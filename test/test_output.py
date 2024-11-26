import pytest
from runtools.runcore.output import InMemoryTailBuffer, OutputLine, Mode


def test_max_capacity():
    output = InMemoryTailBuffer(max_capacity=2)
    output.add_line(OutputLine("line1", False))
    output.add_line(OutputLine("line2", False))
    output.add_line(OutputLine("line3", False))

    lines = output.get_lines()
    assert len(lines) == 2
    assert [line.text for line in lines] == ["line2", "line3"]


def test_get_lines_with_count():
    output = InMemoryTailBuffer()
    output.add_line(OutputLine("line1", False))
    output.add_line(OutputLine("line2", False))
    output.add_line(OutputLine("line3", False))

    head_lines = output.get_lines(mode=Mode.HEAD, count=2)
    assert len(head_lines) == 2
    assert [line.text for line in head_lines] == ["line1", "line2"]

    tail_lines = output.get_lines(mode=Mode.TAIL, count=2)
    assert len(tail_lines) == 2
    assert [line.text for line in tail_lines] == ["line2", "line3"]


def test_get_all_lines():
    output = InMemoryTailBuffer()
    output.add_line(OutputLine("line1", False))
    output.add_line(OutputLine("line2", False))

    lines = output.get_lines(count=0)
    assert len(lines) == 2
    assert [line.text for line in lines] == ["line1", "line2"]


def test_negative_max_capacity():
    with pytest.raises(ValueError):
        InMemoryTailBuffer(max_capacity=-1)


def test_negative_count():
    output = InMemoryTailBuffer()
    with pytest.raises(ValueError):
        output.get_lines(count=-1)
