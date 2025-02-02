from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from runtools.runcore.run import PhaseDetail, RunState, TerminationInfo, TerminationStatus
from runtools.runcore.util import utc_now


def term(status=TerminationStatus.COMPLETED, ended_at=None, fault=None):
    return TerminationInfo(status, ended_at or utc_now().replace(microsecond=0), fault)


def phase_detail(phase_id, run_state=RunState.EXECUTING, children=None,
                 *, phase_type='test', created_at=None, started_at=None, term):
    if not created_at:
        created_at = utc_now().replace(microsecond=0)
    return PhaseDetail(phase_id, phase_type, run_state, '', None, created_at, started_at, term, children or [])


def _determine_container_state(children: List[PhaseDetail]) -> tuple[bool, Optional[TerminationInfo]]:
    """Determine container state based on children"""
    if not children:
        return False, None

    # Container is started if any child is started
    started = any(child.started_at is not None for child in children)

    # Get termination info only if all children are terminated
    all_terminated = all(child.termination is not None for child in children)
    if not all_terminated:
        return started, None

    # Get latest termination time from children, ensuring timezone consistency
    termination_times = []
    for child in children:
        terminated_at = child.termination.terminated_at
        # Convert timezone-aware datetimes to naive by removing tzinfo
        if terminated_at.tzinfo is not None:
            terminated_at = terminated_at.replace(tzinfo=None)
        termination_times.append(terminated_at)

    latest_termination = max(termination_times)

    # If any child failed/stopped, container gets same status
    for child in children:
        if child.termination.status != TerminationStatus.COMPLETED:
            return started, TerminationInfo(
                status=child.termination.status,
                terminated_at=latest_termination,
                fault=child.termination.fault
            )

    # All children completed successfully
    return started, TerminationInfo(
        status=TerminationStatus.COMPLETED,
        terminated_at=latest_termination
    )


@dataclass
class FakePhaseDetailBuilder:
    """Builder for creating test phase hierarchies with sequential timestamps"""
    phase_id: str
    run_state: RunState
    phase_type: str
    parent: Optional['FakePhaseDetailBuilder']
    children: List['FakePhaseDetailBuilder']
    _base_ts: datetime
    _next_offset: int  # Minutes from base timestamp
    _started: bool
    _termination: Optional[TerminationInfo]
    _is_container: bool

    @classmethod
    def root(cls, phase_id: str = "root", run_state: RunState = RunState.EXECUTING_CHILDREN,
             *, phase_type: str = "test", base_ts: Optional[datetime] = None) -> 'FakePhaseDetailBuilder':
        """Create a root builder with initial timestamp"""
        base_ts = base_ts or utc_now().replace(microsecond=0)
        return cls(
            phase_id=phase_id,
            run_state=run_state,
            phase_type=phase_type,
            parent=None,
            children=[],
            _base_ts=base_ts,
            _next_offset=0,
            _started=True,  # Root phase is started by default
            _termination=TerminationInfo(  # And completed
                status=TerminationStatus.COMPLETED,
                terminated_at=base_ts + timedelta(minutes=1)
            ),
            _is_container=True
        )

    def add_phase(self, phase_id: str, run_state: RunState = RunState.EXECUTING,
                  termination_info: Optional[TerminationInfo] = None,
                  *, phase_type: str = "test", started: bool = False) -> 'FakePhaseDetailBuilder':
        """Add a child phase and return self for chaining"""
        offset = self._next_offset
        self._next_offset = offset + 3

        if termination_info:
            started = True

        child = FakePhaseDetailBuilder(
            phase_id=phase_id,
            run_state=run_state,
            phase_type=phase_type,
            parent=self,
            children=[],
            _base_ts=self._base_ts,
            _next_offset=offset + 3,
            _started=started,
            _termination=termination_info,
            _is_container=False
        )
        self.children.append(child)
        return self

    def add_container_phase(self, phase_id: str, run_state: RunState = RunState.EXECUTING_CHILDREN,
                            *, phase_type: str = "test") -> 'FakePhaseDetailBuilder':
        """Add a container phase and return the new builder for adding children to it"""
        offset = self._next_offset
        self._next_offset = offset + 3

        child = FakePhaseDetailBuilder(
            phase_id=phase_id,
            run_state=run_state,
            phase_type=phase_type,
            parent=self,
            children=[],
            _base_ts=self._base_ts,
            _next_offset=offset + 3,
            _started=False,  # Will be determined by children
            _termination=None,  # Will be determined by children
            _is_container=True
        )
        self.children.append(child)
        return child

    def end(self) -> 'FakePhaseDetailBuilder':
        """Return to parent builder for chaining"""
        if self.parent:
            self.parent._next_offset = max(self.parent._next_offset, self._next_offset)
            return self.parent
        return self

    def build(self) -> PhaseDetail:
        """Build the PhaseDetail hierarchy with sequential timestamps"""
        # Build children first to determine container state
        built_children = [child.build() for child in self.children]

        offset = self._next_offset - 3
        created_at = self._base_ts + timedelta(minutes=offset)

        # For containers, determine state based on children
        if self._is_container:
            self._started, self._termination = _determine_container_state(built_children)

        started_at = created_at + timedelta(minutes=1) if self._started else None

        return PhaseDetail(
            phase_id=self.phase_id,
            phase_type=self.phase_type,
            run_state=self.run_state,
            phase_name=self.phase_id,
            attributes=None,
            created_at=created_at,
            started_at=started_at,
            termination=self._termination,
            children=built_children
        )
