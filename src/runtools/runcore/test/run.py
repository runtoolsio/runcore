from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from runtools.runcore.run import PhaseDetail, RunState, TerminationInfo, TerminationStatus, Fault, RunLifecycle
from runtools.runcore.util import utc_now


def term(status=TerminationStatus.COMPLETED, ended_at=None, fault=None):
    return TerminationInfo(status, ended_at or utc_now().replace(microsecond=0), fault)


def phase_detail(phase_id, run_state=RunState.EXECUTING, children=None,
                 *, phase_type='test', created_at=None, started_at=None, termination):
    if not created_at:
        created_at = utc_now().replace(microsecond=0)
    return PhaseDetail(phase_id, phase_type, run_state, '', None, None, RunLifecycle(created_at, started_at, termination), children or tuple())


def _determine_container_state(children: Tuple[PhaseDetail,...]) -> tuple[bool, Optional[TerminationInfo]]:
    """Determine container state based on children"""
    if not children:
        return False, None

    # Container is started if any child is started
    started = any(child.lifecycle.started_at is not None for child in children)

    # Get termination info only if all children are terminated
    all_terminated = all(child.lifecycle.termination is not None for child in children)
    if not all_terminated:
        return started, None

    # Get latest termination time from children, ensuring timezone consistency
    termination_times = []
    for child in children:
        terminated_at = child.lifecycle.termination.terminated_at
        # Convert timezone-aware datetimes to naive by removing tzinfo
        if terminated_at.tzinfo is not None:
            terminated_at = terminated_at.replace(tzinfo=None)
        termination_times.append(terminated_at)

    latest_termination = max(termination_times)

    # If any child failed/stopped, container gets same status
    for child in children:
        if child.lifecycle.termination.status != TerminationStatus.COMPLETED:
            return started, TerminationInfo(
                status=child.lifecycle.termination.status,
                terminated_at=latest_termination,
                message=child.lifecycle.termination.message,
                stack_trace=child.lifecycle.termination.stack_trace,
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
            _termination=None,  # Termination determined by children
            _is_container=True
        )

    def add_phase(
            self,
            phase_id: str,
            run_state: RunState = RunState.EXECUTING,
            term_status: Optional[TerminationStatus] = None,
            *,
            phase_type: str = "test",
            started: bool = False,
            fault: Optional[Fault] = None,
            term_ts: Optional[datetime] = None
    ) -> 'FakePhaseDetailBuilder':
        """
        Add a child phase with flexible termination configuration

        Args:
            phase_id: Unique identifier for the phase
            run_state: Current execution state
            phase_type: Type of the phase
            started: Whether the phase has started
            term_status: Optional termination status
            fault: Optional fault information
            term_ts: Optional specific termination date (if None, uses offset-based timing)

        Returns:
            Self for method chaining
        """
        offset = self._next_offset
        self._next_offset = offset + 3

        if term_status is not None:
            started = True

        # Calculate termination info if status is provided
        termination_info = None
        if term_status is not None:
            # Use provided term_date if available, otherwise calculate based on offset
            terminated_at = term_ts if term_ts is not None else (self._base_ts + timedelta(minutes=offset + 2))
            termination_info = TerminationInfo(
                status=term_status,
                terminated_at=terminated_at,
                message=None,
                stack_trace=fault.stack_trace if fault else None,
            )

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

    def build(self) -> PhaseDetail:
        """Build the PhaseDetail hierarchy with sequential timestamps"""
        # Build children first to determine container state
        built_children = tuple(child.build() for child in self.children)

        # For root, use base_ts directly. For others, calculate offset
        if self.parent is None:  # Root phase
            created_at = self._base_ts
            started_at = self._base_ts
        else:
            offset = self._next_offset - 3
            created_at = self._base_ts + timedelta(minutes=offset)
            started_at = created_at + timedelta(minutes=1) if self._started else None

        # For containers, determine state based on children
        if self._is_container:
            self._started, self._termination = _determine_container_state(built_children)

        return PhaseDetail(
            phase_id=self.phase_id,
            phase_type=self.phase_type,
            run_state=self.run_state,
            phase_name=self.phase_id,
            attributes=None,
            variables=None,
            lifecycle=RunLifecycle(created_at=created_at, started_at=started_at, termination=self._termination),
            children=built_children
        )