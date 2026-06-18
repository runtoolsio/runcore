"""Database-backed instance discovery — transport-neutral.

Finds an environment's active runs by querying the environment database (the rows kept
current by the node-side run-state persister) instead of contacting the producing nodes.
This is the discovery mechanism for transports without their own live presence channel
(e.g. a shared-database transport); socket transports use RPC discovery, which is live
and authoritative, and should not use this.
"""

from runtools.runcore.db import RunStorage
from runtools.runcore.transport import DiscoveredRuns


class DbInstanceDiscovery:
    """Discover active runs from the environment database.

    A database sweep has a single authoritative source, so the result is always
    ``complete``. Snapshots lag the persister's flush interval and, after a producer
    crash, may name runs that are no longer running — expiring those is a reconciler's
    concern, not discovery's.
    """

    def __init__(self, db: RunStorage):
        self._db = db

    def discover_active_runs(self, run_match=None) -> DiscoveredRuns:
        """Run one discovery sweep against the database."""
        return DiscoveredRuns(self._db.read_active_runs(run_match), complete=True)
