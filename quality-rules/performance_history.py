from collections import deque

from pipeline_metrics import PerformanceSnapshot


class PerformanceHistory:
    """Store recent pipeline performance snapshots."""

    def __init__(self, max_size: int = 100):
        if max_size < 1:
            raise ValueError("max_size must be at least 1")

        self.snapshots = deque(maxlen=max_size)

    def add_snapshot(
        self,
        snapshot: PerformanceSnapshot,
    ) -> None:
        """Add a performance snapshot to history."""

        self.snapshots.append(snapshot)

    def get_history(self) -> list[PerformanceSnapshot]:
        """Return snapshots in chronological order."""

        return list(self.snapshots)

    def get_latest(self) -> PerformanceSnapshot | None:
        """Return the most recent snapshot."""

        if not self.snapshots:
            return None

        return self.snapshots[-1]

    def clear(self) -> None:
        """Clear all stored performance snapshots."""

        self.snapshots.clear()

    def get_size(self) -> int:
        """Return the number of stored snapshots."""

        return len(self.snapshots)