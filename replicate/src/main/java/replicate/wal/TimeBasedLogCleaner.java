package replicate.wal;

import replicate.common.Config;

import java.util.ArrayList;
import java.util.List;

public class TimeBasedLogCleaner extends LogCleaner {
    
    public TimeBasedLogCleaner(Config config, WriteAheadLog wal) {
        super(config, wal);
    }

    @Override
    protected List<WALSegment> getSegmentsToBeDeleted() {
        return getSegmentsPast(config.getLogMaxDurationMs());
    }

    private List<WALSegment> getSegmentsPast(Long logMaxDurationMs) {
        long now = System.currentTimeMillis();
        List<WALSegment> toBeDeleted = new ArrayList<>();

        for (WALSegment sortedSegment: wal.getSortedSavedSegments()) {
            if (timeElapsedSince(now, sortedSegment.getLastLogEntryTimestamp()) > logMaxDurationMs)
                toBeDeleted.add(sortedSegment);
        }
        return toBeDeleted;
    }

    private long timeElapsedSince(long now, long lastLogEntryTimestamp) {
        return now - lastLogEntryTimestamp;
    }
}
