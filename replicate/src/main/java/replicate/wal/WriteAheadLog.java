package replicate.wal;

import replicate.common.Config;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class WriteAheadLog {
    static int INT_SIZE_BYTES = 4;
    static int LONG_SIZE_BYTES = 8;

    private Config config;
    private WALSegment openingSegment;
    /**
     * Segments are kept sorted in ascending order of log indexes,
     * so that it easier to traverse them to find specific entry.
     */
    private List<WALSegment> sortedSavedSegments;

    private WriteAheadLog(List<WALSegment> segmentsSortedByIndex, Config config) {
        this.sortedSavedSegments = segmentsSortedByIndex;
        this.config = config;
        this.openingSegment = sortedSavedSegments.remove(sortedSavedSegments.size() - 1);
    }

    public static WriteAheadLog openWAL(Config config) {
        return new WriteAheadLog(openAllSegments(config.getWalDir()), config);
    }

    private static List<WALSegment> openAllSegments(File walDir) {
        List<WALSegment> segments = new ArrayList<>();
        File[] walFiles = walDir.listFiles();

        assert walFiles != null;
        for (File walFile: walFiles)
            segments.add(WALSegment.open(walFile));

        if (segments.isEmpty())
            segments.add(WALSegment.open(0L, walDir));

        segments.sort(Comparator.comparing(WALSegment::getBaseOffset));
        return segments;
    }

    public Long writeEntry(byte[] data) {
        long curLogEntryId = openingSegment.getLastLogEntryIndex() + 1;
        WALEntry curLogEntry = new WALEntry(curLogEntryId, data, EntryType.DATA);
        return writeEntry(curLogEntry);
    }

    public synchronized Long writeEntry(WALEntry entry) {
        rollIfNecessary();

        return openingSegment.writeEntry(entry);
    }

    private void rollIfNecessary() {
        if (openingSegment.size() >= config.getMaxLogSegmentSize()) {
            openingSegment.flush();
            sortedSavedSegments.add(openingSegment);
            long lastId = openingSegment.getLastLogEntryIndex();
            openingSegment = WALSegment.open(lastId, config.getWalDir());
        }
    }

    public List<WALEntry> readAll() {
        List<WALEntry> walEntries = new ArrayList<>();
        for (WALSegment segment: sortedSavedSegments) {
            walEntries.addAll(segment.readAllEntries());
        }
        walEntries.addAll(openingSegment.readAllEntries());
        return walEntries;
    }
}
