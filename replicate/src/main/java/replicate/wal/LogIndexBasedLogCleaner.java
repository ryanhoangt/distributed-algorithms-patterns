package replicate.wal;

import replicate.common.Config;

import java.util.List;

public class LogIndexBasedLogCleaner extends LogCleaner {

    public LogIndexBasedLogCleaner(Config config, WriteAheadLog wal) {
        super(config, wal);
    }

    @Override
    protected List<WALSegment> getSegmentsToBeDeleted() {
        // TODO:

        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
