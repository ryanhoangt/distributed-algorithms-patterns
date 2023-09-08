package replicate.wal;

import replicate.common.Config;

import java.util.List;

public class TimeBasedLogCleaner extends LogCleaner {
    
    public TimeBasedLogCleaner(Config config, WriteAheadLog wal) {
        super(config, wal);
    }

    @Override
    protected List<WALSegment> getSegmentsToBeDeleted() {
        // TODO:

        throw new UnsupportedOperationException("Not implemented yet.");
    }

}
