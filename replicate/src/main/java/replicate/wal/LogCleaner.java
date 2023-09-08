package replicate.wal;

import replicate.common.Config;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class LogCleaner {
    private Config config;
    private WriteAheadLog wal;
    private ScheduledExecutorService singleThreadedExecutor = Executors.newScheduledThreadPool(1);

    public LogCleaner(Config config, WriteAheadLog wal) {
        this.config = config;
        this.wal = wal;
    }

    public void startUp() {
        scheduleLogCleaning();
    }

    protected abstract List<WALSegment> getSegmentsToBeDeleted();

    private void scheduleLogCleaning() {
        singleThreadedExecutor.schedule(this::cleanLogs, config.getCleanTaskIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void cleanLogs() {
        List<WALSegment> segmentsToBeDeleted = getSegmentsToBeDeleted();
        for (WALSegment walSegment: segmentsToBeDeleted)
            wal.removeAndDeleteSegment(walSegment);

        scheduleLogCleaning();
    }
}
