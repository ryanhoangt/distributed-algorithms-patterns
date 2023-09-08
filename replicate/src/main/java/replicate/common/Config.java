package replicate.common;

import java.io.File;

public class Config {
    private String walDir;
    private Long maxLogSegmentSize = Long.MAX_VALUE;

    public Config(String walDir) {
        this.walDir = walDir;
    }

    public File getWalDir() {
        return new File(walDir);
    }

    public Long getMaxLogSegmentSize() {
        return maxLogSegmentSize;
    }

    public long getCleanTaskIntervalMs() {
        return 1000;
    }
}
