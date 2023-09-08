package replicate.wal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class WALSegment {
    private static String LOG_SUFFIX = ".log";
    private static String LOG_PREFIX = "wal";

    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;

    public WALSegment(Long startIndex, File file) {
        this.file = file;
        try {
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static WALSegment open(Long startIndex, File walDir) {
        File file = new File(walDir, createFileName(startIndex));
        return new WALSegment(startIndex, file);
    }

    public static WALSegment open(File walFile) {
        return new WALSegment(getBaseOffsetFromFileName(walFile.getName()), walFile);
    }

    private static String createFileName(Long startIndex) {
        return LOG_PREFIX + "_" + startIndex + LOG_SUFFIX;
    }

    public static Long getBaseOffsetFromFileName(String fileName) {
        String[] nameAndSuffix = fileName.split(LOG_SUFFIX);
        String[] prefixAndOffset = nameAndSuffix[0].split("_");

        if (prefixAndOffset[0].equals(LOG_PREFIX))
            return Long.parseLong(prefixAndOffset[1]);

        return -1L;
    }

    public synchronized long size() {
        try {
            return fileChannel.size();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Long getBaseOffset() {
        return getBaseOffsetFromFileName(file.getName());
    }

    public synchronized Long writeEntry(WALEntry logEntry) {
        writeToChannel(logEntry.toByteBuffer());
        return logEntry.getEntryIndex();
    }

    private void writeToChannel(ByteBuffer buffer) {
        try {
            buffer.flip();
            while (buffer.hasRemaining())
                fileChannel.write(buffer);
            flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Flush updates to the segment to be written to disk.
     */
    public synchronized void flush() {
        try {
            fileChannel.force(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getLastLogEntryIndex() {
        // TODO:

        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public List<WALEntry> readAllEntries() {
        // TODO:

        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public void delete() {
        // TODO:

        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
