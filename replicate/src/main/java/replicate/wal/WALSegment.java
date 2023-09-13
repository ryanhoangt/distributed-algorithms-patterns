package replicate.wal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WALSegment {
    private static String LOG_SUFFIX = ".log";
    private static String LOG_PREFIX = "wal";

    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private Map<Long, Long> entryIndexToOffset = new HashMap<>();

    public WALSegment(Long startIndex, File file) {
        this.file = file;
        try {
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
            buildOffsetIndex();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void buildOffsetIndex() {
        try {
            entryIndexToOffset = new HashMap<>();
            long totalBytesRead = 0L;
            var deserializer = new WALEntryDeserializer(fileChannel);
            while (totalBytesRead < fileChannel.size()) {
                WALEntry entry = deserializer.readEntry(totalBytesRead);
                entryIndexToOffset.put(entry.getEntryIndex(), totalBytesRead);
                totalBytesRead += entry.getLogEntrySize();
            }
        } catch (IOException e) {
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
        return entryIndexToOffset.keySet().stream().max(Long::compareTo).orElse(0L);
    }

    public List<WALEntry> readAllEntries() {
        try {
            Long totalBytesRead = 0L;
            List<WALEntry> entries = new ArrayList<>();
            var deserializer = new WALEntryDeserializer(fileChannel);
            while (totalBytesRead < fileChannel.size()) {
                WALEntry entry = deserializer.readEntry(totalBytesRead);
                totalBytesRead += entry.getLogEntrySize(); // size of entry + size of int which stores length
                entries.add(entry);
            }

            return entries;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Delete the whole log segment.
     */
    public void delete() {
        try {
            fileChannel.close();
            randomAccessFile.close();
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public long getLastLogEntryTimestamp() {
        if (entryIndexToOffset.isEmpty()) return 0L;

        return readAt(getLastLogEntryIndex()).;
    }

    private WALEntry readAt(Long entryIndex) {
        Long filePosition = entryIndexToOffset.get(entryIndex);
        if (filePosition == null)
            throw new IllegalStateException("No file position available for entryIndex=" + entryIndex);

        var deserializer = new WALEntryDeserializer(fileChannel);
        return deserializer.readEntry(filePosition);
    }
}
