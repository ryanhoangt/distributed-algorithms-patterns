package replicate.wal;

import java.nio.ByteBuffer;

public class WALEntry {
    private final Long entryIndex;
    private final byte[] data;
    private final EntryType entryType;
    private final long timeStamp;

    public WALEntry(Long entryIndex, byte[] data, EntryType entryType) {
        this.entryIndex = entryIndex;
        this.data = data;
        this.entryType = entryType;
        this.timeStamp = System.currentTimeMillis();
    }

    public WALEntry(byte[] data) {
        this(-1L, data, EntryType.DATA);
    }

    public ByteBuffer toByteBuffer() {
        int entryBytes = serializedBytes();
        int bufferSize = getLogEntrySize();
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize); // Big Endian
        buffer.clear(); // unflip the buffer
        buffer.putInt(entryBytes);
        buffer.putInt(entryType.getValue());
        buffer.putLong(entryIndex);
        buffer.putLong(timeStamp);
        buffer.put(data);
        return buffer;
    }

    /**
     * Get the total size of the entry being persisted on disk.
     * Calculate: 4 bytes for size + size of serialized entry.
     */
    public Integer getLogEntrySize() {
        return WriteAheadLog.INT_SIZE_BYTES + serializedBytes();
    }

    private int serializedBytes() {
        return data.length +                        // size of data
                WriteAheadLog.LONG_SIZE_BYTES +     // size of index
                WriteAheadLog.LONG_SIZE_BYTES +     // size of timestamp
                WriteAheadLog.LONG_SIZE_BYTES;      // size of entry type
    }

    public Long getEntryIndex() {
        return entryIndex;
    }

    public byte[] getData() {
        return data;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
}
