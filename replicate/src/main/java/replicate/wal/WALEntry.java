package replicate.wal;

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
}
