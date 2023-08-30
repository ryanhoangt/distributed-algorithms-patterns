package replicate.wal;

public enum EntryType {
    DATA(0),
    METADATA(1),
    CRC(2);

    private int value;

    EntryType(int value) {
        this.value = value;
    }
}
