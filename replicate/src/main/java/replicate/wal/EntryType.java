package replicate.wal;

import java.util.HashMap;
import java.util.Map;

public enum EntryType {
    DATA(0),
    METADATA(1),
    CRC(2);

    private static Map<Integer, EntryType> valueMap = new HashMap<>();
    static {
        for (EntryType entryType: EntryType.values()) {
            valueMap.put(entryType.value, entryType);
        }
    }

    private int value;

    EntryType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static EntryType valueOf(int entryType) {
        return (EntryType) valueMap.get(entryType);
    }
}
