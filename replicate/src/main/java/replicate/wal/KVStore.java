package replicate.wal;

import replicate.common.Config;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KVStore {

    private final Map<String, String> kv = new HashMap<>();
    private final WriteAheadLog wal;
    private final Config config;

    public KVStore(Config config) {
        this.config = config;
        this.wal = WriteAheadLog.openWAL(config);
        applyLog(); // apply log at startup
    }

    public String get(String key) {
        return kv.get(key);
    }

    public void put(String key, String value) {
        appendLog(key, value);
        kv.put(key, value);
    }

    private void appendLog(String key, String value) {
        wal.writeEntry(new SetValueCommand(key, value).serialize());
    }

    private void applyLog() {
        List<WALEntry> walEntries = wal.readAll();
        applyEntries(walEntries);
    }

    private void applyEntries(List<WALEntry> walEntries) {
        for (WALEntry walEntry: walEntries) {
            Command cmd = deserialize(walEntry);
            if (cmd instanceof SetValueCommand) {
                SetValueCommand setValueCmd = (SetValueCommand) cmd;
                kv.put(setValueCmd.getKey(), setValueCmd.getValue());
            }
        }
    }

    private Command deserialize(WALEntry walEntry) {
        return Command.deserializeFrom(new ByteArrayInputStream(walEntry.getData()));
    }
}
