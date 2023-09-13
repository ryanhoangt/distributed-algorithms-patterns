package replicate.wal;

import java.nio.channels.FileChannel;

public class WALEntryDeserializer {

    private FileChannel logChannel;

    public WALEntryDeserializer(FileChannel logChannel) {
        this.logChannel = logChannel;
    }

    public WALEntry readEntry(long startPosition) {
        // TODO:

        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
