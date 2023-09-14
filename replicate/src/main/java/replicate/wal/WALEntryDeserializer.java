package replicate.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class WALEntryDeserializer {

    private FileChannel logChannel;
    private ByteBuffer intBuffer = ByteBuffer.allocate(WriteAheadLog.INT_SIZE_BYTES);
    private ByteBuffer longBuffer = ByteBuffer.allocate(WriteAheadLog.LONG_SIZE_BYTES);

    public WALEntryDeserializer(FileChannel logChannel) {
        this.logChannel = logChannel;
    }

    public WALEntry readEntry(long startPosition) {
        Integer entrySize = readInteger(startPosition);
        Header header = new Header(startPosition + WriteAheadLog.INT_SIZE_BYTES);
        Integer entryType = header.readEntryType();
        Long entryId = header.readEntryId();
        Long entryTimestamp = header.readEntryTimestamp();

        int headerSize = header.getSize();
        int dataSize = entrySize - headerSize;

        ByteBuffer buffer = ByteBuffer.allocate(dataSize);
        readFromChannelToBuffer(logChannel, buffer, startPosition + WriteAheadLog.INT_SIZE_BYTES + headerSize);
//        return new WALEntry(entryId, buffer.array(), EntryType.values()[entryType]);
        return new WALEntry(entryId, buffer.array(), EntryType.valueOf(entryType));
    }

    private Integer readInteger(long position) {
        readFromChannelToBuffer(logChannel, intBuffer, position);
        return intBuffer.getInt();
    }

    private Long readLong(long position) {
        readFromChannelToBuffer(logChannel, longBuffer, position);
        return longBuffer.getLong();
    }

    private long readFromChannelToBuffer(FileChannel channel, ByteBuffer buffer, long filePosition) {
        try {
            buffer.clear(); // clear to start reading
            int bytesRead;
            do {
                bytesRead = channel.read(buffer, filePosition);
                filePosition += bytesRead;
            } while (bytesRead != -1 && buffer.hasRemaining());
            buffer.flip(); // flip indicating ready to read
            return channel.position();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    class Header {
        private long headerStartOffset;

        public Header(long headerStartOffset) {
            this.headerStartOffset = headerStartOffset;
        }

        public Integer readEntryType() {
            return readInteger(headerStartOffset);
        }

        public Long readEntryId() { // TODO: recheck the offset position
            return readLong(headerStartOffset +
                    WriteAheadLog.LONG_SIZE_BYTES +
                    WriteAheadLog.INT_SIZE_BYTES);
        }

        public Long readEntryTimestamp() { // TODO: recheck the offset position
            return readLong(headerStartOffset +
                    WriteAheadLog.LONG_SIZE_BYTES +
                    WriteAheadLog.LONG_SIZE_BYTES +
                    WriteAheadLog.INT_SIZE_BYTES);
        }

        public int getSize() { // TODO: recheck header size
            return WriteAheadLog.INT_SIZE_BYTES +
                    WriteAheadLog.LONG_SIZE_BYTES +
                    WriteAheadLog.LONG_SIZE_BYTES +
                    WriteAheadLog.LONG_SIZE_BYTES;
        }
    }

}
