package replicate.wal;

import java.io.*;

public abstract class Command {
    public static int SET_VALUE_TYPE = 1;

    private long clientId = -1;
    private int requestNumber;

    public static Command deserializeFrom(InputStream is) {
        try {
            DataInputStream dis = new DataInputStream(is);
            long clientId = dis.readLong();
            int requestNumber = dis.readInt();
            int cmdType = dis.readInt();

            if (cmdType == SET_VALUE_TYPE) {
                return SetValueCommand.deserializeFrom(is)
                        .withClientId(clientId)
                        .withRequestNumber(requestNumber);
            }
            throw new IllegalArgumentException("Unknown commandType: " + cmdType);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public byte[] serialize() {
        try {
            var baos = new ByteArrayOutputStream();
            var dos = new DataOutputStream(baos);

            dos.writeLong(clientId);
            dos.writeInt(requestNumber);

            serializeTo(dos);
            return baos.toByteArray();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected abstract void serializeTo(DataOutputStream dos) throws IOException;

    public <T extends Command> T withClientId(long clientId) {
        this.clientId = clientId;
        return (T) this;
    }

    public <T extends Command> T withRequestNumber(int requestNumber) {
        this.requestNumber = requestNumber;
        return (T) this;
    }
}
