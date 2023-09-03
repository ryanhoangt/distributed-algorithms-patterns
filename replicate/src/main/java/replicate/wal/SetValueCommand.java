package replicate.wal;

import java.io.*;

public class SetValueCommand extends Command {
    private final String key;
    private final String value;

    public SetValueCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    protected void serializeTo(DataOutputStream dos) throws IOException {
        dos.writeInt(Command.SET_VALUE_TYPE);
        dos.writeUTF(key);
        dos.writeUTF(value);
    }

    public static SetValueCommand deserializeFrom(InputStream is) {
        try {
            DataInputStream dis = new DataInputStream(is);
            return new SetValueCommand(dis.readUTF(), dis.readUTF());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
