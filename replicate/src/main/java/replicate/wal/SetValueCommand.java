package replicate.wal;

public class SetValueCommand extends Command {
    private final String key;
    private final String value;

    public SetValueCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
