package com.ryan.gossip;

public class VersionedValue {

    private long version;
    private String value;

    public VersionedValue(String value, long version) {
        this.value = value;
        this.version = version;
    }

    public long getVersion() {
        return version;
    }

    public String getValue() {
        return value;
    }
}
