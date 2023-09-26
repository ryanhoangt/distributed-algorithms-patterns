package com.ryan.gossip;

import java.util.HashMap;
import java.util.Map;

public class NodeState {

    private Map<String, VersionedValue> values = new HashMap<>();

    public NodeState() {
    }

    public void add(String key, VersionedValue versionedValue) {
        values.put(key, versionedValue);
    }

    public VersionedValue get(String key) {
        return values.get(key);
    }
}
