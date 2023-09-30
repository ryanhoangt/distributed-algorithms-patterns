package com.ryan.gossip;

public enum RequestType {
    HeartbeatRequest(0),
    PushGossipState(1);

    private int id;

    RequestType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}
