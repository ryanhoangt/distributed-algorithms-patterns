package com.ryan.net;

import java.net.InetAddress;

public class InetAddressAndPort {
    private InetAddress address;
    private Integer port;

    public InetAddressAndPort(InetAddress address, Integer port) {
        this.address = address;
        this.port = port;
    }
}
