package com.ryan.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class InetAddressAndPort {
    private InetAddress address;
    private Integer port;

    public InetAddressAndPort(InetAddress address, Integer port) {
        this.address = address;
        this.port = port;
    }

    public static InetAddressAndPort parse(String value) {
        String[] parts = value.substring(1, value.length() - 1).split(",");
        return create(parts[0], Integer.valueOf(parts[1]));
    }

    private static InetAddressAndPort create(String hostIp, Integer port) {
        try {
            return new InetAddressAndPort(InetAddress.getByName(hostIp), port);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InetAddressAndPort that = (InetAddressAndPort) o;
        return Objects.equals(address, that.address) && Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }

    @Override
    public String toString() {
        return "[" + address.getHostAddress() + "," + port + ']';
    }
}
