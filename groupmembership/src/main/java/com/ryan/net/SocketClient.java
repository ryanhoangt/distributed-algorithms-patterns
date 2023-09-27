package com.ryan.net;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

/**
 * A wrapper class over the built-in {@link java.net.Socket}, representing
 * a connection to another node.
 */
public class SocketClient {
    private static final Logger logger = LogManager.getLogger(SocketClient.class);

    private Socket clientSocket;

    public SocketClient(InetAddressAndPort addr) throws IOException {
        this.clientSocket = new Socket(addr.getAddress(), addr.getPort());
    }

}
