package com.ryan.common;

public class Request {

    private Integer requestId;
    private byte[] messageBody; // json

    public Request(Integer requestId, byte[] messageBody) {
        this.requestId = requestId;
        this.messageBody = messageBody;
    }
}
