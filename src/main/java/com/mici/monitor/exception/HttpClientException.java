package com.mici.monitor.exception;

public class HttpClientException extends RuntimeException {

    public HttpClientException() {
    }

    public HttpClientException(Exception e) {
        super(e);
    }

    public HttpClientException(String messsage) {
        super(messsage);
    }

    public HttpClientException(String messsage,Exception e) {
        super(messsage,e);
    }

    private static final long serialVersionUID = -2345036415069363458L;
}