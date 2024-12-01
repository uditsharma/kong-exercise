package com.konnect.cdc.source;

public class EventSourceException extends RuntimeException {

    public EventSourceException(String message) {
        super(message);
    }

    public EventSourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
