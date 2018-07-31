package org.wso2.siddhi.core.exception;

/**
 * Exception class to be used when user provided duration is invalid
 **/
public class NoSuchDurationException extends RuntimeException {
    public NoSuchDurationException() {
        super();
    }

    public NoSuchDurationException(String message) {
        super(message);
    }

    public NoSuchDurationException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public NoSuchDurationException(Throwable throwable) {
        super(throwable);
    }
}
