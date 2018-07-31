package org.wso2.siddhi.core.exception;

/**
 * Exception class to be used when there is an error in data purging.
 **/
public class DataPurgingException extends RuntimeException {
    public DataPurgingException() {
        super();
    }

    public DataPurgingException(String message) {
        super(message);
    }

    public DataPurgingException(String message,
                                Throwable throwable) {
        super(message, throwable);
    }

    public DataPurgingException(Throwable throwable) {
        super(throwable);
    }

}
