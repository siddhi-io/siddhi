package org.wso2.siddhi.core.util.kvstore;

public class KeyValueStoreException extends RuntimeException {

    public KeyValueStoreException(String message) {
        super(message);
    }

    public KeyValueStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}