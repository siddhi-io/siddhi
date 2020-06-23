package io.siddhi.core.exception;

import java.util.List;

public class MappingFailedException extends Exception {
    List<Object> failures;

    public MappingFailedException() {
        super();
    }

    public MappingFailedException(List<Object> failures) {
        this.failures = failures;
    }

    public MappingFailedException(String message) {
        super(message);
    }

    public MappingFailedException(List<Object> failures, String message) {
        super(message);
        this.failures = failures;
    }

    public MappingFailedException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public MappingFailedException(List<Object> failures, String message, Throwable cause) {
        super(message, cause);
        this.failures = failures;
    }

    public MappingFailedException(Throwable throwable) {
        super(throwable);
    }

    public MappingFailedException(List<Object> failures, Throwable cause) {
        super(cause);
        this.failures = failures;
    }

    public List<Object> getFailures() {
        return failures;
    }
}
