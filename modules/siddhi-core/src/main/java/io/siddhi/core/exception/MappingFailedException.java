package io.siddhi.core.exception;

import io.siddhi.core.util.restream.model.ErroneousEvent;

import java.util.List;

public class MappingFailedException extends Exception {
    List<ErroneousEvent> failures;

    public MappingFailedException() {
        super();
    }

    public MappingFailedException(List<ErroneousEvent> failures) {
        this.failures = failures;
    }

    public MappingFailedException(String message) {
        super(message);
    }

    public MappingFailedException(List<ErroneousEvent> failures, String message) {
        super(message);
        this.failures = failures;
    }

    public MappingFailedException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public MappingFailedException(List<ErroneousEvent> failures, String message, Throwable cause) {
        super(message, cause);
        this.failures = failures;
    }

    public MappingFailedException(Throwable throwable) {
        super(throwable);
    }

    public MappingFailedException(List<ErroneousEvent> failures, Throwable cause) {
        super(cause);
        this.failures = failures;
    }

    public List<ErroneousEvent> getFailures() {
        return failures;
    }
}
