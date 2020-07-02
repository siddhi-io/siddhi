package io.siddhi.core.util.restream.model;

public class MappingFailedEvent {
    private Object event;
    private Exception exception;
    private String cause;

    public MappingFailedEvent(Object event, String cause) {
        this.event = event;
        this.cause = cause;
    }

    public MappingFailedEvent(Object event, Exception exception, String cause) {
        this.event = event;
        this.exception = exception;
        this.cause = cause;
    }

    public Object getEvent() {
        return event;
    }

    public String getCause() {
        return cause;
    }

    public Exception getException() {
        return exception;
    }
}
