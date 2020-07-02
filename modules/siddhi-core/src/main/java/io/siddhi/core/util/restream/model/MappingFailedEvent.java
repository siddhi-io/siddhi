package io.siddhi.core.util.restream.model;

public class MappingFailedEvent {
    private Object event;
    private Throwable throwable;
    private String cause;

    public MappingFailedEvent(Object event, String cause) {
        this.event = event;
        this.cause = cause;
    }

    public MappingFailedEvent(Object event, Throwable throwable, String cause) {
        this.event = event;
        this.throwable = throwable;
        this.cause = cause;
    }

    public Object getEvent() {
        return event;
    }

    public String getCause() {
        return cause;
    }

    public Throwable getThrowable() {
        return throwable;
    }
}
