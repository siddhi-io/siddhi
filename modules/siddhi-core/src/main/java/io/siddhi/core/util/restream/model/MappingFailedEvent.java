package io.siddhi.core.util.restream.model;

public class MappingFailedEvent {
    private Object event;
    private String cause;

    public MappingFailedEvent(Object event, String cause) {
        this.event = event;
        this.cause = cause;
    }

    public Object getEvent() {
        return event;
    }

    public String getCause() {
        return cause;
    }
}
