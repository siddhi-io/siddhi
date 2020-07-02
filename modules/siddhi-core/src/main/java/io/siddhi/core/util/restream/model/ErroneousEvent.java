package io.siddhi.core.util.restream.model;

public class ErroneousEvent {
    private Object event;
    private Throwable throwable;
    private String cause;
    private Object originalPayload;

    public ErroneousEvent(Object event, String cause) {
        this.event = event;
        this.cause = cause;
    }

    public ErroneousEvent(Object event, Throwable throwable, String cause) {
        this.event = event;
        this.throwable = throwable;
        this.cause = cause;
    }

    public void setOriginalPayload(Object originalPayload) {
        this.originalPayload = originalPayload;
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

    public Object getOriginalPayload() {
        return originalPayload;
    }
}
