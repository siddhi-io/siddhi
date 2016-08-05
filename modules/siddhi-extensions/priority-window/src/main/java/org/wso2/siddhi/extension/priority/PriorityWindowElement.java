package org.wso2.siddhi.extension.priority;

import org.wso2.siddhi.core.event.stream.StreamEvent;

/**
 * Created by bhagya on 8/3/16.
 */
public class PriorityWindowElement {
    private long priority;
    private String id;
    private long location;
    private PriorityWindowElement nextElement;
    private StreamEvent event;

    public PriorityWindowElement(StreamEvent event, String id, long priority, long location) {
        this.id = id;
        this.priority = priority;
        this.location = location;
        this.event = event;
    }

    public long getPriority() {
        return priority;
    }

    public String getId() {
        return id;
    }

    public long getLocation() {
        return location;
    }

    public void setPriority(long priority) {
        this.priority = priority;
    }

    public void setNextElement(PriorityWindowElement nextElement) {
        this.nextElement = nextElement;
    }

    public PriorityWindowElement getNextElement() {
        return nextElement;
    }

    public StreamEvent getEvent() {
        return event;
    }
}
