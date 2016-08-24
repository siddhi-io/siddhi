/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.extension.priority;

import org.wso2.siddhi.core.event.stream.StreamEvent;

/**
 *  on 8/3/16.
 */
public class PriorityWindowElement {
    private long priority;
    private String id;
    private long location;
    private PriorityWindowElement nextElement;
    private StreamEvent event;
    private long timeStamp;

    public PriorityWindowElement(StreamEvent event, String id, long priority, long location, long timeStamp) {
        this.id = id;
        this.priority = priority;
        this.location = location;
        this.event = event;
        this.timeStamp=timeStamp;
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

    public boolean hasNext(){
        if(nextElement!=null){
            return true;
        }
        return false;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
