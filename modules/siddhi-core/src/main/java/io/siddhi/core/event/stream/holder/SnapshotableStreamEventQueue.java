/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.event.stream.holder;

import io.siddhi.core.event.stream.Operation;
import io.siddhi.core.event.stream.Operation.Operator;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.util.snapshot.SnapshotRequest;
import io.siddhi.core.util.snapshot.state.Snapshot;
import io.siddhi.core.util.snapshot.state.SnapshotStateList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * The class to hold stream events in a queue and by managing its snapshots
 */
public class SnapshotableStreamEventQueue implements Iterator<StreamEvent>, Serializable {
    private static final long serialVersionUID = 3185987841726255019L;
    protected StreamEvent first;
    protected StreamEvent previousToLastReturned;
    protected StreamEvent lastReturned;
    protected StreamEvent last;
    protected int size;
    private int operationChangeLogThreshold;
    private transient StreamEventClonerHolder eventClonerHolder;
    private ArrayList<Operation> operationChangeLog;
    private long operationChangeLogSize;
    private boolean forceFullSnapshot = true;
    private boolean isOperationLogEnabled = true;
    private int eventIndex = -1;

    public SnapshotableStreamEventQueue(StreamEventClonerHolder eventClonerHolder) {
        this(eventClonerHolder, Integer.MAX_VALUE);
    }

    public SnapshotableStreamEventQueue(StreamEventClonerHolder eventClonerHolder, int operationChangeLogThreshold) {
        this.operationChangeLog = new ArrayList<>();
        this.eventClonerHolder = eventClonerHolder;
        this.operationChangeLogThreshold = operationChangeLogThreshold;
    }

    public void add(StreamEvent events) {
        if (!isFullSnapshot()) {
            if (isOperationLogEnabled) {
                operationChangeLog.add(new Operation(Operator.ADD, copyEvents(events)));
            }
            operationChangeLogSize++;
        } else {
            operationChangeLog.clear();
            operationChangeLogSize = 0;
            forceFullSnapshot = true;
        }

        if (first == null) {
            first = events;
        } else {
            last.setNext(events);
        }
        last = getLastEvent(events);
    }

    /**
     * Removes from the underlying collection the last element returned by the
     * iterator (optional operation).  This method can be called only once per
     * call to <tt>next</tt>.  The behavior of an iterator is unspecified if
     * the underlying collection is modified while the iteration is in
     * progress in any way other than by calling this method.
     *
     * @throws UnsupportedOperationException if the <tt>remove</tt>
     *                                       operation is not supported by this Iterator.
     * @throws IllegalStateException         if the <tt>next</tt> method has not
     *                                       yet been called, or the <tt>remove</tt> method has already
     *                                       been called after the last call to the <tt>next</tt>
     *                                       method.
     */
    public void remove() {
        if (lastReturned == null) {
            throw new IllegalStateException();
        }
        if (previousToLastReturned != null) {
            previousToLastReturned.setNext(lastReturned.getNext());
        } else {
            first = lastReturned.getNext();
            if (first == null) {
                last = null;
            }
        }
        lastReturned.setNext(null);
        lastReturned = null;
        if (!isFullSnapshot()) {
            if (isOperationLogEnabled) {
                operationChangeLog.add(new Operation(Operator.DELETE_BY_INDEX, eventIndex));
            }
            operationChangeLogSize++;
        } else {
            operationChangeLog.clear();
            operationChangeLogSize = 0;
            forceFullSnapshot = true;
        }
        eventIndex--;
        size--;
    }

    private StreamEvent getLastEvent(StreamEvent complexEvents) {
        StreamEvent lastEvent = complexEvents;
        while (lastEvent != null && lastEvent.getNext() != null) {
            lastEvent = lastEvent.getNext();
        }
        return lastEvent;
    }

    /**
     * Returns <tt>true</tt> if the iteration has more elements. (In other
     * words, returns <tt>true</tt> if <tt>next</tt> would return an element
     * rather than throwing an exception.)
     *
     * @return <tt>true</tt> if the iterator has more elements.
     */
    public boolean hasNext() {
        if (lastReturned != null) {
            return lastReturned.getNext() != null;
        } else if (previousToLastReturned != null) {
            return previousToLastReturned.getNext() != null;
        } else {
            return first != null;
        }
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration.
     * @throws NoSuchElementException iteration has no more elements.
     */
    public StreamEvent next() {
        StreamEvent returnEvent;
        if (lastReturned != null) {
            returnEvent = lastReturned.getNext();
            previousToLastReturned = lastReturned;
        } else if (previousToLastReturned != null) {
            returnEvent = previousToLastReturned.getNext();
        } else {
            returnEvent = first;
        }
        if (returnEvent == null) {
            throw new NoSuchElementException();
        }
        lastReturned = returnEvent;
        eventIndex++;
        return returnEvent;
    }

    public void clear() {
        this.operationChangeLog.clear();
        operationChangeLogSize = 0;
        forceFullSnapshot = true;

        previousToLastReturned = null;
        lastReturned = null;
        first = null;
        last = null;
        eventIndex = -1;
    }

    public void reset() {
        previousToLastReturned = null;
        lastReturned = null;
        eventIndex = -1;
    }

    public StreamEvent getFirst() {
        return first;
    }

    public StreamEvent getLast() {
        return last;
    }

    public StreamEvent poll() {
        reset();
        if (first != null) {
            StreamEvent firstEvent = first;
            first = first.getNext();
            firstEvent.setNext(null);

            if (!isFullSnapshot()) {
                if (isOperationLogEnabled) {
                    operationChangeLog.add(new Operation(Operator.REMOVE));
                }
                operationChangeLogSize++;
            } else {
                operationChangeLog.clear();
                operationChangeLogSize = 0;
                forceFullSnapshot = true;
            }
            return firstEvent;
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return "EventQueue{" +
                "first=" + first +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SnapshotableStreamEventQueue that = (SnapshotableStreamEventQueue) o;

        if (operationChangeLogThreshold != that.operationChangeLogThreshold) {
            return false;
        }
        if (operationChangeLogSize != that.operationChangeLogSize) {
            return false;
        }
        if (forceFullSnapshot != that.forceFullSnapshot) {
            return false;
        }
        if (isOperationLogEnabled != that.isOperationLogEnabled) {
            return false;
        }
        if (eventIndex != that.eventIndex) {
            return false;
        }
        if (first != null ? !first.equals(that.first) : that.first != null) {
            return false;
        }
        return operationChangeLog != null ? operationChangeLog.equals(that.operationChangeLog) :
                that.operationChangeLog == null;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + operationChangeLogThreshold;
        result = 31 * result + (operationChangeLog != null ? operationChangeLog.hashCode() : 0);
        result = 31 * result + (int) (operationChangeLogSize ^ (operationChangeLogSize >>> 32));
        result = 31 * result + (forceFullSnapshot ? 1 : 0);
        result = 31 * result + (isOperationLogEnabled ? 1 : 0);
        result = 31 * result + eventIndex;
        return result;
    }

    public Snapshot getSnapshot() {
        if (isFullSnapshot()) {
            forceFullSnapshot = false;
            return new Snapshot(this.getFirst(), false);
        } else {
            Snapshot snapshot = new Snapshot(operationChangeLog, true);
            operationChangeLog = new ArrayList<>();
            return snapshot;
        }
    }

    private boolean isFullSnapshot() {
        return operationChangeLogSize > 100 || operationChangeLogSize > operationChangeLogThreshold
                || forceFullSnapshot || SnapshotRequest.isRequestForFullSnapshot();

    }

    public void restore(SnapshotStateList snapshotStatelist) {
        TreeMap<Long, Snapshot> revisions = snapshotStatelist.getSnapshotStates();
        Iterator<Map.Entry<Long, Snapshot>> itr = revisions.entrySet().iterator();
        this.isOperationLogEnabled = false;
        while (itr.hasNext()) {
            Map.Entry<Long, Snapshot> snapshotEntry = itr.next();
            if (!snapshotEntry.getValue().isIncrementalSnapshot()) {
                this.clear();
                this.add((StreamEvent) snapshotEntry.getValue().getState());
                forceFullSnapshot = false;
            } else {
                ArrayList<Operation> operations = (ArrayList<Operation>) snapshotEntry.getValue().getState();
                for (Operation op : operations) {
                    switch (op.operation) {
                        case ADD:
                            add((StreamEvent) op.parameters);
                            break;
                        case REMOVE:
                            poll();
                            break;
                        case CLEAR:
                            clear();
                            break;
                        case OVERWRITE:
                            int overwriteIndex = (int) ((Object[]) op.parameters)[0];
                            StreamEvent streamEvent = (StreamEvent) ((Object[]) op.parameters)[1];
                            while (hasNext()) {
                                next();
                                if (overwriteIndex == eventIndex) {
                                    overwrite(streamEvent);
                                    break;
                                }
                            }
                            break;
                        case DELETE_BY_OPERATOR:
                            break;
                        case DELETE_BY_INDEX:
                            int deleteIndex = (int) op.parameters;
                            while (hasNext()) {
                                next();
                                if (deleteIndex == eventIndex) {
                                    remove();
                                    break;
                                }
                            }
                            break;
                        default:
                            continue;
                    }
                }
            }
        }
        this.isOperationLogEnabled = true;
    }

    private StreamEvent copyEvents(StreamEvent events) {

        StreamEvent currentEvent = events;
        StreamEvent firstCopiedEvent = eventClonerHolder.getStreamEventCloner().copyStreamEvent(events);
        StreamEvent lastCopiedEvent = firstCopiedEvent;

        while (currentEvent.getNext() != null) {
            currentEvent = currentEvent.getNext();
            StreamEvent copiedStreamEvent = eventClonerHolder.getStreamEventCloner().copyStreamEvent(currentEvent);
            lastCopiedEvent.setNext(copiedStreamEvent);
            lastCopiedEvent = copiedStreamEvent;
        }
        return firstCopiedEvent;
    }

    public void overwrite(StreamEvent streamEvent) {
        if (!isFullSnapshot()) {
            if (isOperationLogEnabled) {
                operationChangeLog.add(new Operation(Operator.OVERWRITE,
                        new Object[]{eventIndex,
                                eventClonerHolder.getStreamEventCloner().copyStreamEvent(streamEvent)}));
            }
            operationChangeLogSize++;
        } else {
            operationChangeLog.clear();
            operationChangeLogSize = 0;
            forceFullSnapshot = true;
        }

        if (previousToLastReturned != null) {
            previousToLastReturned.setNext(streamEvent);
        } else {
            first = streamEvent;
        }
        StreamEvent next = lastReturned.getNext();
        if (next != null) {
            streamEvent.setNext(next);
        } else {
            last = streamEvent;
        }
        lastReturned = streamEvent;
    }
}
