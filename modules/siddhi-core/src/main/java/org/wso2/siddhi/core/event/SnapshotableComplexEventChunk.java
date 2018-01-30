/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.core.event;

import org.wso2.siddhi.core.event.stream.Operation;
import org.wso2.siddhi.core.event.stream.Operator;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.table.holder.ListEventHolder;
import org.wso2.siddhi.core.util.snapshot.Snapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;

/**
 * The class which encloses the data structure for state management in Siddhi. Supports the incremental checkpointing
 * functionality.
 *
 * @param <E> sub types of ComplexEvent such as StreamEvent and StateEvent
 */
public class SnapshotableComplexEventChunk<E extends ComplexEvent> implements Iterator<E>, Serializable {
    private static final long serialVersionUID = 3185987841726255019L;
    protected E first;
    protected E previousToLastReturned;
    protected E lastReturned;
    protected E last;
    protected boolean isBatch = false;
    private ArrayList<Operation> changeLog;
    private long eventsCount;
    private static final float FULL_SNAPSHOT_THRESHOLD = 2.1f;
    private boolean isFirstSnapshot = true;
    private boolean isRecovery;
    private String operatorType;
    private long threshold;

    public SnapshotableComplexEventChunk(boolean isBatch) {
        this.isBatch = isBatch;
        this.changeLog = new ArrayList<Operation>();
    }

    //Only to maintain backward compatibility
    @Deprecated
    public SnapshotableComplexEventChunk() {
        this.isBatch = true;
        this.changeLog = new ArrayList<Operation>();
    }

    //Only to maintain backward compatibility
    @Deprecated
    public SnapshotableComplexEventChunk(E first, E last) {
        this.first = first;
        this.last = last;
        this.isBatch = true;
        this.changeLog = new ArrayList<Operation>();
    }

    public SnapshotableComplexEventChunk(E first, E last, boolean isBatch) {
        this.first = first;
        this.last = last;
        this.isBatch = isBatch;
        this.changeLog = new ArrayList<Operation>();
    }

    public SnapshotableComplexEventChunk(boolean isBatch, String metaInfo) {
        this.isBatch = isBatch;
        this.changeLog = new ArrayList<Operation>();
        populateMetaInformation(metaInfo);
    }

    private void populateMetaInformation(String metaInfo) {
        String[] metaInfoStringArr = metaInfo.split(":");
        operatorType = metaInfoStringArr[0];
        threshold = Long.parseLong(metaInfoStringArr[1]);
    }

    public void insertBeforeCurrent(E events) {

        if (lastReturned == null) {
            throw new IllegalStateException();
        }

        E currentEvent = getLastEvent(events);

        if (previousToLastReturned != null) {
            previousToLastReturned.setNext(events);
        } else {
            first = events;
        }
        previousToLastReturned = currentEvent;

        currentEvent.setNext(lastReturned);
    }


    public void insertAfterCurrent(E streamEvents) {

        if (lastReturned == null) {
            throw new IllegalStateException();
        }

        E currentEvent = getLastEvent(streamEvents);

        ComplexEvent nextEvent = lastReturned.getNext();
        lastReturned.setNext(streamEvents);
        currentEvent.setNext(nextEvent);

    }

    public void add(E complexEvents) {

        if (first == null) {
            first = complexEvents;
        } else {
            last.setNext(complexEvents);
        }
        last = getLastEvent(complexEvents);

        eventsCount++;
        if (!isRecovery) {
            this.changeLog.add(new Operation(Operator.ADD, (StreamEvent) complexEvents));
        }
    }

    public void add(E complexEvents, E complexEvents2) {

        if (first == null) {
            first = complexEvents;
        } else {
            last.setNext(complexEvents);
        }
        last = getLastEvent(complexEvents);

        eventsCount++;
        if (!isRecovery) {
            this.changeLog.add(new Operation(Operator.ADD, (StreamEvent) complexEvents2));
        }
    }

    private E getLastEvent(E complexEvents) {
        E lastEvent = complexEvents;
        while (lastEvent != null && lastEvent.getNext() != null) {
            lastEvent = (E) lastEvent.getNext();
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
    public E next() {
        E returnEvent;
        if (lastReturned != null) {
            returnEvent = (E) lastReturned.getNext();
            previousToLastReturned = lastReturned;
        } else if (previousToLastReturned != null) {
            returnEvent = (E) previousToLastReturned.getNext();
        } else {
            returnEvent = first;
        }
        if (returnEvent == null) {
            throw new NoSuchElementException();
        }
        lastReturned = returnEvent;
        return returnEvent;
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
            first = (E) lastReturned.getNext();
            if (first == null) {
                last = null;
            }
        }
        lastReturned.setNext(null);
        lastReturned = null;
        this.changeLog.add(new Operation(Operator.REMOVE));
        eventsCount--;
    }

    public void detach() {
        if (lastReturned == null) {
            throw new IllegalStateException();
        }
        if (previousToLastReturned != null) {
            previousToLastReturned.setNext(null);
        } else {
            clear();
        }
        lastReturned = null;
    }

    public E detachAllBeforeCurrent() {

        if (lastReturned == null) {
            throw new IllegalStateException();
        }

        E firstEvent = null;
        if (previousToLastReturned != null) {
            previousToLastReturned.setNext(null);
            firstEvent = first;
            first = lastReturned;
            previousToLastReturned = null;
        }
        return firstEvent;
    }

    public void clear() {
        previousToLastReturned = null;
        lastReturned = null;
        first = null;
        last = null;
        this.changeLog.add(new Operation(Operator.CLEAR));
        eventsCount = 0;
    }

    public void reset() {
        previousToLastReturned = null;
        lastReturned = null;
    }

    public E getFirst() {
        return first;
    }

    public E getLast() {
        return last;
    }

    public E poll() {
        if (first != null) {
            E firstEvent = first;
            first = (E) first.getNext();
            firstEvent.setNext(null);
            if (!isRecovery) {
                this.changeLog.add(new Operation(Operator.POLL));
            }
            eventsCount--;

            return firstEvent;
        } else {
            return null;
        }
    }

    public boolean isBatch() {
        return isBatch;
    }

    public void setBatch(boolean batch) {
        isBatch = batch;
    }

    @Override
    public String toString() {
        return "EventChunk{" +
                "first=" + first +
                '}';
    }

    public Object getFromIndex(int index) {
        return null;
    }

    public void setAtIndex(int index, Object value) {

    }

    public Snapshot getSnapshot() {
        if (isFirstSnapshot) {
            Snapshot snapshot = new Snapshot(this, false);
            isFirstSnapshot = false;
            this.changeLog.clear();
            return snapshot;
        }

        if (isFullSnapshot()) {
            Snapshot snapshot = new Snapshot(this, false);
            this.changeLog = new ArrayList<Operation>();
            return snapshot;
        } else {
            Snapshot snapshot = new Snapshot(changeLog, true);
            return snapshot;
        }
    }

    private boolean isFullSnapshot() {
        if ((this.changeLog.size() > (eventsCount * FULL_SNAPSHOT_THRESHOLD)) && (eventsCount != 0)) {
            return true;
        } else if (this.changeLog.size() > threshold) {
            return true;
        } else {
            return false;
        }
    }

    public Object restore(String key, Map<String, Object> state) {
        TreeSet<Long> revisions = new TreeSet<Long>();
        for (Map.Entry<String, Object> entry : state.entrySet()) {
            long item = -1L;
            try {
                item = Long.parseLong(entry.getKey());
                revisions.add(item);
            } catch (NumberFormatException e) {
                //ignore
            }
        }

        Iterator<Long> itr = revisions.iterator();
        boolean firstFlag = true;

        while (itr.hasNext()) {
            Object obj = state.get("" + itr.next());

            HashMap<String, Snapshot> firstMap = (HashMap<String, Snapshot>) obj;
            Snapshot snpObj = firstMap.get(key);

            if (snpObj == null) {
                continue;
            }

            if (firstFlag) {
                Object obj2 = snpObj.getState();
                if (obj2.getClass().equals(SnapshotableComplexEventChunk.class)) {
                    SnapshotableComplexEventChunk<StreamEvent> expiredEventChunk =
                            new SnapshotableComplexEventChunk<StreamEvent>(false);

                    expiredEventChunk.add((StreamEvent) ((SnapshotableComplexEventChunk) snpObj.getState()).getFirst());

                    return expiredEventChunk;
                } else if (obj2.getClass().equals(ListEventHolder.class)) {
                    ListEventHolder holder = ((ListEventHolder) snpObj.getState());
                    StreamEvent firstItem = holder.getFirst();
                    holder.clear();
                    holder.add(firstItem);

                    return holder;
                } else if (obj2.getClass().equals(ArrayList.class)) {
                    ArrayList<Operation> addList = (ArrayList<Operation>) snpObj.getState();
                    isRecovery = true;

                    for (Operation op : addList) {
                        switch (op.operation) {
                            case Operator.ADD:
                                //TODO:Need to check whether there is only one event or multiple events.
                                // If so we have to
                                // traverse  the linked list and then get the count by which the eventsCount needs
                                // to be updated.
                                this.add((E) op.parameters);
                                break;
                            case Operator.REMOVE:
                                this.remove();
                                break;
                            case Operator.POLL:
                                this.poll();
                                break;
                            case Operator.CLEAR:
                                this.clear();
                                break;
                            default:
                                continue;
                        }
                    }
                    isRecovery = false;
                }

            } else {
                ArrayList<Operation> addList = (ArrayList<Operation>) snpObj.getState();
                isRecovery = true;

                for (Operation op : addList) {
                    switch (op.operation) {
                        case Operator.ADD:
                            //TODO:Need to check whether there is only one event or multiple events. If so we have to
                            //traverse the linked list and then get the count by which the eventsCount needs
                            // to be updated.
                            this.add((E) op.parameters);
                            break;
                        case Operator.REMOVE:
                            this.remove();
                            break;
                        case Operator.POLL:
                            this.poll();
                            break;
                        case Operator.CLEAR:
                            this.clear();
                            break;
                        default:
                            continue;
                    }
                }
                isRecovery = false;
            }
        }

        return (SnapshotableComplexEventChunk) this;
    }
}
