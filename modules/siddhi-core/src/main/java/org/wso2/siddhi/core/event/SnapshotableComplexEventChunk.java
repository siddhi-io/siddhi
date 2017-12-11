/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.util.snapshot.Snapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;

/**
 * Collection used to group and manage chunk or ComplexEvents
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
    protected ArrayList<ComplexEvent> additionsList;

    public int getNumberOfDeletions() {
        return numberOfDeletions;
    }

    protected int numberOfDeletions;
    private boolean isFirstSnapshot = true;
    private int eventsCount;
    private static final float FULL_SNAPSHOT_THRESHOLD = 0.8f;

    public SnapshotableComplexEventChunk(boolean isBatch) {
        this.isBatch = isBatch;
        this.additionsList = new ArrayList<ComplexEvent>();
    }

    //Only to maintain backward compatibility
    @Deprecated
    public SnapshotableComplexEventChunk() {
        this.isBatch = true;
        this.additionsList = new ArrayList<ComplexEvent>();
    }

    //Only to maintain backward compatibility
    @Deprecated
    public SnapshotableComplexEventChunk(E first, E last) {
        this.first = first;
        this.last = last;
        this.isBatch = true;
    }

    public SnapshotableComplexEventChunk(E first, E last, boolean isBatch) {
        this.first = first;
        this.last = last;
        this.isBatch = isBatch;
        this.additionsList = new ArrayList<ComplexEvent>();
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
        this.additionsList.add(complexEvents);
        eventsCount++;
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

        ComplexEvent event = null;

        if (previousToLastReturned != null) {
            event = lastReturned.getNext();
            previousToLastReturned.setNext(event);
        } else {
            first = (E) lastReturned.getNext();
            if (first == null) {
                last = null;
            } else {
                event = first;
            }
        }
        lastReturned.setNext(null);
        lastReturned = null;
        numberOfDeletions++;
        --eventsCount;
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
            numberOfDeletions++;
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

    public Snapshot getSnapshot() {
        StreamEvent first = null;

        if (isFirstSnapshot) {
            first = (StreamEvent) this.getFirst();
            additionsList = new ArrayList<ComplexEvent>();
            numberOfDeletions = 0;
            isFirstSnapshot = false;
            return new Snapshot(first);
        }

        if (isFullSnapshot()) {
            first = (StreamEvent) this.getFirst();
            return new Snapshot(first);
        } else {
            first = null;
            ArrayList<ComplexEvent> list1 = additionsList;
            additionsList = new ArrayList<ComplexEvent>();
            int deletions = numberOfDeletions;
            numberOfDeletions = 0;
            return new Snapshot(list1, deletions);
        }
    }

    private boolean isFullSnapshot() {
        int numberOfChanges = additionsList.size() + numberOfDeletions;

        if (numberOfChanges > (eventsCount * FULL_SNAPSHOT_THRESHOLD)) {
            return true;
        } else {
            return false;
        }
    }

    public void restore(String key, Map<String, Object> state) {
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

            if (firstFlag) {
                first = (E) snpObj.getEnclosingStreamEvent();
                last = getLastEvent((E) snpObj.getEnclosingStreamEvent());
                firstFlag = false;
            } else {
                ArrayList<ComplexEvent> addList = snpObj.getAdditionsList();

                if (addList != null) {
                    for (ComplexEvent c : addList) {
                        //Maybe we should set next to null during the persistance.
                        c.setNext(null);
                        this.add((E) c);
                    }
                }

                int numDeletions = snpObj.getNumberOfDeletions();
                while (numDeletions > 0) {
                    this.poll();
                    numDeletions--;
                }
            }
        }
    }
}
