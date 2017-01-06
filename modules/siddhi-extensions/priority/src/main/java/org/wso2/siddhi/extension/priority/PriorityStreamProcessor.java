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
package org.wso2.siddhi.extension.priority;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.*;

/**
 * PriorityStreamProcessor keeps track of the priority of events in a stream. This stream processor
 * requires three arguments which are:
 * - Unique key variable to identify the event
 * - Priority variable which contains the priority increment
 * - Timeout in constant to decrease the priority by one after the given timeout
 * <p>
 * PriorityStreamProcessor injects two new attributes into the stream which are
 * - priorityKey (see the constant {@link PriorityStreamProcessor#ATTRIBUTE_PRIORITY_KEY})
 * - currentPriority (see the constant {@link PriorityStreamProcessor#ATTRIBUTE_CURRENT_PRIORITY})
 * <p>
 * When an event with new unique key arrives, PriorityStreamProcessor checks the priority and if
 * the priority is 0 the event will be sent out without being stored internally. If the event has
 * a priority greater than 0, it will be stored in the stream processor and the current priority
 * will be injected into that event.
 * <p>
 * When an event with existing priority key arrives, it will be stored as the recent event and the
 * priority will be increased by the priority of the received event, and the priorityKey and
 * currentPriority will be injected into the event.
 * <p>
 * After every given timeout, priority of every events will be reduced by 1 and the updated priority,
 * will be sent out with the last known attributes of those events. It will continue until their
 * priority reduced to 0.
 */
public class PriorityStreamProcessor extends StreamProcessor implements SchedulingProcessor {

    /**
     * First attribute name injected by PriorityStreamProcessor into the output stream event.
     */
    public static final String ATTRIBUTE_PRIORITY_KEY = "priorityKey";

    /**
     * Second attribute name injected by PriorityStreamProcessor into the output stream event.
     */
    public static final String ATTRIBUTE_CURRENT_PRIORITY = "currentPriority";

    /**
     * Time to wait for a new event arrival before reducing the priority.
     */
    private long timeInMilliSeconds;

    /**
     * Scheduler used to trigger events for every timeInMilliSeconds.
     */
    private Scheduler scheduler;

    /**
     * ExecutionPlanContext of Siddhi.
     */
    private ExecutionPlanContext executionPlanContext;

    /**
     * Timestamp of the last event received by PriorityStreamProcessor.
     */
    private volatile long lastTimestamp = Long.MIN_VALUE;

    /**
     * Position of the unique event key variable in the event attributes.
     */
    private int[] keyPosition;

    /**
     * Position of the priority variable in the event attributes.
     */
    private int[] priorityPosition;

    /**
     * Keep track of events with priority higher than zero.
     */
    private Map<Object, EventHolder> eventHolderMap = new HashMap<Object, EventHolder>();

    /**
     * Initialize the PriorityStreamProcessor.
     *
     * @param inputDefinition              Input Definition
     * @param attributeExpressionExecutors Array of AttributeExpressionExecutor
     * @param executionPlanContext         ExecutionPlanContext of Siddhi
     * @return list of new attributes injected by PriorityStreamProcessor
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {

        this.executionPlanContext = executionPlanContext;

        // Expression executors are used to get the position and type of attributes
        VariableExpressionExecutor keyExpressionExecutor;
        VariableExpressionExecutor priorityExpressionExecutor;

        // PriorityStreamProcessor requires 3 parameters: uniqueKey, priority, time
        if (attributeExpressionExecutors.length == 3) {

            // First parameter: uniqueKey. Must be a variable of any type
            if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
                keyExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[0];
                this.keyPosition = keyExpressionExecutor.getPosition();
            } else {
                throw new UnsupportedOperationException("First parameter of priority stream processor must be a variable but found " + attributeExpressionExecutors[0].getClass().getCanonicalName());
            }

            // Second parameter: priority. Must be a variable of INT or LONG
            if (attributeExpressionExecutors[1] instanceof VariableExpressionExecutor) {
                Attribute.Type type = attributeExpressionExecutors[1].getReturnType();
                if (type != Attribute.Type.INT && type != Attribute.Type.LONG) {
                    throw new ExecutionPlanValidationException("Second parameter of priority stream processor should be either int or long, but found " + type);
                }
                priorityExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[1];
                this.priorityPosition = priorityExpressionExecutor.getPosition();
            } else {
                throw new UnsupportedOperationException("Second parameter of priority stream processor must be a variable but found " + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }

            // Third parameter: time. Must be a constant of INT or LONG
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                Attribute.Type type = attributeExpressionExecutors[2].getReturnType();
                if (type == Attribute.Type.INT) {
                    this.timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
                } else if (type == Attribute.Type.LONG) {
                    this.timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new ExecutionPlanValidationException("Third parameter of priority stream processor should be either int or long, but found " + type);
                }
            } else {
                throw new ExecutionPlanValidationException("Third parameter of priority stream processor must be a variable but found " + attributeExpressionExecutors[2].getClass().getCanonicalName());
            }
        } else {
            // Invalid number of arguments
            throw new UnsupportedOperationException("Invalid number of arguments passed to priority stream fin. Required 3, but found " + attributeExpressionExecutors.length);
        }

        List<Attribute> attributes = new ArrayList<Attribute>();
        attributes.add(new Attribute(ATTRIBUTE_PRIORITY_KEY, keyExpressionExecutor.getAttribute().getType()));
        attributes.add(new Attribute(ATTRIBUTE_CURRENT_PRIORITY, priorityExpressionExecutor.getAttribute().getType()));

        return attributes;
    }

    /**
     * Process events received by PriorityStreamProcessor.
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {

            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();

                // Timer event sent by scheduler
                long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
                Iterator<Map.Entry<Object, EventHolder>> iterator = this.eventHolderMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    EventHolder eventHolder = iterator.next().getValue();
                    long timeDiff = eventHolder.getTimestamp() + timeInMilliSeconds - currentTime;
                    if (timeDiff <= 0) {
                        // Timeout happens
                        eventHolder.decreasePriority(currentTime);
                        if (eventHolder.isExpired()) {
                            // Remove from the map
                            iterator.remove();
                        }
                        streamEventChunk.insertBeforeCurrent(eventHolder.getClonedEvent());
                    }
                }

                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {
                    // Current event received by the processor
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);

                    // Unique key of the event
                    Object key = streamEvent.getAttribute(this.keyPosition);
                    EventHolder eventHolder = eventHolderMap.get(key);

                    if (eventHolder == null) {
                        // Create a new event holder
                        eventHolder = new EventHolder(key, clonedEvent);

                        if (!eventHolder.isExpired()) {
                            // Event with a priority higher than 0
                            this.eventHolderMap.put(key, eventHolder);
                        }

                    } else {
                        // Set the recent event and increase the priority
                        eventHolder.setEvent(clonedEvent);
                    }

                    // Add priorityKey and currentPriority to current stream
                    complexEventPopulater.populateComplexEvent(streamEvent, new Object[]{key, eventHolder.getPriority()});

                }

//                // Schedule the next alert if this is a new event and there are events in the event holder map
                if (lastTimestamp < currentTime && !eventHolderMap.isEmpty()) {
                    scheduler.notifyAt(currentTime + timeInMilliSeconds);
                    lastTimestamp = currentTime;
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }


    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        return new Object[]{this.eventHolderMap};
    }

    @Override
    public void restoreState(Object[] state) {
        this.eventHolderMap = (Map<Object, EventHolder>) state[0];
    }


    /**
     * This class holds events and performs utility operations on the stored event.
     */
    private class EventHolder {
        /**
         * Unique priority key of the event.
         */
        private Object key;

        /**
         * Current priority of the event.
         */
        private Object priority;

        /**
         * The latest event arrived with the given unique priority key.
         */
        private StreamEvent event;

        /**
         * Create an EventHolder instance.
         *
         * @param key   unique priority key
         * @param event Stream Event
         */
        public EventHolder(Object key, StreamEvent event) {
            if (key == null) {
                throw new IllegalArgumentException("Priority unique key cannot be null");
            }
            this.priority = event.getAttribute(priorityPosition);
            if (this.priority == null) {
                throw new IllegalArgumentException("Priority value cannot be null, but event with priority key " + key + " contains a null priority");
            }
            this.key = key;
            this.event = event;
        }

        /**
         * Current priority of the event.
         *
         * @return the current priority
         */
        public Object getPriority() {
            return priority;
        }

        /**
         * Timestamp of the latest event.
         *
         * @return the timestamp in milliseconds
         */
        public long getTimestamp() {
            return event.getTimestamp();
        }

        /**
         * Decrease the priority of the event by 1 and update the timestamp.
         *
         * @param currentTime Current time in milliseconds
         */
        public void decreasePriority(long currentTime) {
            // PriorityStreamProcessor accepts only INT and LONG priority values.
            if (priority instanceof Integer) {
                priority = ((Integer) priority) - 1;
            } else {
                priority = ((Long) priority) - 1;
            }
            this.event.setTimestamp(currentTime);
        }

        /**
         * If the current priority is 0, the event is expired.
         *
         * @return true if current priority is 0
         */
        public boolean isExpired() {
            boolean expired;
            // PriorityStreamProcessor accepts only INT and LONG priority values.
            if (priority instanceof Integer) {
                expired = ((Integer) priority).intValue() <= 0;
            } else {
                // According to the validation in init, type can be either INT or LONG
                expired = ((Long) priority).longValue() <= 0L;
            }
            return expired;
        }

        /**
         * Clone the underlying event, inject the priority key and current priority and return the cloned event.
         *
         * @return cloned event
         */
        public StreamEvent getClonedEvent() {
            StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(event);
            complexEventPopulater.populateComplexEvent(clonedEvent, new Object[]{key, priority});
            clonedEvent.setTimestamp(executionPlanContext.getTimestampGenerator().currentTime());
            return clonedEvent;
        }

        /**
         * Set the event and increase the priority by the the priority of the new event.
         *
         * @param event Stream Event
         */
        public void setEvent(StreamEvent event) {
            this.event = event;

            if (priority instanceof Integer) {
                priority = ((Integer) priority) + ((Integer) event.getAttribute(priorityPosition));
            } else {
                // According to the validation in init, type can be either INT or LONG
                priority = ((Long) priority) + ((Long) event.getAttribute(priorityPosition));
            }
        }
    }
}
