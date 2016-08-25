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
import org.wso2.siddhi.core.event.ComplexEvent;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Operator which is related to notify changes in the priority of an event
 */
public class PriorityWindowProcessor extends StreamProcessor implements SchedulingProcessor {
    private VariableExpressionExecutor variableExpressionID;
    private VariableExpressionExecutor variableExpressionPriority;
    private long duration;
    long currentTime = 0L;
    private PriorityWindowElement currentPriorityWindowElement = null;
    private PriorityWindowElement firstEntryVisited = null;
    Map<String, Long> priorityScoreMap = new HashMap<String, Long>();
    private Scheduler scheduler;

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or
     *                              modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>(false);
        boolean isTimerEvent = false;
        long timeStamp = executionPlanContext.getTimestampGenerator().currentTime();
        /*
          * get modulus value of the time stamp
          */
        long eventLocation = timeStamp % duration;
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                long priority = 0L;
                String id = null;
                if (event.getType() == ComplexEvent.Type.TIMER) {
                    isTimerEvent = true;
                    streamEventChunk.remove();
                }

                 /*
                * If it is not a timer event get variables
                */
                if (!isTimerEvent) {
                    id = (String) variableExpressionID.execute(event);

                    if (variableExpressionPriority.execute(event) instanceof Long) {
                        priority = (Long) variableExpressionPriority.execute(event);
                    } else {
                        priority = (Integer) variableExpressionPriority.execute(event);
                    }


                    //  If events are already visited
                    if (currentPriorityWindowElement != null) {
                        //update current time
                        currentTime = timeStamp;
                        mainLoop:
                        while (true) {
                            long currentLocation = currentPriorityWindowElement.getLocation();
                            long currentPriority = currentPriorityWindowElement.getPriority();
                            String currentID = currentPriorityWindowElement.getId();
                            long currentEventTimeStamp = currentPriorityWindowElement.getTimeStamp();
                        /*
                         * current event is the place to add the new event and it is the last event arrive
                         */
                            if (currentLocation <= eventLocation && !currentPriorityWindowElement.hasNext()) {
                                /*
                                 * priority reduction amount calculation
                                 */
                                long amountToDecrement = ((timeStamp - currentEventTimeStamp) / duration);
                                long newPriority = currentPriority - amountToDecrement;
                                if (newPriority > 0) {
                                    if (newPriority != currentPriority) {
                                        managePositivePriority(newPriority, currentPriorityWindowElement, returnEventChunk, timeStamp);
                                    }
                                    addNewNode(event, id, priority, eventLocation, returnEventChunk, currentPriorityWindowElement, timeStamp);
                                    break;
                                } else {
                                    removeFromMap(currentID, currentPriority);
                                    PriorityWindowElement predecessor = getBeforeElement(currentPriorityWindowElement);
                                    addNewNode(event, id, priority, eventLocation, returnEventChunk, predecessor, timeStamp);
                                    break;
                                }

                            }
                            //currentEvent is the only event available and new event is the predecessor of it
                            else if (currentLocation > eventLocation && currentPriorityWindowElement == firstEntryVisited) {
                                long amountToDecrement = ((timeStamp - currentEventTimeStamp) / duration);
                                long newPriority = currentPriority - amountToDecrement;
                                if (newPriority > 0) {
                                    if (newPriority != currentPriority) {
                                        managePositivePriority(newPriority, currentPriorityWindowElement, returnEventChunk, timeStamp);
                                    }
                                    PriorityWindowElement newElement = new PriorityWindowElement(event, id, priority, eventLocation, timeStamp);
                                    addToMap(id, priority);
                                    newElement.setNextElement(currentPriorityWindowElement);
                                    firstEntryVisited = newElement;
                                    currentPriorityWindowElement = newElement;
                                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(event);
                                    clonedEvent.setOutputData(new Object[]{priority});
                                    scheduler.notifyAt(clonedEvent.getTimestamp() + duration);
                                    returnEventChunk.add(clonedEvent);
                                    break;
                                } else {
                                    removeFromMap(currentID, currentPriority);
                                    PriorityWindowElement newElement = new PriorityWindowElement(event, id, priority, eventLocation, timeStamp);
                                    currentPriorityWindowElement = newElement;
                                    newElement.setNextElement(null);
                                    addToMap(id, priority);
                                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(event);
                                    clonedEvent.setOutputData(new Object[]{priority});
                                    scheduler.notifyAt(clonedEvent.getTimestamp() + duration);
                                    returnEventChunk.add(clonedEvent);
                                    break;
                                }
                            }
                            //current event is the last event arrived but it is not the only event and place to add new event
                            else if (currentLocation > eventLocation && !currentPriorityWindowElement.hasNext()) {
                                long amountToDecrement = ((timeStamp - currentEventTimeStamp) / duration);
                                long newPriority = currentPriority - amountToDecrement;
                                if (newPriority > 0) {
                                    if (newPriority != currentPriority) {
                                        managePositivePriority(newPriority, currentPriorityWindowElement, returnEventChunk, timeStamp);
                                    }
                                    currentPriorityWindowElement = firstEntryVisited;
                                    continue;
                                } else {
                                    removeFromMap(currentID, currentPriority);
                                    PriorityWindowElement predecessor = getBeforeElement(currentPriorityWindowElement);
                                    predecessor.setNextElement(null);
                                    currentPriorityWindowElement = firstEntryVisited;
                                    continue;
                                }

                            }
                            // When current event is not the exact place and it is not the last event arrived
                            else if (currentPriorityWindowElement.hasNext()) {
                                PriorityWindowElement nextElement = currentPriorityWindowElement.getNextElement();
                                long nextElementTimeStamp = nextElement.getTimeStamp();
                                long nextElementLocation = nextElement.getLocation();
                                long nextElementPriority = nextElement.getPriority();
                                String nextElementId = nextElement.getId();
                                if (currentLocation <= eventLocation && nextElementLocation > eventLocation) {
                                    long amountToDecrement = ((timeStamp - currentEventTimeStamp) / duration);
                                    long newPriority = currentPriority - amountToDecrement;
                                    if (newPriority > 0) {
                                        if (newPriority != currentPriority) {
                                            managePositivePriority(newPriority, nextElement, returnEventChunk, timeStamp);
                                        }
                                        addNewNode(event, id, priority, eventLocation, returnEventChunk, currentPriorityWindowElement, timeStamp);
                                        break;
                                    } else {
                                        removeFromMap(currentID, currentPriority);
                                        PriorityWindowElement predecessor = getBeforeElement(currentPriorityWindowElement);
                                        addNewNode(event, id, priority, eventLocation, returnEventChunk, predecessor, timeStamp);
                                        break;
                                    }

                                }
                                //if next event is the place to add current event
                                if (nextElementLocation <= eventLocation && !nextElement.hasNext()) {
                                    long amountToDecrement = ((timeStamp - nextElementTimeStamp) / duration);
                                    long newPriority = nextElementPriority - amountToDecrement;
                                    if (newPriority > 0) {
                                        if (newPriority != nextElementPriority) {
                                            managePositivePriority(newPriority, nextElement, returnEventChunk, timeStamp);
                                        }
                                        addNewNode(event, id, priority, eventLocation, returnEventChunk, nextElement, timeStamp);
                                        break;
                                    } else {
                                        removeFromMap(nextElementId, currentPriority);
                                        PriorityWindowElement predecessor = getBeforeElement(nextElement);
                                        addNewNode(event, id, priority, eventLocation, returnEventChunk, predecessor, timeStamp);
                                        break;
                                    }

                                }
                                //next event has the probability to add current event
                                else if (nextElementLocation <= eventLocation && nextElement.hasNext()) {
                                    //confirm the exact place to add new element
                                    while (nextElement.hasNext()) {
                                        PriorityWindowElement nextEventsSuccessor = nextElement.getNextElement();
                                        long nextEventsSuccessorLocation = nextEventsSuccessor.getLocation();
                                        long nextEventsSuccessorsTimeStamp = nextEventsSuccessor.getTimeStamp();
                                        //exact place is the successor of next event
                                        if (nextEventsSuccessorLocation <= eventLocation && !nextEventsSuccessor.hasNext()) {
                                            currentPriority = nextEventsSuccessor.getPriority();
                                            long amountToDecrement = ((timeStamp - nextEventsSuccessorsTimeStamp) / duration);
                                            long newPriority = currentPriority - amountToDecrement;
                                            if (priority > 0) {
                                                if (newPriority != currentPriority) {
                                                    managePositivePriority(newPriority, nextEventsSuccessor, returnEventChunk, timeStamp);
                                                }
                                                addNewNode(event, id, priority, eventLocation, returnEventChunk, nextEventsSuccessor, timeStamp);
                                                break mainLoop;
                                            } else {
                                                removeFromMap(nextEventsSuccessor.getId(), currentPriority);
                                                PriorityWindowElement predecessor = getBeforeElement(nextEventsSuccessor);
                                                addNewNode(event, id, priority, eventLocation, returnEventChunk, predecessor, timeStamp);
                                                break mainLoop;
                                            }
                                        }
                                        //exact place is the next event
                                        else if (nextEventsSuccessorLocation > eventLocation) {
                                            break;
                                        }
                                        //more to check but reduce priority of all the passing by events
                                        else if (nextEventsSuccessorLocation <= eventLocation && nextEventsSuccessor.hasNext()) {
                                            currentPriority = nextEventsSuccessor.getPriority();
                                            long amountToDecrement = ((timeStamp - nextEventsSuccessorsTimeStamp) / duration);
                                            long newPriority = currentPriority - amountToDecrement;
                                            if (priority > 0) {
                                                if (newPriority != currentPriority) {
                                                    managePositivePriority(newPriority, nextEventsSuccessor, returnEventChunk, timeStamp);
                                                }
                                                nextElement = nextEventsSuccessor;
                                                continue;
                                            } else {
                                                removeFromMap(nextEventsSuccessor.getId(), currentPriority);
                                                PriorityWindowElement predecessor = getBeforeElement(nextEventsSuccessor);
                                                predecessor.setNextElement(nextEventsSuccessor.getNextElement());
                                                nextElement = nextEventsSuccessor.getNextElement();
                                                continue;
                                            }
                                        }

                                    }
                                    long amountToDecrement = ((timeStamp - nextElementTimeStamp) / duration);
                                    long newPriority = nextElementPriority - amountToDecrement;
                                    if (newPriority > 0) {
                                        if (newPriority != currentPriority) {
                                            managePositivePriority(newPriority, nextElement, returnEventChunk, timeStamp);
                                        }
                                        addNewNode(event, id, priority, eventLocation, returnEventChunk, nextElement, timeStamp);
                                        break;
                                    } else {
                                        removeFromMap(nextElementId, currentPriority);
                                        PriorityWindowElement predecessor = getBeforeElement(nextElement);
                                        addNewNode(event, id, priority, eventLocation, returnEventChunk, predecessor, timeStamp);
                                        break;
                                    }

                                } else if (nextElementLocation > eventLocation) {
                                    long amountToDecrement = ((timeStamp - nextElementTimeStamp) / duration);
                                    long newPriority = nextElementPriority - amountToDecrement;
                                    if (newPriority > 0) {
                                        if (newPriority != currentPriority) {
                                            managePositivePriority(newPriority, nextElement, returnEventChunk, timeStamp);
                                        }
                                        currentPriorityWindowElement = firstEntryVisited;
                                        continue;
                                    } else {
                                        removeFromMap(nextElementId, currentPriority);
                                        PriorityWindowElement predecessor = getBeforeElement(nextElement);
                                        if (nextElement.hasNext()) {
                                            predecessor.setNextElement(nextElement.getNextElement());
                                        } else {
                                            predecessor.setNextElement(null);
                                        }
                                        currentPriorityWindowElement = firstEntryVisited;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                    // First event arrived
                    else {
                        currentPriorityWindowElement = new PriorityWindowElement(event, id, priority, eventLocation, timeStamp);
                        priorityScoreMap.put(id, priority);
                        StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(event);
                        clonedEvent.setOutputData(new Object[]{priority});
                        scheduler.notifyAt(clonedEvent.getTimestamp() + duration);
                        returnEventChunk.add(clonedEvent);
                        firstEntryVisited = currentPriorityWindowElement;
                        currentTime = timeStamp;
                    }
                }
                //If timer event arrives, then reduce priority of the expired events
                else {
                    currentTime = timeStamp;
                    //begin from first node
                    PriorityWindowElement currentNode = firstEntryVisited;
                    while (currentNode != null) {
                        long currentLocation = currentNode.getLocation();
                        long currentPriority = currentNode.getPriority();
                        long currentTimeStamp = currentNode.getTimeStamp();
                        long amountToDecrement = (timeStamp - currentTimeStamp) / duration;
                        //need to decrement all elements passing until the exact event location
                        if (currentLocation <= eventLocation) {
                            //have to check all the successors whether they are satisfying given condition
                            if (currentNode.hasNext()) {
                                PriorityWindowElement nextNode = currentNode.getNextElement();
                                long nextElementLocation = nextNode.getLocation();
                                //currentNode is the place to stop reducing priority
                                if (nextElementLocation > eventLocation) {
                                    if (amountToDecrement != 0) {
                                        long newPriority = currentPriority - amountToDecrement;
                                        if (newPriority > 0) {
                                            managePositivePriority(newPriority, currentNode, returnEventChunk, timeStamp);
                                            currentPriorityWindowElement = currentNode;
                                            break;
                                        } else {
                                            removeFromMap(currentNode.getId(), currentPriority);
                                            PriorityWindowElement predecessor = getBeforeElement(currentNode);
                                            predecessor.setNextElement(nextNode);
                                            currentPriorityWindowElement = nextNode;
                                            break;
                                        }
                                    }
                                    break;
                                } else {
                                    long newPriority = currentPriority - amountToDecrement;
                                    if (newPriority > 0) {
                                        if (newPriority != currentPriority) {
                                            managePositivePriority(newPriority, currentNode, returnEventChunk, timeStamp);
                                        }
                                        currentNode = nextNode;
                                        continue;
                                    } else {
                                        removeFromMap(currentNode.getId(), currentPriority);
                                        PriorityWindowElement predecessor = getBeforeElement(currentNode);
                                        predecessor.setNextElement(nextNode);
                                        currentNode = nextNode;
                                        continue;
                                    }
                                }
                            }
                            //If no successor exists reduce priority and break
                            else {
                                if (amountToDecrement != 0) {
                                    long newPriority = currentPriority - amountToDecrement;
                                    if (newPriority > 0) {
                                        managePositivePriority(newPriority, currentNode, returnEventChunk, timeStamp);
                                        currentPriorityWindowElement = currentNode;
                                        break;
                                    } else {
                                        removeFromMap(currentNode.getId(), currentPriority);
                                        PriorityWindowElement predecessor = getBeforeElement(currentNode);
                                        predecessor.setNextElement(null);
                                        currentPriorityWindowElement = predecessor;
                                        break;
                                    }
                                }
                                currentPriorityWindowElement = currentNode;
                                break;
                            }
                        }
                        //event Location is smaller than first element but amount to reduce then go a round and reduce priority
                        else if (amountToDecrement != 0) {
                            long newPriority = currentPriority - amountToDecrement;
                            if (priority > 0 && newPriority != currentPriority) {
                                managePositivePriority(newPriority, currentNode, returnEventChunk, timeStamp);
                                currentNode = currentNode.getNextElement();
                                continue;
                            } else {
                                removeFromMap(currentNode.getId(), currentPriority);
                                PriorityWindowElement predecessor = getBeforeElement(currentNode);
                                if (currentNode.hasNext()) {
                                    if (predecessor != null) {
                                        predecessor.setNextElement(currentNode.getNextElement());
                                        currentNode = currentNode.getNextElement();
                                        continue;
                                    } else {
                                        currentNode = currentNode.getNextElement();
                                        continue;
                                    }
                                } else {
                                    if (predecessor != null) {
                                        predecessor.setNextElement(null);
                                        currentNode = predecessor;
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

            }
            if (returnEventChunk.getFirst() != null) {
                nextProcessor.process(returnEventChunk);
            }
        }
    }

    /**
     * The init method of the MaximumCliqueStreamProcessor,
     * this method will be called before other methods
     *
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     * @return the additional output attributes introduced by the function
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 3) {
            throw new UnsupportedOperationException("Invalid no of arguments passed to priority:PriorityWindowProcessor," + "required 3, but found" + attributeExpressionExecutors.length);
        } else {
            if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Invalid parameter found for the first parameter of priority:PriorityWindowProcessor, Required a variable, but found a constant parameter  " + attributeExpressionExecutors[0].getReturnType());
            } else {
                variableExpressionID = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            }
            if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Invalid parameter found for the second parameter of priority:PriorityWindowProcessor, Required a variable, but found a constant parameter " + attributeExpressionExecutors[1].getReturnType());
            } else {
                variableExpressionPriority = (VariableExpressionExecutor) attributeExpressionExecutors[1];
            }
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                    duration = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();

                } else if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                    duration = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new ExecutionPlanValidationException("PriorityWindowProcessor third parameter should be either int or long, but found " + attributeExpressionExecutors[2].getReturnType());
                }

            } else {
                throw new ExecutionPlanValidationException("PriorityWindowProcessor should have constant parameter attribute as third parameter but found a dynamic attribute " + attributeExpressionExecutors[2].getClass().getCanonicalName());
            }

        }


        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("changedEvents", Attribute.Type.STRING));
        return attributeList;
    }

    /**
     * This will be called only once and this can be used to acquire required resources for the
     * processing element.
     * This will be called after initializing the system and before starting to process the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once and this can be used to release the acquired resources
     * for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state at a different point of time
     *
     * @return stateful objects of the processing element as an array
     */
    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on the same order provided
     *              by currentState().
     */
    @Override
    public void restoreState(Object[] state) {

    }

    /**
     * Add priority entry to the map
     *
     * @param id       id of the PriorityWindowElement
     * @param priority Priority that needs to be added
     */
    private void addToMap(String id, long priority) {
         /*
             * If event is already available at priorityScoreMap
             */
        if (priorityScoreMap.containsKey(id)) {
            long currentScore = priorityScoreMap.get(id);
            long newScore = currentScore + priority;
            priorityScoreMap.put(id, newScore);
        }
        /*
         * Event is not there in the priorityScoreMap
         */
        else {
            priorityScoreMap.put(id, priority);
        }
    }

    /**
     * If priority goes to zero that amount is reduced from the map
     *
     * @param id              id of the PriorityWindowElement
     * @param currentPriority priority which should be reduced from the map
     */
    private void removeFromMap(String id, long currentPriority) {
        long currentScore = priorityScoreMap.get(id);
        long newScore = (currentScore - currentPriority);
        if (newScore > 0) {
            priorityScoreMap.put(id, newScore);
        } else {
            priorityScoreMap.remove(id);
        }
    }

    /**
     * if priority greater than zero and changed,then new value added to the map and event will added to notify priority change
     *
     * @param priority         new priority
     * @param element          PriorityWindowElement
     * @param returnEventChunk ComplexEventChunk which notify priority changes
     */
    public void managePositivePriority(long priority, PriorityWindowElement element, ComplexEventChunk<StreamEvent> returnEventChunk, long newTimeStamp) {
        String id = element.getId();
        element.setPriority(priority);
        element.setTimeStamp(newTimeStamp);
        addToMap(id, priority);
        StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(element.getEvent());
        scheduler.notifyAt(clonedEvent.getTimestamp() + duration);
        clonedEvent.setOutputData(new Object[]{priority});
        returnEventChunk.add(clonedEvent);
    }

    /**
     * New PriorityWindowElement will be added and notify as new priority event being added
     *
     * @param event            new event
     * @param id               id of the new PriorityWindowElement
     * @param priority         new priority
     * @param eventLocation    location at linked list
     * @param returnEventChunk ComplexEventChunk which notify priority changes
     * @param previousNode     predecessor of new event
     * @param timeStamp        time which event arrives
     */
    public void addNewNode(StreamEvent event, String id, Long priority, Long eventLocation, ComplexEventChunk<StreamEvent> returnEventChunk, PriorityWindowElement previousNode, long timeStamp) {
        PriorityWindowElement newElement = new PriorityWindowElement(event, id, priority, eventLocation, timeStamp);
        StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(event);
        scheduler.notifyAt(clonedEvent.getTimestamp() + duration);
        clonedEvent.setOutputData(new Object[]{priority});
        returnEventChunk.add(clonedEvent);
        previousNode.setNextElement(newElement);
        addToMap(id, priority);
        currentPriorityWindowElement = newElement;
    }

    /**
     * Find predecessor of a node in linked list
     *
     * @param nextElement Element which we want to find the predecessor
     * @return PriorityWindowElement
     */
    public PriorityWindowElement getBeforeElement(PriorityWindowElement nextElement) {
        PriorityWindowElement beforeElement = null;
        PriorityWindowElement currentNode = firstEntryVisited;
        while (currentNode != null) {
            if (currentNode.getNextElement() == nextElement) {
                beforeElement = currentNode;
            } else {
                currentNode = currentNode.getNextElement();
            }
        }
        return beforeElement;
    }

    /**
     * Set scheduler
     *
     * @param scheduler Scheduler object
     */
    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Get scheduler object
     *
     * @return Scheduler object
     */
    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }
}
