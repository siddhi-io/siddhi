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
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bhagya on 8/3/16.
 */
public class PriorityWindowProcessor extends StreamProcessor {
    private VariableExpressionExecutor variableExpressionID;
    private VariableExpressionExecutor variableExpressionPriority;
    private long duration;
    long currentTime = 0L;
    private PriorityWindowElement currentPriorityWindowElement = null;
    private PriorityWindowElement firstEntryVisited = null;
    Map<String, Long> priorityScoreMap = new HashMap<String, Long>();


    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>(false);
        long timeStamp = executionPlanContext.getTimestampGenerator().currentTime();
        /*
         * priority reduction amount calculation
         */
        long amountToDecrement = ((timeStamp - currentTime) / duration);
        /*
          * get modulus value of the time stamp
          */
        long eventLocation = timeStamp % duration;
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                String id = (String) variableExpressionID.execute(event);
                long priority;
                if (variableExpressionPriority.execute(event) instanceof Long) {
                    priority = (Long) variableExpressionPriority.execute(event);
                } else {
                    priority = (Integer) variableExpressionPriority.execute(event);
                }
//  If events are already visited
                if (currentPriorityWindowElement != null) {
                    /*
                     * Update current time
                     */
                    currentTime = timeStamp;
                    while (true) {
                        StreamEvent currentEvent = currentPriorityWindowElement.getEvent();
                        long currentLocation = currentPriorityWindowElement.getLocation();
                        long currentPriority = currentPriorityWindowElement.getPriority();
                        String currentID = currentPriorityWindowElement.getId();
                        /*
                         * current event is the place to add the new event
                         */
                        if (currentLocation <= eventLocation) {
                            long newPriority = currentPriority - amountToDecrement;
                            currentPriorityWindowElement.setPriority(newPriority);
                            priorityScoreMap.put(currentID, newPriority);
                            PriorityWindowElement nextElement = new PriorityWindowElement(event, id, priority, eventLocation);
                            currentPriorityWindowElement.setNextElement(nextElement);
                            returnEventChunk.add(currentEvent);
                            returnEventChunk.add(event);
                            if (priorityScoreMap.containsKey(id)) {
                                long currentScore = priorityScoreMap.get(id);
                                long newScore = currentScore + priority;
                                priorityScoreMap.put(id, newScore);
                            } else {
                                priorityScoreMap.put(id, priority);
                            }
                            currentPriorityWindowElement = nextElement;
                            break;
                        }

// When current event is not the place and it is not the last event arrived
                        else if (currentPriorityWindowElement.getNextElement() != null) {
                            PriorityWindowElement nextElement = currentPriorityWindowElement.getNextElement();
                            StreamEvent nextElementEvent = nextElement.getEvent();
                            long nextElementLocation = nextElement.getLocation();
                            long nextElementPriority = nextElement.getPriority();
                            String nextElementId = nextElement.getId();
/*
 * If arrived event has to place as the next event of the current event
 */

                            if (nextElementLocation <= eventLocation) {
                                PriorityWindowElement newElement = new PriorityWindowElement(event, id, priority, eventLocation);
                                returnEventChunk.add(event);
                                currentPriorityWindowElement.setNextElement(newElement);
                                newElement.setNextElement(nextElement);
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
                                currentPriorityWindowElement = newElement;
                                break;

                            }
/*
 * If next event is not the place to add arrived event
 */
                            else {
                                /*
                                 * need to reduce priority of each and every event we are passing
                                 */
                                long newPriority = nextElementPriority - amountToDecrement;
                                /*
                                 * If priority is a positive value and priority has changed then update priority
                                 */

                                if (newPriority > 0) {
                                    nextElement.setPriority(newPriority);
                                    long currentScore = priorityScoreMap.get(nextElementId);
                                    long newScore = currentScore - amountToDecrement;
                                    /*
                                     * Meantime update priorityScoreMap
                                     */
                                    priorityScoreMap.put(nextElementId, newScore);
                                    returnEventChunk.add(nextElementEvent);
                                    currentPriorityWindowElement = nextElement;
                                    continue;
                                }
                                /*
                                 * If priority goes to 0 or negative
                                 */
                                else {
                                    /*
                                     * Get map and calculate new score, if that goes to negative remove event from map
                                     */
                                    long currentScore = priorityScoreMap.get(nextElementId);
                                    long newScore = (currentScore - nextElementPriority);
                                    if (newScore > 0) {
                                        priorityScoreMap.put(nextElementId, newPriority);
                                    } else {
                                        priorityScoreMap.remove(nextElementId);
                                    }
                                    /*
                                    * if the element has next element make that the next element of the current element
                                     */
                                    if (nextElement.getNextElement() != null) {
                                        PriorityWindowElement newNextElement = nextElement.getNextElement();
                                        currentPriorityWindowElement.setNextElement(newNextElement);
                                        currentPriorityWindowElement = newNextElement;
                                        continue;
                                    }
                                    /*
                                     * If it doesn't have any following event set next element to null
                                     */
                                    else {
                                        currentPriorityWindowElement.setNextElement(null);
                                        currentPriorityWindowElement = firstEntryVisited;
                                        continue;
                                    }
                                }


                            }


                        }

// If current event is the last event arrived and it is the exact place where next event should be added
                        else if (currentPriorityWindowElement.getLocation() <= eventLocation && amountToDecrement == 0) {
                            PriorityWindowElement nextElement = new PriorityWindowElement(event, id, priority, eventLocation);
                            returnEventChunk.add(event);
                            currentPriorityWindowElement.setNextElement(nextElement);
                            if (priorityScoreMap.containsKey(id)) {
                                long currentScore = priorityScoreMap.get(id);
                                long newScore = currentScore + priority;
                                priorityScoreMap.put(id, newScore);
                            } else {
                                priorityScoreMap.put(id, priority);
                            }
                            currentPriorityWindowElement = nextElement;
                            break;

                        }
                        /*
                         * When current event is the last event arrived and it is the only one event available
                         */
                        else if (currentLocation >= eventLocation && currentPriorityWindowElement.equals(firstEntryVisited)) {

                            long newPriority = currentPriority - amountToDecrement;
                            currentPriorityWindowElement.setPriority(newPriority);
                            priorityScoreMap.put(currentID, newPriority);
                            PriorityWindowElement previousElement = new PriorityWindowElement(event, id, priority, eventLocation);
                            previousElement.setNextElement(currentPriorityWindowElement);
                            returnEventChunk.add(currentEvent);
                            returnEventChunk.add(event);
                            if (priorityScoreMap.containsKey(id)) {
                                long currentScore = priorityScoreMap.get(id);
                                long newScore = currentScore + priority;
                                priorityScoreMap.put(id, newScore);
                            } else {
                                priorityScoreMap.put(id, priority);
                            }
                            currentPriorityWindowElement = previousElement;
                            break;
                        }
// Current event is the last event arrived but it is not the place to add the event
                        else {
                            currentPriorityWindowElement = firstEntryVisited;
                            continue;
                        }

                    }
// First event arrived
                } else {
                    currentPriorityWindowElement = new PriorityWindowElement(event, id, priority, eventLocation);
                    priorityScoreMap.put(id, priority);
                    returnEventChunk.add(event);
                    firstEntryVisited = currentPriorityWindowElement;
                    currentTime = timeStamp;
                }

            }
            if (returnEventChunk.getFirst() != null) {
                nextProcessor.process(returnEventChunk);
            }
        }
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (!(attributeExpressionExecutors.length == 3)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        } else {
            if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Required a variable, but found a other parameter");
            } else {
                variableExpressionID = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            }
            if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Required a variable, but found a other parameter");
            } else {
                variableExpressionPriority = (VariableExpressionExecutor) attributeExpressionExecutors[1];
            }
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                    duration = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();

                } else if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                    duration = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new ExecutionPlanValidationException("Time window's parameter attribute should be either int or long, but found " + attributeExpressionExecutors[0].getReturnType());
                }

            } else {
                throw new ExecutionPlanValidationException("Time window should have constant parameter attribute but found a dynamic attribute " + attributeExpressionExecutors[2].getClass().getCanonicalName());
            }

        }


        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("changedEvents", Attribute.Type.STRING));
        return attributeList;

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {

    }
}
