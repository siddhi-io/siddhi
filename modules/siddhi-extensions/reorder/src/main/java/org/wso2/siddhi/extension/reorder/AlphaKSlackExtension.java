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

package org.wso2.siddhi.extension.reorder;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.extension.reorder.utils.WindowCoverage;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;


/**
 * The following code conducts reordering of an out-of-order event stream.
 * This implements the Alpha K-Slack based disorder handling algorithm which was originally
 * described in http://dl.acm.org/citation.cfm?doid=2675743.2771828
 */
public class AlphaKSlackExtension extends StreamProcessor implements SchedulingProcessor {
    private long k = 0; //In the beginning the K is zero.
    private long largestTimestamp = 0; //Used to track the greatest timestamp of tuples seen so far.
    private TreeMap<Long, List<StreamEvent>> primaryTreeMap;
    private TreeMap<Long, List<StreamEvent>> secondaryTreeMap;
    private ExpressionExecutor timestampExecutor;
    private ExpressionExecutor correlationFieldExecutor;
    private long maxK = Long.MAX_VALUE;
    private long timerDuration = -1l;
    private boolean discardFlag = false;
    private long lastSentTimestamp = -1l;
    private Scheduler scheduler;
    private long lastScheduledTimestamp = -1l;
    private ReentrantLock lock = new ReentrantLock();
    private double previousAlpha = 0;
    private int counter = 0;
    private long batchSize = 10000;
    private double previousError = 0;
    private List<Double> dataItemList = new ArrayList<Double>();
    private List<Long> timestampList = new ArrayList<Long>();
    private double Kp = 0.5; // Weight configuration parameters
    private double Kd = 0.8;
    private boolean flag = true;
    private boolean timerFlag = true;
    private double errorThreshold = 0.03;
    private double confidenceLevel = 0.95;
    private double alpha = 1;

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
        return new Object[]{k, largestTimestamp, lastSentTimestamp, lastScheduledTimestamp,
                previousAlpha, counter, previousError, Kp, Kd, primaryTreeMap,
                secondaryTreeMap, dataItemList, timestampList};
    }

    @Override
    public void restoreState(Object[] state) {
        k = (Long) state[0];
        largestTimestamp = (Long) state[1];
        lastSentTimestamp = (Long) state[2];
        lastScheduledTimestamp = (Long) state[3];
        previousAlpha = (Double) state[4];
        counter = (Integer) state[5];
        previousError = (Double) state[6];
        Kp = (Double) state[7];
        Kd = (Double) state[8];
        primaryTreeMap = (TreeMap<Long, List<StreamEvent>>) state[9];
        secondaryTreeMap = (TreeMap<Long, List<StreamEvent>>) state[10];
        dataItemList = (List<Double>) state[11];
        timestampList = (List<Long>) state[12];
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);
        lock.lock();
        NormalDistribution actualDistribution = new NormalDistribution();

        double criticalValue = Math.abs(actualDistribution.inverseCumulativeProbability((1 - confidenceLevel) / 2));
        WindowCoverage obj = new WindowCoverage(errorThreshold);
        try {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();

                if (event.getType() != ComplexEvent.Type.TIMER) {
                    streamEventChunk.remove();
                    long timestamp = (Long) timestampExecutor.execute(event);
                    timestampList.add(timestamp);
                    double correlationField = (Double) correlationFieldExecutor.execute(event);
                    dataItemList.add(correlationField);
                    if (discardFlag) {
                        if (timestamp < lastSentTimestamp) {
                            continue;
                        }
                    }

                    if(timerFlag){
                        timerFlag = false;
                        lastScheduledTimestamp = lastScheduledTimestamp + timerDuration;
                        scheduler.notifyAt(lastScheduledTimestamp);
                    }

                    List<StreamEvent> eventList = primaryTreeMap.get(timestamp);
                    if (eventList == null) {
                        eventList = new ArrayList<StreamEvent>();
                        primaryTreeMap.put(timestamp, eventList);
                    }
                    eventList.add(event);
                    counter += 1;
                    if (counter > batchSize) {
                        long adjustedBatchsize = Math.round(batchSize * 0.75);
                        alpha = calculateAlpha(obj.calculateWindowCoverageThreshold(criticalValue,dataItemList),obj.calculateRuntimeWindowCoverage(timestampList,adjustedBatchsize));
                        counter = 0;
                        timestampList = new ArrayList<Long>();
                        dataItemList = new ArrayList<Double>();
                    }
                    if (timestamp > largestTimestamp) {
                        largestTimestamp = timestamp;
                        long minTimestamp = primaryTreeMap.firstKey();
                        long timeDifference = largestTimestamp - minTimestamp;
                        if (timeDifference > k) {
                            if (timeDifference < maxK) {
                                k = Math.round(timeDifference * alpha);
                            } else {
                                k = maxK;
                            }
                        }
// TODO: 20/12/16 put a comment to review the possiblity of hvng single data strcture instaed of two treemaps 
                        Iterator<Map.Entry<Long, List<StreamEvent>>> entryIterator =
                                primaryTreeMap.entrySet()
                                        .iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Long, List<StreamEvent>> entry = entryIterator.next();
                            List<StreamEvent> list = secondaryTreeMap.get(entry.getKey());
                            if (list != null) {
                                list.addAll(entry.getValue());
                            } else {
                                secondaryTreeMap.put(entry.getKey(),
                                        new ArrayList<StreamEvent>(entry.getValue()));
                            }
                        }
                        primaryTreeMap = new TreeMap<Long, List<StreamEvent>>();
                        entryIterator = secondaryTreeMap.entrySet().iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Long, List<StreamEvent>> entry = entryIterator.next();
                            if (entry.getKey() + k <= largestTimestamp) {
                                entryIterator.remove();
                                List<StreamEvent> timeEventList = entry.getValue();
                                lastSentTimestamp = entry.getKey();

                                for (StreamEvent aTimeEventList : timeEventList) {
                                    complexEventChunk.add(aTimeEventList);
                                }
                            }
                        }
                    }
                } else {
                    if (secondaryTreeMap.size() > 0) {
                        for (Map.Entry<Long, List<StreamEvent>> longListEntry : secondaryTreeMap.entrySet()) {
                            List<StreamEvent> timeEventList = longListEntry.getValue();

                            for (StreamEvent aTimeEventList : timeEventList) {
                                complexEventChunk.add(aTimeEventList);
                            }
                        }

                        secondaryTreeMap= new TreeMap<Long, List<StreamEvent>>();

                    }

                    if (primaryTreeMap.size() > 0) {
                        for (Map.Entry<Long, List<StreamEvent>> longListEntry : primaryTreeMap.entrySet()) {
                            List<StreamEvent> timeEventList = longListEntry.getValue();

                            for (StreamEvent aTimeEventList : timeEventList) {
                                complexEventChunk.add(aTimeEventList);
                            }
                        }

                        primaryTreeMap= new TreeMap<Long, List<StreamEvent>>();
                    }

                    timerFlag = true;
                }
            }
        } catch (ArrayIndexOutOfBoundsException ec) {
            //This happens due to user specifying an invalid field index.
            throw new ExecutionPlanCreationException("The very first parameter must be an " +
                    "Integer with a valid " +
                    " field index (0 to (fieldsLength-1)).");
        }
        // TODO: 20/12/16 need to check exception, if not use if, need to handle it in init 
        lock.unlock();
        nextProcessor.process(complexEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ExecutionPlanContext executionPlanContext) {
        List<Attribute> attributes = new ArrayList<Attribute>();

        if (attributeExpressionLength > 8 && attributeExpressionLength < 2
                || attributeExpressionLength == 7) {
            throw new ExecutionPlanCreationException("Maximum six input parameters " +
                    "and minimum two input parameters " +
                    "can be specified for AK-Slack. " +
                    " Timestamp (long), velocity (long), batchSize (long), timerTimeout " +
                    "(long), maxK (long), " +
                    "discardFlag (boolean), errorThreshold (double) and confidenceLevel " +
                    "(double)  fields. But found " +
                    attributeExpressionLength + " attributes.");
        }

        if (attributeExpressionExecutors.length >= 2) {
            flag = false;
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timestampExecutor = attributeExpressionExecutors[0];
                attributes.add(new Attribute("beta0", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for " +
                        "the first argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[0].getReturnType());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
                correlationFieldExecutor = attributeExpressionExecutors[1];
                attributes.add(new Attribute("beta1", Attribute.Type.DOUBLE));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for " +
                        "the second argument of " +
                        " reorder:akslack() function. Required DOUBLE, but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

        }
        if (attributeExpressionExecutors.length >= 3) {
            flag = true;
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                    attributes.add(new Attribute("beta2", Attribute.Type.LONG));
                    batchSize = (Long) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new ExecutionPlanCreationException("Invalid parameter type found " +
                            "for the third argument of " +
                            " reorder:akslack() function. Required LONG, but found " +
                            attributeExpressionExecutors[2].getReturnType());
                }
            } else {
                throw new ExecutionPlanCreationException("Batch size parameter must be a constant.");
            }

        }
        if (attributeExpressionExecutors.length >= 4) {
            if(attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.LONG) {
                    timerDuration = (Long) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[3]).getValue();
                    attributes.add(new Attribute("beta3", Attribute.Type.LONG));
                } else {
                    throw new ExecutionPlanCreationException("Invalid parameter type found for " +
                            "the fourth argument of " +
                            " reorder:akslack() function. Required LONG, but found " +
                            attributeExpressionExecutors[3].getReturnType());
                }
            }else{
                throw new ExecutionPlanCreationException("timerDuration must be a constant");
            }
        }
        if (attributeExpressionExecutors.length >= 5) {
            if(attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.LONG) {
                    maxK = (Long) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[4]).getValue();
                    if (maxK == -1) {
                        maxK = Long.MAX_VALUE;
                    }
                    attributes.add(new Attribute("beta4", Attribute.Type.LONG));
                } else {
                    throw new ExecutionPlanCreationException("Invalid parameter type found " +
                            "for the fifth argument of " +
                            " reorder:akslack() function. Required LONG, but found " +
                            attributeExpressionExecutors[4].getReturnType());
                }
            }else{
                throw new ExecutionPlanCreationException("maxK must be a constant");
            }
        }
        if (attributeExpressionExecutors.length >= 6) {
            if(attributeExpressionExecutors[5] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[5].getReturnType() == Attribute.Type.BOOL) {
                    discardFlag = (Boolean) attributeExpressionExecutors[5].execute(null);
                    attributes.add(new Attribute("beta5", Attribute.Type.BOOL));
                } else {
                    throw new ExecutionPlanCreationException("Invalid parameter type found " +
                            "for the sixth argument of " +
                            " reorder:akslack() function. Required BOOL, but found " +
                            attributeExpressionExecutors[5].getReturnType());
                }
            }else{
                throw new ExecutionPlanCreationException("discardFlag must be a constant");
            }
        }
        if (attributeExpressionExecutors.length == 8) {
            if((attributeExpressionExecutors[6] instanceof ConstantExpressionExecutor) && attributeExpressionExecutors[7] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[6].getReturnType() == Attribute.Type.DOUBLE) {
                    errorThreshold = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[6]).getValue();
                    attributes.add(new Attribute("beta6", Attribute.Type.DOUBLE));
                } else {
                    throw new ExecutionPlanCreationException("Invalid parameter type found " +
                            "for the seventh argument of " +
                            " reorder:akslack() function. Required DOUBLE, but found " +
                            attributeExpressionExecutors[6].getReturnType());
                }
                if (attributeExpressionExecutors[7].getReturnType() == Attribute.Type.DOUBLE) {
                    confidenceLevel = (Double) ((ConstantExpressionExecutor)attributeExpressionExecutors[7]).getValue();
                    attributes.add(new Attribute("beta6", Attribute.Type.DOUBLE));
                } else {
                    throw new ExecutionPlanCreationException("Invalid parameter type found for " +
                            "the eighth argument of " +
                            " reorder:akslack() function. Required DOUBLE, but found " +
                            attributeExpressionExecutors[7].getReturnType());
                }
            }else{
                throw new ExecutionPlanCreationException("errorThreshold and confidenceLevel must be constants");
            }
        }

        primaryTreeMap = new TreeMap<Long, List<StreamEvent>>();
        secondaryTreeMap = new TreeMap<Long, List<StreamEvent>>();

        return attributes;
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        if (lastScheduledTimestamp < 0 && flag) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() +
                    timerDuration;
            scheduler.notifyAt(lastScheduledTimestamp);
        }
    }

    private double calculateAlpha(double windowCoverageThreshold, double runtimeWindowCoverage) {
        double error = windowCoverageThreshold - runtimeWindowCoverage;
        double deltaAlpha = (Kp * error) + (Kd * (error - previousError));
        double alpha = Math.abs(previousAlpha + deltaAlpha);
        previousError = error;
        previousAlpha = alpha;
        return alpha;
    }
}
