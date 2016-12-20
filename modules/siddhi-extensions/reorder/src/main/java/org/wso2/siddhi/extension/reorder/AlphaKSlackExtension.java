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
import org.wso2.siddhi.extension.reorder.utils.Runtime;
import org.wso2.siddhi.extension.reorder.utils.ThetaThreshold;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;


/**
 * The following code conducts reordering of an out-of-order event stream.
 * This implements the Adaptive K-Slack based disorder handling algorithm which was originally
 * described in http://dl.acm.org/citation.cfm?doid=2675743.2771828
 */
public class AlphaKSlackExtension extends StreamProcessor implements SchedulingProcessor {
    private long k = 0; //In the beginning the K is zero.
    private long greatestTimestamp = 0; //Used to track the greatest timestamp of tuples seen so far.
    private TreeMap<Long, List<StreamEvent>> eventTreeMap;
    private TreeMap<Long, List<StreamEvent>> expiredEventTreeMap;
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
    private double Kp, Kd; // Weight configuration parameters
    private boolean flag = true;
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
        return new Object[]{k, greatestTimestamp, maxK, timerDuration, discardFlag,
                lastSentTimestamp, lastScheduledTimestamp, previousAlpha, counter, batchSize,
                previousError, Kp, Kd, errorThreshold, confidenceLevel, eventTreeMap,
                expiredEventTreeMap};
    }

    @Override
    public void restoreState(Object[] state) {
        eventTreeMap = new TreeMap<Long, List<StreamEvent>>();
        expiredEventTreeMap = new TreeMap<Long, List<StreamEvent>>();
        k = (Long) state[0];
        greatestTimestamp = (Long) state[1];
        maxK = (Long) state[2];
        timerDuration = (Long) state[3];
        discardFlag = (Boolean) state[4];
        lastSentTimestamp = (Long) state[5];
        lastScheduledTimestamp = (Long) state[6];
        previousAlpha = (Double) state[7];
        counter = (Integer) state[8];
        batchSize = (Long) state[9];
        previousError = (Double) state[10];
        Kp = (Double) state[11];
        Kd = (Double) state[12];
        errorThreshold = (Double) state[13];
        confidenceLevel = (Double) state[14];
        eventTreeMap = (TreeMap<Long,List<StreamEvent>>) state[15];
        expiredEventTreeMap = (TreeMap<Long,List<StreamEvent>>) state[16];
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);
        lock.lock();


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

                    List<StreamEvent> eventList = eventTreeMap.get(timestamp);
                    if (eventList == null) {
                        eventList = new ArrayList<StreamEvent>();
                        eventTreeMap.put(timestamp, eventList);
                    }
                    eventList.add(event);
                    counter += 1;
                    if (counter > batchSize) {
                        long adjustedBatchsize = Math.round(batchSize*0.25);
                        alpha = calculateAlpha(adjustedBatchsize,timestampList,dataItemList);
                        counter = 0;
                        timestampList = new ArrayList<Long>();
                        dataItemList = new ArrayList<Double>();
                    }

                    if (timestamp > greatestTimestamp) {
                        greatestTimestamp = timestamp;
                        long minTimestamp = eventTreeMap.firstKey();
                        long timeDifference = greatestTimestamp - minTimestamp;
                        if (timeDifference > k) {
                            if (timeDifference < maxK) {
                                k = Math.round(timeDifference * alpha);
                            } else {
                                k = maxK;
                            }
                        }

                        Iterator<Map.Entry<Long, List<StreamEvent>>> entryIterator =
                                eventTreeMap.entrySet()
                                .iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Long, List<StreamEvent>> entry = entryIterator.next();
                            List<StreamEvent> list = expiredEventTreeMap.get(entry.getKey());
                            if (list != null) {
                                list.addAll(entry.getValue());
                            } else {
                                expiredEventTreeMap.put(entry.getKey(),
                                        new ArrayList<StreamEvent>(entry.getValue()));
                            }
                        }
                        eventTreeMap = new TreeMap<Long, List<StreamEvent>>();

                        entryIterator = expiredEventTreeMap.entrySet().iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Long, List<StreamEvent>> entry = entryIterator.next();
                            if (entry.getKey() + k <= greatestTimestamp) {
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
                    if (expiredEventTreeMap.size() > 0) {
                        Iterator<Map.Entry<Long, List<StreamEvent>>> entryIterator =
                                expiredEventTreeMap.entrySet().iterator();

                        while (entryIterator.hasNext()) {
                            List<StreamEvent> timeEventList = entryIterator.next().getValue();

                            for (StreamEvent aTimeEventList : timeEventList) {
                                complexEventChunk.add(aTimeEventList);
                            }
                        }
                    }

                    if (eventTreeMap.size() > 0) {
                        Iterator<Map.Entry<Long, List<StreamEvent>>> entryIterator =
                                eventTreeMap.entrySet().iterator();
                        while (entryIterator.hasNext()) {
                            List<StreamEvent> timeEventList = entryIterator.next().getValue();

                            for (StreamEvent aTimeEventList : timeEventList) {
                                complexEventChunk.add(aTimeEventList);
                            }
                        }
                    }

                    lastScheduledTimestamp = lastScheduledTimestamp + timerDuration;
                    scheduler.notifyAt(lastScheduledTimestamp);
                    nextProcessor.process(complexEventChunk);
                }
            }
        } catch (ArrayIndexOutOfBoundsException ec) {
            //This happens due to user specifying an invalid field index.
            throw new ExecutionPlanCreationException("The very first parameter must be an " +
                    "Integer with a valid " +
                    " field index (0 to (fieldsLength-1)).");
        }
        lock.unlock();
        nextProcessor.process(complexEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ExecutionPlanContext executionPlanContext) {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();

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
            flag = false;
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
            if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.LONG) {
                timerDuration = (Long) attributeExpressionExecutors[3].execute(null);
                attributes.add(new Attribute("beta3", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for " +
                        "the fourth argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[3].getReturnType());
            }

        }
        if (attributeExpressionExecutors.length >= 5) {
            if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.LONG) {
                maxK = (Long) attributeExpressionExecutors[4].execute(null);
                if(maxK == -1){
                    maxK = Long.MAX_VALUE;
                }
                attributes.add(new Attribute("beta4", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found " +
                        "for the fifth argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[4].getReturnType());
            }
        }
        if (attributeExpressionExecutors.length >= 6) {
            if (attributeExpressionExecutors[5].getReturnType() == Attribute.Type.BOOL) {
                discardFlag = (Boolean) attributeExpressionExecutors[5].execute(null);
                attributes.add(new Attribute("beta5", Attribute.Type.BOOL));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found " +
                        "for the sixth argument of " +
                        " reorder:akslack() function. Required BOOL, but found " +
                        attributeExpressionExecutors[5].getReturnType());
            }
        }
        if (attributeExpressionExecutors.length == 8) {
            if (attributeExpressionExecutors[6].getReturnType() == Attribute.Type.DOUBLE) {
                errorThreshold = (Double) attributeExpressionExecutors[6].execute(null);
                attributes.add(new Attribute("beta6", Attribute.Type.DOUBLE));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found " +
                        "for the seventh argument of " +
                        " reorder:akslack() function. Required DOUBLE, but found " +
                        attributeExpressionExecutors[6].getReturnType());
            }
            if (attributeExpressionExecutors[7].getReturnType() == Attribute.Type.DOUBLE) {
                confidenceLevel = (Double) attributeExpressionExecutors[7].execute(null);
                attributes.add(new Attribute("beta6", Attribute.Type.DOUBLE));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for " +
                        "the eighth argument of " +
                        " reorder:akslack() function. Required DOUBLE, but found " +
                        attributeExpressionExecutors[7].getReturnType());
            }
        }

        eventTreeMap = new TreeMap<Long, List<StreamEvent>>();
        expiredEventTreeMap = new TreeMap<Long, List<StreamEvent>>();

        if (timerDuration != -1l && scheduler != null) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() +
                    timerDuration;
            scheduler.notifyAt(lastScheduledTimestamp);
        }
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

    private double calculateAlpha(long batchSize, List<Long> timestampList,
                                  List<Double> dataItemList){
        Runtime runTime = new Runtime();
        ThetaThreshold thetaThreshold = new ThetaThreshold(errorThreshold, 1 - confidenceLevel);
        double windowCoverage = runTime.calculateWindowCoverage(timestampList,
                Math.round(batchSize * 0.75));
        double criticalValue = thetaThreshold.calculateCriticalValue();
        double thresholdValue = thetaThreshold.calculateThetaThreshold(criticalValue,
                thetaThreshold.calculateMean(dataItemList),
                thetaThreshold.calculateVariance(dataItemList));

        double error = thresholdValue - windowCoverage;
        Kp = 0.5;
        Kd = 0.8;
        double alpha;
        double deltaAlpha = (Kp * error) + (Kd * (error - previousError));
        alpha = Math.abs(previousAlpha + deltaAlpha);
        previousError = error;
        previousAlpha = alpha;
        return alpha;
    }
}
