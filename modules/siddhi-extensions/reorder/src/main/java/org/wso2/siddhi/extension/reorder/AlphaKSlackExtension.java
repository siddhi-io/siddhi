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
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.extension.reorder.alphakslack.Runtime;
import org.wso2.siddhi.extension.reorder.alphakslack.ThetaThreshold;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;


/**
 * The following code conducts reordering of an out-of-order event stream.
 * This implements the Adaptive K-Slack based disorder handling algorithm which was originally described in
 * http://dl.acm.org/citation.cfm?doid=2675743.2771828
 */
public class AlphaKSlackExtension extends StreamProcessor implements SchedulingProcessor {
    private long k = 0; //In the beginning the K is zero.
    private long greatestTimestamp = 0; //Used to track the greatest timestamp of tuples seen so far.
    private TreeMap<Long, ArrayList<StreamEvent>> eventTreeMap;
    private TreeMap<Long, ArrayList<StreamEvent>> expiredEventTreeMap;
    private ExpressionExecutor timestampExecutor;
    private ExpressionExecutor dataItemExecutor;
    private ExpressionExecutor batchSizeExecutor;
    private long MAX_K = Long.MAX_VALUE;
    private long TIMER_DURATION = -1l;
    private boolean expireFlag = false;
    private long lastSentTimeStamp = -1l;
    private Scheduler scheduler;
    private long lastScheduledTimestamp = -1l;
    private ReentrantLock lock = new ReentrantLock();
    private double alpha=0;
    private int counter =0;
    private long batchSize = -1;
    private int NANOSECOND = 1000000000;
    private ArrayList<Long> timeStampList = new ArrayList<Long>();
    private ArrayList<Long> dataItemList = new ArrayList<Long>();
    private LinkedHashSet<Long> eventTimeStamps = new LinkedHashSet<Long>();
    private ArrayList errorList = new ArrayList();
    private int count2=0;
    private double differenceError;
    private double windowCoverage=1;
    private double thetaThreshold=0;
    private ArrayList dataList = new ArrayList();
    private LinkedHashSet bufferSize = new LinkedHashSet();
    private double Kp,Kd;
    private boolean flag = true;

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
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {
        //Do nothing
    }


    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);
        lock.lock();

        Runtime runTime = new Runtime();
        ThetaThreshold thetaThreshold = new ThetaThreshold(0.2, 0.5);

        try {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();

                if(event.getType() != ComplexEvent.Type.TIMER) {
                    streamEventChunk.remove();

                    long timestamp = (Long) timestampExecutor.execute(event);
                    timeStampList.add(timestamp);

                    if(batchSize == -1l){
                        batchSize = (Long) batchSizeExecutor.execute(event);
                    }

//                    if(attributeExpressionExecutors.length == 2) {
//                        batchSize = 10000;
//                    } else{
//                        batchSize = (Long) batchSizeExecutor.execute(event);
//                    }

                    /*if((Long) batchSizeExecutor.execute(event)!= 0) {
                        batchSize = (Long) batchSizeExecutor.execute(event);
                    } else{
                        batchSize = 10000;
                    }*/

                    if (expireFlag) {
                        if (timestamp < lastSentTimeStamp) {
                            continue;
                        }
                    }

                    ArrayList<StreamEvent> eventList = eventTreeMap.get(timestamp);
                    if (eventList == null) {
                        eventList = new ArrayList<StreamEvent>();
                    }
                    eventList.add(event);
                    eventTreeMap.put(timestamp, eventList);

                    counter+=1;
                    long dataItem = (Long) dataItemExecutor.execute(event);
                    dataItemList.add(dataItem);

                    if(counter>batchSize){
                        Iterator<Long> itr = timeStampList.iterator();
                        while(itr.hasNext()){
                            long data = itr.next()/NANOSECOND;
                            eventTimeStamps.add(data);
                        }
                        windowCoverage = runTime.calculateWindowCoverage(eventTimeStamps);
                        double criticalValue = thetaThreshold.calculateCriticalValue();

                        Iterator itr2 = dataItemList.iterator();
                        while(itr2.hasNext()){
                            dataList.add(itr2.next());
                        }
                        this.thetaThreshold = thetaThreshold.calculateThetaThreshold(criticalValue, thetaThreshold.
                                        calculateMean(dataList),
                                thetaThreshold.calculateVariance(dataList));

                        double error = this.thetaThreshold - windowCoverage;
                        errorList.add(error);

                        if(count2==0){
                            differenceError = error;
                        } else {
                            differenceError = error - (Double) errorList.get(errorList.indexOf(error) - 1);
                        }

                        Kp = 0.1;
                        Kd = 0.1;

                        alpha = Math.abs(Kp*error + Kd*differenceError);
                        timeStampList = new ArrayList<Long>();
                        dataItemList = new ArrayList<Long>();
                        eventTimeStamps = new LinkedHashSet<Long>();
                        counter = 0;
                        count2 += 1;
                    }

                    if (timestamp > greatestTimestamp) {
                        greatestTimestamp = timestamp;
                        long minTimestamp = eventTreeMap.firstKey();
                        long timeDifference = greatestTimestamp - minTimestamp;
                        if (timeDifference > k) {
                            if (timeDifference < MAX_K) {
                                if (count2!=0) {
                                    k = (Math.round(timeDifference * alpha));
                                } else {
                                    k = timeDifference;
                                }
                            } else {
                                if (count2!= 0) {
                                    k = Math.round(MAX_K * alpha);
                                } else {
                                    k = MAX_K;
                                }
                            }
                        }
                        bufferSize.add(k);

                        Iterator<Map.Entry<Long, ArrayList<StreamEvent>>> entryIterator = eventTreeMap.entrySet()
                                .iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Long, ArrayList<StreamEvent>> entry = entryIterator.next();
                            ArrayList<StreamEvent> list = expiredEventTreeMap.get(entry.getKey());

                            if (list != null) {
                                list.addAll(entry.getValue());
                            } else {
                                expiredEventTreeMap.put(entry.getKey(), entry.getValue());
                            }
                        }
                        eventTreeMap = new TreeMap<Long, ArrayList<StreamEvent>>();

                        entryIterator = expiredEventTreeMap.entrySet().iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Long, ArrayList<StreamEvent>> entry = entryIterator.next();
                            if (entry.getKey() + k <= greatestTimestamp) {
                                entryIterator.remove();
                                ArrayList<StreamEvent> timeEventList = entry.getValue();
                                lastSentTimeStamp = entry.getKey();

                                for (StreamEvent aTimeEventList : timeEventList) {
                                    complexEventChunk.add(aTimeEventList);
                                }
                            }
                        }
                    }
                } else {
                    if(expiredEventTreeMap.size() > 0) {
                        TreeMap<Long, ArrayList<StreamEvent>> expiredEventTreeMapSnapShot = expiredEventTreeMap;
                        expiredEventTreeMap = new TreeMap<Long, ArrayList<StreamEvent>>();
                        onTimerEvent(expiredEventTreeMapSnapShot, nextProcessor);
                            lastScheduledTimestamp = lastScheduledTimestamp + TIMER_DURATION;
                            scheduler.notifyAt(lastScheduledTimestamp);
                    }
                }
            }
        } catch (ArrayIndexOutOfBoundsException ec) {
            //This happens due to user specifying an invalid field index.
            throw new ExecutionPlanCreationException("The very first parameter must be an Integer with a valid " +
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

        if (attributeExpressionLength > 6 && attributeExpressionLength<2) {
            throw new ExecutionPlanCreationException("Maximum six input parameters and minimum two input parameters " +
                    "can be specified for AK-Slack. " +
                    " Timestamp (long), velocity (long), batchSize (long), timerTimeout (long), maxK (long), " +
                    "and discardFlag (boolean)  fields. But found " +
                    attributeExpressionLength + " attributes.");
        }

        if(attributeExpressionExecutors.length == 2){
            flag = false;
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timestampExecutor = attributeExpressionExecutors[0];
                attributes.add(new Attribute("beta0", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the first argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[0].getReturnType());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                dataItemExecutor = attributeExpressionExecutors[1];
                attributes.add(new Attribute("beta1", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the second argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            batchSize = 10000;
        }else if(attributeExpressionExecutors.length == 3){
            flag = false;
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timestampExecutor = attributeExpressionExecutors[0];
                attributes.add(new Attribute("beta0", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the first argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[0].getReturnType());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                dataItemExecutor = attributeExpressionExecutors[1];
                attributes.add(new Attribute("beta1", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the second argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                batchSizeExecutor = attributeExpressionExecutors[2];
                attributes.add(new Attribute("beta2", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the third argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }
        }else if(attributeExpressionExecutors.length == 4){
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timestampExecutor = attributeExpressionExecutors[0];
                attributes.add(new Attribute("beta0", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the first argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[0].getReturnType());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                dataItemExecutor = attributeExpressionExecutors[1];
                attributes.add(new Attribute("beta1", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the second argument of " +
                        " reorder:akslack() function. Required INT, but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                batchSizeExecutor = attributeExpressionExecutors[2];
                attributes.add(new Attribute("beta2", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the third argument of " +
                        " reorder:akslack() function. Required INT, but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }
            if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.LONG) {
                TIMER_DURATION = (Long)attributeExpressionExecutors[3].execute(null);
                attributes.add(new Attribute("beta3", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the fourth argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[3].getReturnType());
            }
        }else if(attributeExpressionExecutors.length == 5){
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timestampExecutor = attributeExpressionExecutors[0];
                attributes.add(new Attribute("beta0", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the first argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[0].getReturnType());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                dataItemExecutor = attributeExpressionExecutors[1];
                attributes.add(new Attribute("beta1", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the second argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                batchSizeExecutor = attributeExpressionExecutors[2];
                attributes.add(new Attribute("beta2", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the third argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }
            if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.LONG) {
                TIMER_DURATION = (Long)attributeExpressionExecutors[3].execute(null);
                attributes.add(new Attribute("beta3", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the fourth argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[3].getReturnType());
            }
            if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.LONG) {
                TIMER_DURATION = (Long)attributeExpressionExecutors[4].execute(null);
                attributes.add(new Attribute("beta4", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the fifth argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[4].getReturnType());
            }
        }else if(attributeExpressionExecutors.length == 6){
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timestampExecutor = attributeExpressionExecutors[0];
                attributes.add(new Attribute("beta0", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the first argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[0].getReturnType());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                dataItemExecutor = attributeExpressionExecutors[1];
                attributes.add(new Attribute("beta1", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the second argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                batchSizeExecutor = attributeExpressionExecutors[2];
                attributes.add(new Attribute("beta2", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the third argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }
            if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.LONG) {
                TIMER_DURATION = (Long)attributeExpressionExecutors[3].execute(null);
                attributes.add(new Attribute("beta3", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the fourth argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[3].getReturnType());
            }
            if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.LONG) {
                TIMER_DURATION = (Long)attributeExpressionExecutors[4].execute(null);
                attributes.add(new Attribute("beta4", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the fifth argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[4].getReturnType());
            }
            if (attributeExpressionExecutors[5].getReturnType() == Attribute.Type.BOOL) {
                expireFlag = (Boolean) attributeExpressionExecutors[5].execute(null);
                attributes.add(new Attribute("beta5", Attribute.Type.BOOL));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the sixth argument of " +
                        " reorder:akslack() function. Required BOOL, but found " +
                        attributeExpressionExecutors[5].getReturnType());
            }
        }

        eventTreeMap = new TreeMap<Long, ArrayList<StreamEvent>>();
        expiredEventTreeMap = new TreeMap<Long, ArrayList<StreamEvent>>();

        if(TIMER_DURATION != -1l && scheduler != null) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() + TIMER_DURATION;
            scheduler.notifyAt(lastScheduledTimestamp);
        }

        return attributes;
    }
    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        if (lastScheduledTimestamp < 0 && flag) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() + TIMER_DURATION;
            scheduler.notifyAt(lastScheduledTimestamp);
        }
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    private void onTimerEvent(TreeMap<Long, ArrayList<StreamEvent>> treeMap, Processor nextProcessor) {
        Iterator<Map.Entry<Long, ArrayList<StreamEvent>>> entryIterator = treeMap.entrySet().iterator();
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);

        while (entryIterator.hasNext()) {
            ArrayList<StreamEvent> timeEventList = entryIterator.next().getValue();

            for (StreamEvent aTimeEventList : timeEventList) {
                complexEventChunk.add(aTimeEventList);
            }
        }
        nextProcessor.process(complexEventChunk);
    }
}
