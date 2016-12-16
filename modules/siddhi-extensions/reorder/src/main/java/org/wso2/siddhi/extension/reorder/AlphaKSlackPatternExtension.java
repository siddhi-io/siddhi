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
import org.wso2.siddhi.query.api.expression.condition.In;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;


/**
 * The following code conducts reordering of an out-of-order event stream.
 * This implements the Adaptive K-Slack based disorder handling algorithm which was originally described in
 * http://dl.acm.org/citation.cfm?doid=2675743.2771828
 */
public class AlphaKSlackPatternExtension extends StreamProcessor implements SchedulingProcessor {
    private long k = 0; //In the beginning the K is zero.
    private long greatestTimestamp = 0; //Used to track the greatest timestamp of tuples seen so far.
    private TreeMap<Long, ArrayList<StreamEvent>> eventTreeMap;
    private TreeMap<Long, ArrayList<StreamEvent>> expiredEventTreeMap;
    private ExpressionExecutor timestampExecutor;
    private ExpressionExecutor correlationFieldExecutor;
    private ExpressionExecutor queryExecutor;
    private ExpressionExecutor batchSizeExecutor;
    private long maxK = Long.MAX_VALUE;
    private String query;
    private boolean discardFlag = false;
    private long lastSentTimestamp = -1l;
    private Scheduler scheduler;
    private long lastScheduledTimestamp = -1l;
    private long timerDuration = -1l;
    private ReentrantLock lock = new ReentrantLock();
    private double alpha = 1;
    private double previousAlpha = 0;
    private int counter = 0;
    private long batchSize = 10000;
    private double previousError = 0;
    private List<Double> dataItemList = new ArrayList<Double>();
    private Set<Long> eventTimestamps = new LinkedHashSet<Long>();
    private double Kp, Kd; // Weight configuration parameters
    private boolean flag = true;
    LinkedHashSet buffersize = new LinkedHashSet();
    LinkedHashSet eventTimeStamps = new LinkedHashSet();
    ArrayList timeStampList = new ArrayList();
    private final long NANOSECOND = 1000000000l;
    private int count =-1;
    private boolean flag2 = false;
    private Set e1 = new LinkedHashSet();
    private Set e2 = new LinkedHashSet();
    private int lower_x1,upper_x1,lower_y1,upper_y1,sid1,sid2,sid3,sid4,z,a_abs,upper_x2,lower_x2,y2;

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

                if (event.getType() != ComplexEvent.Type.TIMER) {
                    streamEventChunk.remove();

                    long timestamp = (Long) timestampExecutor.execute(event);
                    //eventTimestamps.add(timestamp);
                    timeStampList.add(timestamp);

                    if (discardFlag) {
                        if (timestamp < lastSentTimestamp) {
                            continue;
                        }
                    }

                    ArrayList<StreamEvent> eventList = eventTreeMap.get(timestamp);
                    if (eventList == null) {
                        eventList = new ArrayList<StreamEvent>();
                        eventTreeMap.put(timestamp, eventList);
                    }
                    eventList.add(event);


                    counter += 1;
                    double correlationField = (Double) correlationFieldExecutor.execute(event);
                    dataItemList.add(correlationField);

                    if (counter > batchSize) {
                        Iterator<Long> itr = timeStampList.iterator();
                        while(itr.hasNext()){
                        long data = Math.round(itr.next()*1.0/NANOSECOND);
                            eventTimeStamps.add(data);
                        }
                        long largestTimestamp = (Long) Collections.max(eventTimeStamps);
                        long smallestTimestamp = (Long)Collections.min(eventTimeStamps);
                        long windowSize = largestTimestamp - smallestTimestamp;
                        double windowCoverage = runTime.calculateWindowCoverage(eventTimestamps,windowSize);
                        double criticalValue = thetaThreshold.calculateCriticalValue();
                        double thresholdValue = thetaThreshold.calculateThetaThreshold(criticalValue, thetaThreshold.
                                        calculateMean(dataItemList),
                                thetaThreshold.calculateVariance(dataItemList));

                        double error = thresholdValue - windowCoverage;
                        //System.out.println(thresholdValue+"\t\t\t"+windowCoverage);
                        Kp = 0.1;
                        Kd = 0.1;

                        double deltaAlpha = Math.abs(Kp * error + Kd * (error - previousError));
                        alpha = previousAlpha + deltaAlpha;
//                        alpha = Math.abs(Kp * error + Kd * (error - previousError));
                        eventTimestamps = new LinkedHashSet<Long>();
                        counter = 0;
                        previousError = error;
                        previousAlpha = alpha;
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
                            buffersize.add(k);
                        }


                        Iterator<Map.Entry<Long, ArrayList<StreamEvent>>> entryIterator = eventTreeMap.entrySet()
                                .iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Long, ArrayList<StreamEvent>> entry = entryIterator.next();
                            ArrayList<StreamEvent> list = expiredEventTreeMap.get(entry.getKey());

                            if (list != null) {
                                list.addAll(entry.getValue());
                            } else {
                                expiredEventTreeMap.put(entry.getKey(), new ArrayList<StreamEvent>(entry.getValue()));
                            }
                        }
                        eventTreeMap = new TreeMap<Long, ArrayList<StreamEvent>>();
                        StreamEvent temp1 = null;
                        StreamEvent temp2 = null;
                        entryIterator = expiredEventTreeMap.entrySet().iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Long, ArrayList<StreamEvent>> entry = entryIterator.next();
                            StreamEvent a = entry.getValue().get(0);
                            if((Integer)a.getOutputData()[0]==sid1||(Integer)a.getOutputData()[0]==sid2||
                              (Integer)a.getOutputData()[0]==sid3||(Integer)a.getOutputData()[0]==sid4){
                                    if((Double)a.getOutputData()[2]>lower_x1 || (Double)a.getOutputData()[2]<upper_x1
                                    && (Double)a.getOutputData()[3]>lower_y1 && (Double)a.getOutputData()[3]<upper_y1){

                                        if(temp1!=a){
                                            e1.add(a);
                                        }
                                        temp1 = a;
                                    }
                                    else if((Double)a.getOutputData()[2]<=upper_x2 && (Double)a.getOutputData()[2]>lower_x2
                                            && (Double)a.getOutputData()[3]<=y2
                                            && (Integer)a.getOutputData()[4]<z && (Integer)a.getOutputData()[6]>=a_abs){
                                        if(temp2!=a) {
                                            e2.add(a);
                                        }
                                        temp2 = a;
                                    }
                            }

//                            if((Integer)a.getOutputData()[0]==4||(Integer)a.getOutputData()[0]==8||
//                                    (Integer)a.getOutputData()[0]==10||(Integer)a.getOutputData()[0]==12){
//                                if((Integer)a.getOutputData()[2]>29880 || (Integer)a.getOutputData()[2]<22560
//                                        && (Integer)a.getOutputData()[3]>-33968 && (Integer)a.getOutputData()[3]<33965){
//                                    e1.add(a);
//                                }
//                                else if((Integer)a.getOutputData()[2]<=29898 && (Integer)a.getOutputData()[2]>22579
//                                        && (Integer)a.getOutputData()[3]<=-33968
//                                        && (Integer)a.getOutputData()[4]<2440 && (Integer)a.getOutputData()[6]>=55){
//                                    e2.add(a);
//                                }
//                            }
                            if (entry.getKey() + k <= greatestTimestamp) {
                                entryIterator.remove();
                                ArrayList<StreamEvent> timeEventList = entry.getValue();
                                lastSentTimestamp = entry.getKey();
                                for (StreamEvent aTimeEventList : timeEventList) {
                                    complexEventChunk.add(aTimeEventList);
                                }
                            }
                        }


                        Iterator<StreamEvent> setIterator = e1.iterator();

                        while (setIterator.hasNext()) {
                            StreamEvent setEntry = setIterator.next();
                            Iterator<StreamEvent> setItr = e2.iterator();
                            while (setItr.hasNext()) {
                                StreamEvent entry2 = setItr.next();
                                if(flag2){
                                    break;
                                }
                                if ((Long) setEntry.getOutputData()[1] < (Long) entry2.getOutputData()[1]) {
                                    entry2.getOutputData()[13] = System.currentTimeMillis()-(Long) setEntry.getOutputData()[13];
                                    System.out.println(entry2);
                                    setItr.remove();
                                    setIterator.remove();
                                    flag2 = true;
                                }

                            }
                            if(flag2){
                                break;
                            }
                        }
                    }
                } else {
                    if (expiredEventTreeMap.size() > 0) {
                        Iterator<Map.Entry<Long, ArrayList<StreamEvent>>> entryIterator =
                                expiredEventTreeMap.entrySet().iterator();
                        while (entryIterator.hasNext()) {
                            ArrayList<StreamEvent> timeEventList = entryIterator.next().getValue();
                            for (StreamEvent aTimeEventList : timeEventList) {
                                complexEventChunk.add(aTimeEventList);
                            }
                        }
                    }

                    if (eventTreeMap.size() > 0) {
                        Iterator<Map.Entry<Long, ArrayList<StreamEvent>>> entryIterator =
                                eventTreeMap.entrySet().iterator();

                        while (entryIterator.hasNext()) {
                            ArrayList<StreamEvent> timeEventList = entryIterator.next().getValue();
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

        if (attributeExpressionLength > 6 && attributeExpressionLength < 2) {
            throw new ExecutionPlanCreationException("Maximum six input parameters and minimum two input parameters " +
                    "can be specified for AK-Slack. " +
                    " Timestamp (long), velocity (long), batchSize (long), timerTimeout (long), maxK (long), " +
                    "and discardFlag (boolean)  fields. But found " +
                    attributeExpressionLength + " attributes.");
        }

        if (attributeExpressionExecutors.length >= 2) {
            flag = false;
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timestampExecutor = attributeExpressionExecutors[0];
                attributes.add(new Attribute("beta0", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the first argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[0].getReturnType());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
                correlationFieldExecutor = attributeExpressionExecutors[1];
                attributes.add(new Attribute("beta1", Attribute.Type.DOUBLE));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the second argument of " +
                        " reorder:akslack() function. Required DOUBLE, but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

        }
        if (attributeExpressionExecutors.length >= 3) {
            flag = false;
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                    batchSizeExecutor = attributeExpressionExecutors[2];
                    attributes.add(new Attribute("beta2", Attribute.Type.LONG));
                    batchSize = (Long) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new ExecutionPlanCreationException("Invalid parameter type found for the third argument of " +
                            " reorder:akslack() function. Required LONG, but found " +
                            attributeExpressionExecutors[2].getReturnType());
                }
            } else {
                throw new ExecutionPlanCreationException("Batch size parameter must be a constant.");
            }

        }
        if (attributeExpressionExecutors.length >= 4) {
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.STRING) {
                    queryExecutor = attributeExpressionExecutors[3];
                    attributes.add(new Attribute("beta3", Attribute.Type.STRING));
                    query = (String) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[3]).getValue();
                    extractQuery(query);
                }
            }else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the fourth argument of " +
                        " reorder:akslack() function. Required STRING, but found " +
                        attributeExpressionExecutors[3].getReturnType());
            }

        }
        if (attributeExpressionExecutors.length >= 5) {
            if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.LONG) {
                maxK = (Long) attributeExpressionExecutors[4].execute(null);
                attributes.add(new Attribute("beta4", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the fifth argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[4].getReturnType());
            }
        }
        if (attributeExpressionExecutors.length >= 6) {
            if (attributeExpressionExecutors[5].getReturnType() == Attribute.Type.BOOL) {
                discardFlag = (Boolean) attributeExpressionExecutors[5].execute(null);
                attributes.add(new Attribute("beta5", Attribute.Type.BOOL));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the sixth argument of " +
                        " reorder:akslack() function. Required BOOL, but found " +
                        attributeExpressionExecutors[5].getReturnType());
            }
        }

        eventTreeMap = new TreeMap<Long, ArrayList<StreamEvent>>();
        expiredEventTreeMap = new TreeMap<Long, ArrayList<StreamEvent>>();

        if (timerDuration != -1l && scheduler != null) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() + timerDuration;
            scheduler.notifyAt(lastScheduledTimestamp);
        }

        return attributes;
    }

    private void extractQuery(String query){
        ArrayList<String > conditions = new ArrayList<String>();
        ArrayList<Integer> value = new ArrayList<Integer>();
        conditions.add(query.split("->")[0].split("from ")[1]);
        conditions.add(query.split("->")[1]);
        Pattern pattern = Pattern.compile("(x(>|<=)| (or||and) x(<|>)|\\) and y(>|<=)| and z<| and a_abs>=| and y<| and \\(sid==| or sid ==| or sid==| or sid==|\\)\\])");

        Iterator<String> itr = conditions.iterator();
        while(itr.hasNext()){
            String entry = itr.next();
            String[] values = pattern.split(entry);
            for(int i=1;i<values.length;i++){
                try {
                    value.add(Integer.parseInt(values[i]));
                }
                catch (Exception e){
                    values[i]="";
                }
            }
        }
            lower_x1 = value.get(0);
            upper_x1 = value.get(1);
            lower_y1 = value.get(2);
            upper_y1 = value.get(3);
            sid1 = value.get(4);
            sid2 = value.get(5);
            sid3 = value.get(6);
            sid4 =value.get(7);
            upper_x2 = value.get(8);
            lower_x2 = value.get(9);
            y2 = value.get(10);
            z = value.get(11);
            a_abs =value.get(12);
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        if (lastScheduledTimestamp < 0 && flag) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() + timerDuration;
            scheduler.notifyAt(lastScheduledTimestamp);
        }
    }
}
