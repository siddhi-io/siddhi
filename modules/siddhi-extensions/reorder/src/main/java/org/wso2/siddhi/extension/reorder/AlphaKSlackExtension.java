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
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;



import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by vithursa on 10/3/16.
 */
public class AlphaKSlackExtension extends StreamProcessor {
    private long k = 0; //In the beginning the K is zero.
    private long greatestTimestamp = 0; //Used to track the greatest timestamp of tuples seen so far in the stream history.
    private TreeMap<Long, ArrayList<StreamEvent>> eventTreeMap;
    private TreeMap<Long, ArrayList<StreamEvent>> expiredEventTreeMap;
    private ExpressionExecutor timestampExecutor;
    private ExpressionExecutor dataItemExecutor;
    private long MAX_K = Long.MAX_VALUE;
    private long TIMER_DURATION = -1l;
    private boolean expireFlag = false;
    private long lastSentTimeStamp = -1l;
    private Scheduler scheduler;
    private long lastScheduledTimestamp = -1;
    private ReentrantLock lock = new ReentrantLock();
    private double alpha=0;
    private int counter =0;
    private int batchSize = 10000;
    private int NANOSECOND = 1000000000;
    private ArrayList<Long> timeStampList = new ArrayList<Long>();
    private ArrayList<Integer> dataItemList = new ArrayList<Integer>();
    private LinkedHashSet<Long> eventTimeStamps = new LinkedHashSet<Long>();
    private ArrayList errorList = new ArrayList();
    private int count2=0;
    private double differenceError;
    private double windowCoverage=1;
    private double thetaThreshold=0;
    private ArrayList dataList = new ArrayList();
    private LinkedHashSet bufferSize = new LinkedHashSet();
    private long cumulativeTime=0;
    private int counter_now=0;


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
        double startTime = System.nanoTime();
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);
        lock.lock();
        Runtime obj5 = new Runtime();
        boolean flag = false;
        ThetaThreshold obj2 = new ThetaThreshold(0.2,0.5,10000);
        try {
            while (streamEventChunk.hasNext()) {

                StreamEvent event = streamEventChunk.next();

                if(event.getType() != ComplexEvent.Type.TIMER) {

                    streamEventChunk.remove(); //We might have the rest of the events linked to this event forming a chain.

                    long timestamp = (Long) timestampExecutor.execute(event);
                    timeStampList.add(timestamp);

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
                    counter_now+=1;
                    int dataItem = (Integer) dataItemExecutor.execute(event);
                    dataItemList.add(dataItem);


                    if(counter>batchSize){
                        Iterator<Long> itr = timeStampList.iterator();
                        while(itr.hasNext()){
                            long data = itr.next()/NANOSECOND;
                            eventTimeStamps.add(data);
                        }
                        ArrayList<Long> itemsList = obj5.CurentTimeStamp(eventTimeStamps);
                        windowCoverage = obj5.WindowCoverageCalculator(eventTimeStamps, itemsList);
                        double criticalValue = obj2.CriticalValueCalculator();

                        Iterator itr2 = dataItemList.iterator();
                        while(itr.hasNext()){
                            dataList.add((Integer) itr2.next()*1.0/1000000);
                        }
                        thetaThreshold = obj2.ThetaThresholdCalculator(criticalValue,obj2.MeanCalculator(dataList),
                                obj2.VarianceCalculator(dataList));

                        double error = thetaThreshold - windowCoverage;
                        errorList.add(error);
                        if(count2==0){
                            differenceError = error;
                        }
                        else{
                            differenceError = error- (Double) errorList.get(errorList.indexOf(error)-1);
                        }

                        alpha = Math.abs(0.1*error + 0.1*differenceError);
                        timeStampList = new ArrayList<Long>();
                        dataItemList = new ArrayList<Integer>();
                        eventTimeStamps = new LinkedHashSet<Long>();
                        counter=0;
                        count2+=1;
                        //System.out.println(cumulativeTime*1.0/batchSize);
                        cumulativeTime =0;
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
                        //System.out.println(bufferSize);

                        Iterator<Map.Entry<Long, ArrayList<StreamEvent>>> entryIterator = eventTreeMap.entrySet().iterator();
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
                                    flag = true;
                                }
                            }
                        }
                    }
                } else {
                    if(expiredEventTreeMap.size() > 0) {
                        TreeMap<Long, ArrayList<StreamEvent>> expiredEventTreeMapSnapShot = expiredEventTreeMap;
                        expiredEventTreeMap = new TreeMap<Long, ArrayList<StreamEvent>>();
                        lastScheduledTimestamp = lastScheduledTimestamp + TIMER_DURATION;
                    }
                }



            }
        } catch (ArrayIndexOutOfBoundsException ec) {
            //This happens due to user specifying an invalid field index.
            throw new ExecutionPlanCreationException("The very first parameter must be an Integer with a valid " +
                    " field index (0 to (fieldsLength-1)).");
        }
        lock.unlock();
        if(flag) {
            nextProcessor.process(complexEventChunk);
        }
        else{
            int i = 0;
        }
        flag = false;
        double endTime = System.nanoTime();
        double executionTime = endTime - startTime;
        cumulativeTime+=executionTime;
        if(counter_now>batchSize){
            System.out.println(cumulativeTime);
            cumulativeTime=0;
            counter_now=0;
        }
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                   ExecutionPlanContext executionPlanContext) {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();

        if (attributeExpressionLength > 2) {
            throw new ExecutionPlanCreationException("Maximum two input parameters can be specified for AKSlack. " +
                    " Timestamp field (long) and velocity field. But found " +
                    attributeExpressionLength + " attributes.");
        }

        //This is the most basic case. Here we do not use a timer. The basic K-slack algorithm is implemented.
        if(attributeExpressionExecutors.length == 1){
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timestampExecutor = attributeExpressionExecutors[0];
                attributes.add(new Attribute("beta0", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the first argument of " +
                        "reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[0].getReturnType());
            }
            //In the following case we have the timer operating in background. But we do not impose a K-slack window length.
        }else if(attributeExpressionExecutors.length == 2){
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timestampExecutor = attributeExpressionExecutors[0];
                attributes.add(new Attribute("beta0", Attribute.Type.LONG));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the first argument of " +
                        " reorder:akslack() function. Required LONG, but found " +
                        attributeExpressionExecutors[0].getReturnType());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                dataItemExecutor = attributeExpressionExecutors[1];
                attributes.add(new Attribute("beta1", Attribute.Type.INT));
            } else {
                throw new ExecutionPlanCreationException("Invalid parameter type found for the second argument of " +
                        " reorder:akslack() function. Required INT, but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }
            //In the third case we have both the timer operating in the background and we have also specified a K-slack window length.
        }
        eventTreeMap = new TreeMap<Long, ArrayList<StreamEvent>>();
        expiredEventTreeMap = new TreeMap<Long, ArrayList<StreamEvent>>();

        if(TIMER_DURATION != -1l && scheduler != null) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() + TIMER_DURATION;
            scheduler.notifyAt(lastScheduledTimestamp);
        }

        return attributes;
    }

    /*private void onTimerEvent(TreeMap<Long, ArrayList<StreamEvent>> treeMap, Processor nextProcessor) {
        Iterator<Map.Entry<Long, ArrayList<StreamEvent>>> entryIterator = treeMap.entrySet().iterator();
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);

        while (entryIterator.hasNext()) {
            ArrayList<StreamEvent> timeEventList = entryIterator.next().getValue();

            for (StreamEvent aTimeEventList : timeEventList) {
                complexEventChunk.add(aTimeEventList);
            }
        }
        nextProcessor.process(complexEventChunk);
    }*/
    //public static void main(String[] args){
    // KSlackExtension obj = new KSlackExtension();
    //obj.process();
    //}
}
