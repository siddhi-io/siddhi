package org.wso2.siddhi.core;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class testSiddhiLatency {
    private static final Logger log = Logger.getLogger(testSiddhiLatency.class);
    private static InputHandler eligibilityStreamInputHandler;
    private static Map<String, ResultContainer> resultMap = new ConcurrentHashMap<String, ResultContainer>();
    private static Map<String, InputHandler> requestStreamInputHandlerMap = new ConcurrentHashMap<String, InputHandler>();
    private static AtomicInteger ruleCount = new AtomicInteger(0);

    public static void main(String[] args) {
        init();
        while (true) {
            isThrottled(new Object[]{"IBM", "TEST1", "TEST1", "TEST1", "Gold", "Test1", null,
                    System.currentTimeMillis()});
            isThrottled(new Object[]{"IBM", "TEST1", "TEST1", "TEST1", "Gold", "Test1", null,
                    System.currentTimeMillis()});
            isThrottled(new Object[]{"IBM", "TEST1", "TEST1", "TEST1", "Gold", "Test1", null,
                    System.currentTimeMillis()});
            isThrottled(new Object[]{"IBM", "TEST1", "TEST1", "TEST1", "Gold", "Test1", null,
                    System.currentTimeMillis()});
            isThrottled(new Object[]{"IBM", "TEST1", "TEST1", "TEST1", "Gold", "Test1", null,
                    System.currentTimeMillis()});
            isThrottled(new Object[]{"IBM", "TEST1", "TEST1", "TEST1", "Gold", "Test1", null,
                    System.currentTimeMillis()});

        }

    }

    public static void init() {
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("str:concat", ConcatFunctionExtension.class);
        String eligibilityStream = " define stream EligibilityStream (rule string, messageID string, isEligible bool," +
                " isLocallyThrottled bool, throttle_key string , timeNow long); ";
        String commonQuery = "FROM EligibilityStream[isEligible==false]\n" +
                "\t\tSELECT rule, messageID, false AS isThrottled, timeNow\n" +
                "\t\tINSERT INTO ThrottleStream;\n" +
                "\n" +
                "\t\tFROM EligibilityStream[isEligible==true AND isLocallyThrottled==true]\n" +
                "\t\tSELECT rule, messageID, true AS isThrottled, timeNow\n" +
                "\t\tINSERT INTO ThrottleStream; \n" +
                "\n" +
                "\t\tFROM EligibilityStream[isEligible==true AND isLocallyThrottled==false]\n" +
                "\t\tSELECT rule, messageID, false AS isThrottled, timeNow\n" +
                "\t\tINSERT INTO ThrottleStream; ";

        ExecutionPlanRuntime commonExecutionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (eligibilityStream + commonQuery);

        commonExecutionPlanRuntime.addCallback("ThrottleStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                log.info("Common callback latency =" + (System.currentTimeMillis() - (Long) events[0].getData(3)));

                for (Event event : events) {
                    resultMap.get(event.getData(1).toString()).addResult((String) event.getData(0), (Boolean) event.getData(2));
                }
            }
        });

        commonExecutionPlanRuntime.start();
        //get and register inputHandler
        eligibilityStreamInputHandler = commonExecutionPlanRuntime.getInputHandler("EligibilityStream");


        String requestStream = "define stream RequestStream (messageID string, app_key string, api_key string, " +
                "app_tier string, api_tier string, user_id string, properties object, timeNow long);";
        String eligibilityQuery = "FROM RequestStream\n" +
                "SELECT 'sub_gold' AS rule, messageID, ( api_tier == 'Gold') AS isEligible,false as " +
                "isLocallyThrottled,  str:concat('sub_gold_', api_key,app_key,user_id,'_key') AS throttle_key, timeNow\n" +
                "INSERT INTO EligibilityStream;";

        ExecutionPlanRuntime ruleRuntime = siddhiManager.createExecutionPlanRuntime(requestStream + eligibilityQuery);
        ruleRuntime.addCallback("EligibilityStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                try {
                    log.info("Gold callback latency =" + (System.currentTimeMillis() - (Long) events[0].getData(5)));
                    eligibilityStreamInputHandler.send(events);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Error occurred when publishing to EligibilityStream of throttling policy " + "sub+gold",
                            e);
                }
            }
        });


        ruleRuntime.start();

        //get and register input handler for RequestStream, so isThrottled() can use it.

        requestStreamInputHandlerMap.put("sub_gold", ruleRuntime.getInputHandler("RequestStream"));
        ruleCount.incrementAndGet();
    }

    public static boolean isThrottled(Object[] throttleRequest) {
        log.info("Start latency =" + (System.currentTimeMillis() - (Long) throttleRequest[7]));
        if (ruleCount.get() != 0) {

            String uniqueKey = (String) throttleRequest[0];
            ResultContainer result = new ResultContainer(ruleCount.get());
            resultMap.put(uniqueKey.toString(), result);
            for (InputHandler inputHandler : requestStreamInputHandlerMap.values()) {
                try {
                    inputHandler.send(Arrays.copyOf(throttleRequest, throttleRequest.length));
                } catch (InterruptedException e) {
                    //interrupt current thread so that interrupt can propagate
                    Thread.currentThread().interrupt();
                    log.error(e.getMessage(), e);
                }
            }
            log.info("sending latency =" + (System.currentTimeMillis() - (Long) throttleRequest[7]));
            //Blocked call to return synchronous result
            boolean isThrottled = false;
            try {
                isThrottled = result.isThrottled();
                log.info("After result latency =" + (System.currentTimeMillis() - (Long) throttleRequest[7]));

                if (log.isDebugEnabled()) {
                    log.debug("Throttling status for request to API " + throttleRequest[2] + " is " + isThrottled);
                }
            } catch (InterruptedException e) {
                //interrupt current thread so that interrupt can propagate
                Thread.currentThread().interrupt();
                // log.error(e.getMessage(), e);
            }

            if (!isThrottled) {
                //Converting properties map into json compatible String
                if (throttleRequest[6] != null) {
                    throttleRequest[6] = (throttleRequest[6]).toString();
                }
                //Only send served throttleRequest to global throttler
                // sendToGlobalThrottler(throttleRequest);

            }
            resultMap.remove(uniqueKey);
            log.info("Return latency =" + (System.currentTimeMillis() - (Long) throttleRequest[7]));
            return isThrottled;
        } else {
            return false;
        }
    }


}
