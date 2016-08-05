package org.wso2.siddhi.extension.priority;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by bhagya on 8/3/16.
 */
public class PriorityWindowTestCase {
    private static final Logger log = Logger.getLogger(PriorityWindowTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void priorityWindowTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (id string, priority long, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:priorityWindow(id,priority,1 sec) " +
                "select changedEvents " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count.incrementAndGet();
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 2, 0});
        Thread.sleep(1500);
        inputHandler.send(new Object[]{"WSO2", 5, 1});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"WSO2", 5, 3});
        Thread.sleep(4000);
        Assert.assertEquals(5, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }
}

