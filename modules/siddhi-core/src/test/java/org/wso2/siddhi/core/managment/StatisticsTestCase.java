package org.wso2.siddhi.core.managment;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class StatisticsTestCase {

    static final Logger log = Logger.getLogger(StatisticsTestCase.class);
    private int count;
    private boolean eventArrived;
    private long firstValue;
    private long lastValue;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
        firstValue = 0;
        lastValue = 0;
    }

    @Test
    public void statisticsTest1() throws InterruptedException {
        log.info("statistics test 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "@plan:statistics(reporter = 'console', interval = '5' )" +
                " " +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "define stream cseEventStream2 (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[70 > price] " +
                "select * " +
                "insert into outputStream ;" +
                "" +
                "@info(name = 'query2') " +
                "from cseEventStream[volume > 90] " +
                "select * " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    count++;
                    Assert.assertTrue("IBM".equals(event.getData(0)) || "WSO2".equals(event.getData(0)));
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream old = System.out;
        System.setOut(ps);

        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 55.6f, 100});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});

        Thread.sleep(5010);
        executionPlanRuntime.shutdown();
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(3, count);

        System.out.flush();
        System.setOut(old);

        String output= baos.toString();

        Assert.assertTrue(output.contains("Gauges"));
        Assert.assertTrue(output.contains("org.wso2.siddhi.executionplan"));
        Assert.assertTrue(output.contains("query1.memory"));
        Assert.assertTrue(output.contains("Meters"));
        Assert.assertTrue(output.contains("org.wso2.siddhi.stream.cseEventStream"));
        Assert.assertTrue(output.contains("Timers"));
        Assert.assertTrue(output.contains("query1.latency"));

        System.out.println(output);

    }
}
