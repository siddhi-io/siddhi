package org.wso2.siddhi.test;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by seshika on 4/7/14.
 */
public class SimpleRegressionTestCase
{
    static final Logger log = Logger.getLogger(SimpleRegressionTestCase.class);

    private int count;
    private double betaZero;

    @Before
    public void init() {
        count = 0;
    }

    @Test
    public void testRegression() throws InterruptedException {
        log.info("Regression Test 1 - Simple Linear");

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();

        List<Class> list = new ArrayList<Class>();
        list.add(org.wso2.siddhi.extension.timeseries.LinearRegressionTransformProcessor.class);

        siddhiConfiguration.setSiddhiExtensions(list);

        SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);

        InputHandler inputHandler = siddhiManager.defineStream("define stream DataStream ( y double, x double )");

        String queryReference = siddhiManager.addQuery("from DataStream#transform.timeseries:regress( 30, 0.95, y, x) \n" +
                "        select *  \n" +
                "        insert into RegressionResult;\n");

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents[0].getData1() != null) {
                    betaZero = (Double) inEvents[0].getData1();
                }
                count++;
            }
        });

        System.out.println(System.currentTimeMillis());

        inputHandler.send(new Object[]{ 3300, 31 });
        inputHandler.send(new Object[]{ 2600, 18 });
        inputHandler.send(new Object[]{ 2500, 17 });
        inputHandler.send(new Object[]{ 2475, 12 });
        inputHandler.send(new Object[]{ 2313, 8 });
        inputHandler.send(new Object[]{ 2175, 26 });
        inputHandler.send(new Object[]{ 600, 14 });
        inputHandler.send(new Object[]{ 460, 3 });
        inputHandler.send(new Object[]{ 240, 1 });
        inputHandler.send(new Object[]{ 200, 10 });
        inputHandler.send(new Object[]{ 177, 0 });
        inputHandler.send(new Object[]{ 140, 6 });
        inputHandler.send(new Object[]{ 117, 1 });
        inputHandler.send(new Object[]{ 115, 0 });
        inputHandler.send(new Object[]{ 2600, 19 });
        inputHandler.send(new Object[]{ 1907, 13 });
        inputHandler.send(new Object[]{ 1190, 3 });
        inputHandler.send(new Object[]{ 990, 16 });
        inputHandler.send(new Object[]{ 925, 6 });
        inputHandler.send(new Object[]{ 365, 0 });
        inputHandler.send(new Object[]{ 302, 10 });
        inputHandler.send(new Object[]{ 300, 6 });
        inputHandler.send(new Object[]{ 129, 2 });
        inputHandler.send(new Object[]{ 111, 1 });
        inputHandler.send(new Object[]{ 6100, 18 });
        inputHandler.send(new Object[]{ 4125, 19 });
        inputHandler.send(new Object[]{ 3213, 1 });
        inputHandler.send(new Object[]{ 2319, 38 });
        inputHandler.send(new Object[]{ 2000, 10 });
        inputHandler.send(new Object[]{ 1600, 0 });
        inputHandler.send(new Object[]{ 1394, 4 });
        inputHandler.send(new Object[]{ 935, 4 });
        inputHandler.send(new Object[]{ 850, 0 });
        inputHandler.send(new Object[]{ 775, 5 });
        inputHandler.send(new Object[]{ 760, 6 });
        inputHandler.send(new Object[]{ 629, 1 });
        inputHandler.send(new Object[]{ 275, 6 });
        inputHandler.send(new Object[]{ 120, 0 });
        inputHandler.send(new Object[]{ 2567, 12 });
        inputHandler.send(new Object[]{ 2500, 28 });
        inputHandler.send(new Object[]{ 2350, 21 });
        inputHandler.send(new Object[]{ 2317, 3 });
        inputHandler.send(new Object[]{ 2000, 12 });
        inputHandler.send(new Object[]{ 715, 1 });
        inputHandler.send(new Object[]{ 660, 9 });
        inputHandler.send(new Object[]{ 650, 0 });
        inputHandler.send(new Object[]{ 260, 0 });
        inputHandler.send(new Object[]{ 250, 1 });
        inputHandler.send(new Object[]{ 200, 13 });
        inputHandler.send(new Object[]{ 180, 6 });


        Thread.sleep(1000);
        siddhiManager.shutdown();

//        Assert.assertEquals("No of events: ", 50, count);
//        Assert.assertEquals("Beta0: ", 573.1418421169498, betaZero);

    }
}
