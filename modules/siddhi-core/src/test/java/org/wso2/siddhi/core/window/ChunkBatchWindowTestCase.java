package org.wso2.siddhi.core.window;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class ChunkBatchWindowTestCase {
    private static final Logger log = Logger.getLogger(BatchWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void testChunkBatchWindow1() throws InterruptedException {
        log.info("ChunkBatchWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume long); " +
                "define window cseEventWindow (symbol string, price float, volume long) " +
                "chunkBatch(1) output all events; ";
        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventWindow " +
                "select * " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        int length = 10;
        Event[] eventsSet1 = new Event[length];
        Event[] eventsSet2 = new Event[length];
        for (int i = 0; i < length; i++) {
            eventsSet1[i] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", i * 2.5f, 10L});
            eventsSet2[i] = new Event(System.currentTimeMillis(), new Object[]{"IBM", i * 2.5f, 10L});
        }
        inputHandler.send(eventsSet1);
        inputHandler.send(eventsSet2);
        Thread.sleep(1000);
        AssertJUnit.assertEquals(20, inEventCount);
        AssertJUnit.assertEquals(10, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testChunkBatchWindow2() throws InterruptedException {
        log.info("ChunkBatchWindow test2 - current event");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume long); " +
                "define window cseEventWindow (symbol string, price float, volume long) chunkBatch(4); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventWindow " +
                "select * " +
                "insert current events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        int length = 10;
        Event[] eventsSet1 = new Event[length];
        Event[] eventsSet2 = new Event[length];
        for (int i = 0; i < length; i++) {
            eventsSet1[i] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", i * 2.5f, 10L});
            eventsSet2[i] = new Event(System.currentTimeMillis(), new Object[]{"IBM", i * 2.5f, 10L});
        }
        inputHandler.send(eventsSet1);
        inputHandler.send(eventsSet2);
        Thread.sleep(1000);
        AssertJUnit.assertEquals(20, inEventCount);
        AssertJUnit.assertEquals(0, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testChunkBatchWindow3() throws InterruptedException {
        log.info("ChunkBatchWindow test3 - expired event");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume long); " +
                "define window cseEventWindow (symbol string, price float, volume long) chunkBatch(5); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventWindow " +
                "select * " +
                "insert expired events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        int length = 10;
        Event[] eventsSet1 = new Event[length];
        Event[] eventsSet2 = new Event[length];
        for (int i = 0; i < length; i++) {
            eventsSet1[i] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", i * 2.5f, 10L});
            eventsSet2[i] = new Event(System.currentTimeMillis(), new Object[]{"IBM", i * 2.5f, 10L});
        }
        inputHandler.send(eventsSet1);
        inputHandler.send(eventsSet2);
        Thread.sleep(1000);
        AssertJUnit.assertEquals(0, inEventCount);
        AssertJUnit.assertEquals(10, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testChunkBatchWindow4() throws InterruptedException {
        log.info("ChunkBatchWindow test4 - different batch sizes");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume long); " +
                "define window cseEventWindow (symbol string, price float, volume long) " +
                "chunkBatch(5) output all events; ";
        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventWindow " +
                "select * " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        int length1 = 20;
        int length2 = 10;
        Event[] eventsSet1 = new Event[length1];
        Event[] eventsSet2 = new Event[length2];
        for (int i = 0; i < length1; i++) {
            eventsSet1[i] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", i * 2.5f, 10L});
            if (i < length2) {
                eventsSet2[i] = new Event(System.currentTimeMillis(), new Object[]{"IBM", i * 2.5f, 10L});
            }
        }
        inputHandler.send(eventsSet1);
        inputHandler.send(eventsSet2);
        Thread.sleep(1000);
        AssertJUnit.assertEquals(30, inEventCount);
        AssertJUnit.assertEquals(20, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
