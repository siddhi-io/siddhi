package io.siddhi.core.query.table.cache;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;

public class TestStoreContainingInMemoryTableTestCase {
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }
    private static final Logger log = Logger.getLogger(TestStoreContainingInMemoryTableTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== Test Store Containing InMemoryTable tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Test Store Containing InMemoryTable tests completed ==");
    }


    @Test
    public void insertIntoTableAndQueryTest() throws InterruptedException, SQLException {
        log.info("insertIntoTableAndQueryTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreContainingInMemoryTable\")\n" +
                "@PrimaryKey(\"symbol\", \"price\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 58.6F, 100L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(4, events.length);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "deleteFromTableTest")
    public void deleteFromTableTest() throws InterruptedException, SQLException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreContainingInMemoryTable\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(null, events);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void deleteFromTableTest2() throws InterruptedException, SQLException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreContainingInMemoryTable\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol AND StockTable.volume == volume ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(null, events);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void InTableTest() throws InterruptedException, SQLException {
        log.info("InTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckInStockStream (symbol string); " +
                "@Store(type=\"testStoreContainingInMemoryTable\")\n" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, volume\n" +
                "insert into StockTable ;";
        String query2 = "" +
                "@info(name = 'query2') " +
                "from CheckInStockStream[StockTable.symbol == symbol in StockTable]\n" +
                "insert into OutputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1 + query2);

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2"});
                                break;
                        }
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler CheckInStockStream = siddhiAppRuntime.getInputHandler("CheckInStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        CheckInStockStream.send(new Object[]{"WSO2"});
        Assert.assertEquals(true, eventArrived, "Event arrived");

        siddhiAppRuntime.shutdown();
    }

//    @Test
//    public void InTableTest2() throws InterruptedException, SQLException {
//        log.info("InTableTest2");
//        SiddhiManager siddhiManager = new SiddhiManager();
//        String streams = "" +
//                "define stream StockStream (symbol string, price float, volume long); " +
//                "define stream CheckInStockStream (symbol string, volume long); " +
//                "@Store(type=\"testStoreContainingInMemoryTable\")\n" +
//                //"@Index(\"volume\")" +
//                "define table StockTable (symbol string, volume long); ";
//
//        String query1 = "" +
//                "@info(name = 'query1') " +
//                "from StockStream\n" +
//                "select symbol, volume\n" +
//                "insert into StockTable ;";
//        String query2 = "" +
//                "@info(name = 'query2') " +
//                "from CheckInStockStream[StockTable.symbol == symbol in StockTable AND StockTable.volume == volume in StockTable]\n" +
//                "insert into OutputStream ;";
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1 + query2);
//
//        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
//            @Override
//            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timestamp, inEvents, removeEvents);
//                if (inEvents != null) {
//                    for (Event event : inEvents) {
//                        inEventCount++;
//                        switch (inEventCount) {
//                            case 1:
//                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 100L});
//                                break;
//                        }
//                    }
//                    eventArrived = true;
//                }
//            }
//        });
//
//        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
//        InputHandler CheckInStockStream = siddhiAppRuntime.getInputHandler("CheckInStockStream");
//        siddhiAppRuntime.start();
//
//        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
//        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
//        CheckInStockStream.send(new Object[]{"WSO2", 100L});
//        Assert.assertEquals(eventArrived, true, "Event arrived");
//
//        siddhiAppRuntime.shutdown();
//    }

    @Test
    public void testTableJoinQuery() throws InterruptedException, SQLException {
        log.info("testTableJoinQuery");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
                "@Store(type=\"testStoreContainingInMemoryTable\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) join StockTable " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "IBM", 10L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 2);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void updateFromTableTest7() throws InterruptedException, SQLException {
        log.info("updateFromTableTest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (comp string, prc float, volume long); " +
                "@Store(type=\"testStoreContainingInMemoryTable\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select comp as symbol, prc as price, volume " +
                "update StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and volume==StockTable.volume " +
                "and price<StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 150.6f, 100L}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 190.6f, 100L}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(2, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 185.6f, 100L});

        checkStockStream.send(new Object[]{"IBM", 150.6f, 100L});
        checkStockStream.send(new Object[]{"WSO2", 175.6f, 100L});

        updateStockStream.send(new Object[]{"IBM", 200f, 100L});

        checkStockStream.send(new Object[]{"IBM", 190.6f, 100L});
        checkStockStream.send(new Object[]{"WSO2", 155.6f, 100L});
        Thread.sleep(2000);

        AssertJUnit.assertEquals("Number of success events", 2, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void updateOrInsertTableTest6() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@Store(type=\"testStoreContainingInMemoryTable\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select comp as symbol, 0f as price, vol as volume " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 100L}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            case 3:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(3, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        updateStockStream.send(new Object[]{"IBM", 200L});
        updateStockStream.send(new Object[]{"FB", 300L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        Thread.sleep(500);

        AssertJUnit.assertEquals("Number of success events", 3, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

}
