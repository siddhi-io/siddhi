/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.extension.eventtable.rdbms;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.DuplicateDefinitionException;

import javax.sql.DataSource;
import java.sql.SQLException;

public class InsertOverwriteTableTestCase {
    private static final Logger log = Logger.getLogger(InsertOverwriteTableTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private DataSource dataSource = new BasicDataSource();

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void insertOverwriteTableTest1() throws InterruptedException {
        log.info("insertOverwriteTableTest1");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource(RDBMSTestConstants.DATA_SOURCE_NAME, dataSource);

        try {
            if (dataSource.getConnection() != null) {
                DBConnectionHelper.getDBConnectionHelperInstance().clearDatabaseTable(dataSource, RDBMSTestConstants.TABLE_NAME);
                String streams = "" +
                        "define stream StockStream (symbol string, price float, volume long); " +
                        "define stream UpdateStockStream (symbol string, price float, volume long); " +
                        "@from(eventtable = 'rdbms' , datasource.name = '" + RDBMSTestConstants.DATA_SOURCE_NAME + "' , table.name = '" + RDBMSTestConstants.TABLE_NAME + "') " +
                        "define table StockTable (symbol string, price float, volume long); ";
                String query = "" +
                        "@info(name = 'query1') " +
                        "from StockStream " +
                        "insert into StockTable ;" +
                        "" +
                        "@info(name = 'query2') " +
                        "from UpdateStockStream " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol=='IBM' ;";

                ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

                InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
                InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

                executionPlanRuntime.start();

                stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
                stockStream.send(new Object[]{"IBM", 75.6f, 100l});
                stockStream.send(new Object[]{"WSO2", 57.6f, 100l});
                updateStockStream.send(new Object[]{"GOOG", 10.6f, 100l});

                Thread.sleep(500);
                executionPlanRuntime.shutdown();
            }
        } catch (SQLException e) {
            log.info("Test case ignored due to DB connection unavailability");
        }

    }

    @Test
    public void insertOverwriteTableTest2() throws InterruptedException {
        log.info("insertOverwriteTableTest2");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource(RDBMSTestConstants.DATA_SOURCE_NAME, dataSource);

        try {
            if (dataSource.getConnection() != null) {
                DBConnectionHelper.getDBConnectionHelperInstance().clearDatabaseTable(dataSource, RDBMSTestConstants.TABLE_NAME);
                String streams = "" +
                        "define stream StockStream (symbol string, price float, volume long); " +
                        "@from(eventtable = 'rdbms' , datasource.name = '" + RDBMSTestConstants.DATA_SOURCE_NAME + "' , table.name = '" + RDBMSTestConstants.TABLE_NAME + "') " +
                        "define table StockTable (symbol string, price float, volume long); ";
                String query = "" +
                        "@info(name = 'query2') " +
                        "from StockStream " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol==symbol ;";

                ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

                InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");

                executionPlanRuntime.start();

                stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
                stockStream.send(new Object[]{"IBM", 75.6f, 100l});
                stockStream.send(new Object[]{"WSO2", 57.6f, 100l});
                stockStream.send(new Object[]{"WSO2", 10f, 100l});

                Thread.sleep(500);
                executionPlanRuntime.shutdown();
            }
        } catch (SQLException e) {
            log.info("Test case ignored due to DB connection unavailability");
        }
    }

    @Test
    public void insertOverwriteTableTest3() throws InterruptedException {
        log.info("insertOverwriteTableTest3");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource(RDBMSTestConstants.DATA_SOURCE_NAME, dataSource);

        try {
            if (dataSource.getConnection() != null) {
                DBConnectionHelper.getDBConnectionHelperInstance().clearDatabaseTable(dataSource, RDBMSTestConstants.TABLE_NAME);

                String streams = "" +
                        "define stream StockStream (symbol string, price float, volume long); " +
                        "define stream CheckStockStream (symbol string, volume long); " +
                        "define stream UpdateStockStream (symbol string, price float, volume long); " +
                        "@from(eventtable = 'rdbms' , datasource.name = '" + RDBMSTestConstants.DATA_SOURCE_NAME + "' , table.name = '" + RDBMSTestConstants.TABLE_NAME + "') " +
                        "define table StockTable (symbol string, price float, volume long); ";
                String query = "" +
                        "@info(name = 'query1') " +
                        "from StockStream " +
                        "insert into StockTable ;" +
                        "" +
                        "@info(name = 'query2') " +
                        "from UpdateStockStream " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol==symbol;" +
                        "" +
                        "@info(name = 'query3') " +
                        "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume) in StockTable] " +
                        "insert into OutStream;";

                ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

                executionPlanRuntime.addCallback("query3", new QueryCallback() {
                    @Override
                    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                        EventPrinter.print(timeStamp, inEvents, removeEvents);
                        if (inEvents != null) {
                            for (Event event : inEvents) {
                                inEventCount++;
                                switch (inEventCount) {
                                    case 1:
                                        Assert.assertArrayEquals(new Object[]{"IBM", 100l}, event.getData());
                                        break;
                                    case 2:
                                        Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                        break;
                                    case 3:
                                        Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                        break;
                                    default:
                                        Assert.assertSame(3, inEventCount);
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

                InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
                InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
                InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

                executionPlanRuntime.start();

                stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
                stockStream.send(new Object[]{"IBM", 55.6f, 100l});
                checkStockStream.send(new Object[]{"IBM", 100l});
                checkStockStream.send(new Object[]{"WSO2", 100l});
                updateStockStream.send(new Object[]{"IBM", 77.6f, 200l});
                checkStockStream.send(new Object[]{"IBM", 100l});
                checkStockStream.send(new Object[]{"WSO2", 100l});


                Thread.sleep(500);

                Assert.assertEquals("Number of success events", 3, inEventCount);
                Assert.assertEquals("Number of remove events", 0, removeEventCount);
                Assert.assertEquals("Event arrived", true, eventArrived);

                executionPlanRuntime.shutdown();
            }
        } catch (SQLException e) {
            log.info("Test case ignored due to DB connection unavailability");
        }

    }

    @Test
    public void insertOverwriteTableTest4() throws InterruptedException {
        log.info("insertOverwriteTableTest4");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource(RDBMSTestConstants.DATA_SOURCE_NAME, dataSource);

        try {
            if (dataSource.getConnection() != null) {
                DBConnectionHelper.getDBConnectionHelperInstance().clearDatabaseTable(dataSource, RDBMSTestConstants.TABLE_NAME);

                String streams = "" +
                        "define stream StockStream (symbol string, price float, volume long); " +
                        "define stream CheckStockStream (symbol string, volume long); " +
                        "@from(eventtable = 'rdbms' , datasource.name = '" + RDBMSTestConstants.DATA_SOURCE_NAME + "' , table.name = '" + RDBMSTestConstants.TABLE_NAME + "') " +
                        "define table StockTable (symbol string, price float, volume long); ";
                String query = "" +
                        "@info(name = 'query2') " +
                        "from StockStream " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol==symbol;" +
                        "" +
                        "@info(name = 'query3') " +
                        "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume) in StockTable] " +
                        "insert into OutStream;";

                ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

                executionPlanRuntime.addCallback("query3", new QueryCallback() {
                    @Override
                    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                        EventPrinter.print(timeStamp, inEvents, removeEvents);
                        if (inEvents != null) {
                            for (Event event : inEvents) {
                                inEventCount++;
                                switch (inEventCount) {
                                    case 1:
                                        Assert.assertArrayEquals(new Object[]{"IBM", 100l}, event.getData());
                                        break;
                                    case 2:
                                        Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                        break;
                                    case 3:
                                        Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                        break;
                                    default:
                                        Assert.assertSame(3, inEventCount);
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

                InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
                InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");

                executionPlanRuntime.start();

                stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
                stockStream.send(new Object[]{"IBM", 55.6f, 100l});
                checkStockStream.send(new Object[]{"IBM", 100l});
                checkStockStream.send(new Object[]{"WSO2", 100l});
                stockStream.send(new Object[]{"IBM", 77.6f, 200l});
                checkStockStream.send(new Object[]{"IBM", 100l});
                checkStockStream.send(new Object[]{"WSO2", 100l});


                Thread.sleep(500);

                Assert.assertEquals("Number of success events", 3, inEventCount);
                Assert.assertEquals("Number of remove events", 0, removeEventCount);
                Assert.assertEquals("Event arrived", true, eventArrived);

                executionPlanRuntime.shutdown();

            }
        } catch (SQLException e) {
            log.info("Test case ignored due to DB connection unavailability");
        }

    }

    @Ignore
    @Test(expected = DuplicateDefinitionException.class)
    public void insertOverwriteTableTest5() throws InterruptedException {
        log.info("insertOverwriteTableTest5");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource(RDBMSTestConstants.DATA_SOURCE_NAME, dataSource);

        try {
            if (dataSource.getConnection() != null) {
                DBConnectionHelper.getDBConnectionHelperInstance().clearDatabaseTable(dataSource, RDBMSTestConstants.TABLE_NAME);

                String streams = "" +
                        "define stream StockStream (symbol string, price float, volume long); " +
                        "define stream CheckStockStream (symbol string, volume long); " +
                        "define stream UpdateStockStream (comp string, vol long); " +
                        "@from(eventtable = 'rdbms' , datasource.name = '" + RDBMSTestConstants.DATA_SOURCE_NAME + "' , table.name = '" + RDBMSTestConstants.TABLE_NAME + "') " +
                        "define table StockTable (symbol string, price float, volume long); ";
                String query = "" +
                        "@info(name = 'query1') " +
                        "from StockStream " +
                        "insert into StockTable ;" +
                        "" +
                        "@info(name = 'query2') " +
                        "from UpdateStockStream " +
                        "select comp as symbol, vol as volume " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol==symbol;";

                ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

                executionPlanRuntime.addCallback("query3", new QueryCallback() {
                    @Override
                    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                        EventPrinter.print(timeStamp, inEvents, removeEvents);

                        eventArrived = true;

                    }

                });

                InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
                InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
                InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

                executionPlanRuntime.start();

                stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
                stockStream.send(new Object[]{"IBM", 55.6f, 100l});
                checkStockStream.send(new Object[]{"IBM", 100l});
                checkStockStream.send(new Object[]{"WSO2", 100l});
                updateStockStream.send(new Object[]{"FB", 300l});
                checkStockStream.send(new Object[]{"FB", 300l});
                checkStockStream.send(new Object[]{"WSO2", 100l});

                Thread.sleep(500);

                Assert.assertEquals("Number of success events", 0, inEventCount);
                Assert.assertEquals("Number of remove events", 0, removeEventCount);
                Assert.assertEquals("Event arrived", false, eventArrived);

                executionPlanRuntime.shutdown();
            }
        } catch (SQLException e) {
            log.info("Test case ignored due to DB connection unavailability");
        }

    }

    @Test
    public void insertOverwriteTableTest6() throws InterruptedException {
        log.info("insertOverwriteTableTest6");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource(RDBMSTestConstants.DATA_SOURCE_NAME, dataSource);

        try {
            if (dataSource.getConnection() != null) {
                DBConnectionHelper.getDBConnectionHelperInstance().clearDatabaseTable(dataSource, RDBMSTestConstants.TABLE_NAME);

                String streams = "" +
                        "define stream StockStream (symbol string, price float, volume long); " +
                        "define stream CheckStockStream (symbol string, volume long); " +
                        "define stream UpdateStockStream (comp string, vol long); " +
                        "@from(eventtable = 'rdbms' , datasource.name = '" + RDBMSTestConstants.DATA_SOURCE_NAME + "' , table.name = '" + RDBMSTestConstants.TABLE_NAME + "' , bloom.filters = 'enable') " +
                        "define table StockTable (symbol string, price float, volume long); ";
                String query = "" +
                        "@info(name = 'query1') " +
                        "from StockStream " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol==symbol;" +
                        "" +
                        "@info(name = 'query2') " +
                        "from UpdateStockStream " +
                        "select comp as symbol, 0f as price, vol as volume " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol==symbol;" +
                        "" +
                        "@info(name = 'query3') " +
                        "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume) in StockTable] " +
                        "insert into OutStream;";

                ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

                executionPlanRuntime.addCallback("query3", new QueryCallback() {
                    @Override
                    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                        EventPrinter.print(timeStamp, inEvents, removeEvents);
                        if (inEvents != null) {
                            for (Event event : inEvents) {
                                inEventCount++;
                                switch (inEventCount) {
                                    case 1:
                                        Assert.assertArrayEquals(new Object[]{"IBM", 100l}, event.getData());
                                        break;
                                    case 2:
                                        Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                        break;
                                    case 3:
                                        Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                        break;
                                    default:
                                        Assert.assertSame(3, inEventCount);
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

                InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
                InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
                InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

                executionPlanRuntime.start();

                stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
                stockStream.send(new Object[]{"IBM", 55.6f, 100l});
                checkStockStream.send(new Object[]{"IBM", 100l});
                checkStockStream.send(new Object[]{"WSO2", 100l});
                updateStockStream.send(new Object[]{"IBM", 200l});
                updateStockStream.send(new Object[]{"FB", 300l});
                checkStockStream.send(new Object[]{"IBM", 100l});
                checkStockStream.send(new Object[]{"WSO2", 100l});

                Thread.sleep(500);

                Assert.assertEquals("Number of success events", 3, inEventCount);
                Assert.assertEquals("Number of remove events", 0, removeEventCount);
                Assert.assertEquals("Event arrived", true, eventArrived);

                executionPlanRuntime.shutdown();
            }
        } catch (SQLException e) {
            log.info("Test case ignored due to DB connection unavailability");
        }

    }


    @Test
    public void insertOverwriteTableTest7() throws InterruptedException {
        log.info("insertOverwriteTableTest7");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource(RDBMSTestConstants.DATA_SOURCE_NAME, dataSource);

        try {
            if (dataSource.getConnection() != null) {
                DBConnectionHelper.getDBConnectionHelperInstance().clearDatabaseTable(dataSource, RDBMSTestConstants.TABLE_NAME);

                String streams = "" +
                        "define stream StockStream (symbol string, price float, volume long); " +
                        "define stream CheckStockStream (symbol string, volume long, price float); " +
                        "define stream UpdateStockStream (comp string, vol long); " +
                        "@from(eventtable = 'rdbms' , datasource.name = '" + RDBMSTestConstants.DATA_SOURCE_NAME + "' , table.name = '" + RDBMSTestConstants.TABLE_NAME + "') " +
                        "define table StockTable (symbol string, price float, volume long); ";
                String query = "" +
                        "@info(name = 'query1') " +
                        "from StockStream " +
                        "insert into StockTable ;" +
                        "" +
                        "@info(name = 'query2') " +
                        "from UpdateStockStream " +
                        "select comp as symbol,  5f as price, vol as volume " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol==symbol;" +
                        "" +
                        "@info(name = 'query3') " +
                        "from CheckStockStream[(symbol==StockTable.symbol and volume==StockTable.volume and price < StockTable.price) in StockTable] " +
                        "insert into OutStream;";

                ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

                executionPlanRuntime.addCallback("query3", new QueryCallback() {
                    @Override
                    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                        EventPrinter.print(timeStamp, inEvents, removeEvents);
                        if (inEvents != null) {
                            for (Event event : inEvents) {
                                inEventCount++;
                                switch (inEventCount) {
                                    case 1:
                                        Assert.assertArrayEquals(new Object[]{"IBM", 100l, 56.6f}, event.getData());
                                        break;
                                    case 2:
                                        Assert.assertArrayEquals(new Object[]{"IBM", 200l, 0f}, event.getData());
                                        break;
                                    default:
                                        Assert.assertSame(2, inEventCount);
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

                InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
                InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
                InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

                executionPlanRuntime.start();

                stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
                stockStream.send(new Object[]{"IBM", 155.6f, 100l});
                checkStockStream.send(new Object[]{"IBM", 100l, 56.6f});
                checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});
                updateStockStream.send(new Object[]{"IBM", 200l});
                checkStockStream.send(new Object[]{"IBM", 200l, 0f});
                checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});

                Thread.sleep(2000);

                Assert.assertEquals("Number of success events", 2, inEventCount);
                Assert.assertEquals("Number of remove events", 0, removeEventCount);
                Assert.assertEquals("Event arrived", true, eventArrived);

                executionPlanRuntime.shutdown();
            }
        } catch (SQLException e) {
            log.info("Test case ignored due to DB connection unavailability");
        }


    }

    @Test
    public void insertOverwriteTableTest8() throws InterruptedException {
        log.info("insertOverwriteTableTest8");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource(RDBMSTestConstants.DATA_SOURCE_NAME, dataSource);

        try {
            if (dataSource.getConnection() != null) {
                DBConnectionHelper.getDBConnectionHelperInstance().clearDatabaseTable(dataSource, RDBMSTestConstants.TABLE_NAME);

                String streams = "" +
                        "define stream StockStream (symbol string, price float, volume long); " +
                        "define stream CheckStockStream (symbol string, volume long, price float); " +
                        "@from(eventtable = 'rdbms' , datasource.name = '" + RDBMSTestConstants.DATA_SOURCE_NAME + "' , table.name = '" + RDBMSTestConstants.TABLE_NAME + "') " +
                        "define table StockTable (symbol string, price float, volume long); ";
                String query = "" +
                        "@info(name = 'query2') " +
                        "from StockStream " +
                        "select symbol, price, volume " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol==symbol;" +
                        "" +
                        "@info(name = 'query3') " +
                        "from CheckStockStream[(symbol==StockTable.symbol and volume==StockTable.volume and price < StockTable.price) in StockTable] " +
                        "insert into OutStream;";

                ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

                executionPlanRuntime.addCallback("query3", new QueryCallback() {
                    @Override
                    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                        EventPrinter.print(timeStamp, inEvents, removeEvents);
                        if (inEvents != null) {
                            for (Event event : inEvents) {
                                inEventCount++;
                                switch (inEventCount) {
                                    case 1:
                                        Assert.assertArrayEquals(new Object[]{"IBM", 100l, 55.6f}, event.getData());
                                        break;
                                    case 2:
                                        Assert.assertArrayEquals(new Object[]{"IBM", 200l, 55.6f}, event.getData());
                                        break;
                                    default:
                                        Assert.assertSame(2, inEventCount);
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

                InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
                InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");

                executionPlanRuntime.start();

                stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
                stockStream.send(new Object[]{"IBM", 155.6f, 100l});
                checkStockStream.send(new Object[]{"IBM", 100l, 55.6f});
                checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});
                stockStream.send(new Object[]{"IBM", 155.6f, 200l});
                checkStockStream.send(new Object[]{"IBM", 200l, 55.6f});
                checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});

                Thread.sleep(500);

                Assert.assertEquals("Number of success events", 2, inEventCount);
                Assert.assertEquals("Number of remove events", 0, removeEventCount);
                Assert.assertEquals("Event arrived", true, eventArrived);

                executionPlanRuntime.shutdown();
            }
        } catch (SQLException e) {
            log.info("Test case ignored due to DB connection unavailability");
        }


    }

    @Test
    public void insertOverwriteTableTest9() throws InterruptedException {
        log.info("insertOverwriteTableTest9");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource(RDBMSTestConstants.DATA_SOURCE_NAME, dataSource);

        try {
            if (dataSource.getConnection() != null) {
                DBConnectionHelper.getDBConnectionHelperInstance().clearDatabaseTable(dataSource, RDBMSTestConstants.TABLE_NAME);

                String streams = "" +
                        "define stream StockStream (symbol string, price float, volume long); " +
                        "define stream CheckStockStream (symbol string, volume long, price float); " +
                        "define stream UpdateStockStream (comp string, vol long); " +
                        "@from(eventtable = 'rdbms' , datasource.name = '" + RDBMSTestConstants.DATA_SOURCE_NAME + "' , table.name = '" + RDBMSTestConstants.TABLE_NAME + "') " +
                        "define table StockTable (symbol string, price float, volume long); ";
                String query = "" +
                        "@info(name = 'query1') " +
                        "from StockStream " +
                        "insert into StockTable ;" +
                        "" +
                        "@info(name = 'query2') " +
                        "from UpdateStockStream left outer join StockTable " +
                        "   on UpdateStockStream.comp == StockTable.symbol " +
                        "select  symbol, ifThenElse(price is null,0f,price) as price, vol as volume " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol==symbol;" +
                        "" +
                        "@info(name = 'query3') " +
                        "from CheckStockStream[(CheckStockStream.symbol==StockTable.symbol and CheckStockStream.volume==StockTable.volume and CheckStockStream.price < StockTable.price) in StockTable] " +
                        "insert into OutStream;";

                ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

                executionPlanRuntime.addCallback("query3", new QueryCallback() {
                    @Override
                    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                        EventPrinter.print(timeStamp, inEvents, removeEvents);
                        if (inEvents != null) {
                            for (Event event : inEvents) {
                                inEventCount++;
                                switch (inEventCount) {
                                    case 1:
                                        Assert.assertArrayEquals(new Object[]{"IBM", 100l, 55.6f}, event.getData());
                                        break;
                                    case 2:
                                        Assert.assertArrayEquals(new Object[]{"IBM", 200l, 55.6f}, event.getData());
                                        break;
                                    default:
                                        Assert.assertSame(2, inEventCount);
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

                InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
                InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
                InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

                executionPlanRuntime.start();

                stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
                stockStream.send(new Object[]{"IBM", 155.6f, 100l});
                checkStockStream.send(new Object[]{"IBM", 100l, 55.6f});
                checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});
                updateStockStream.send(new Object[]{"IBM", 200l});
                checkStockStream.send(new Object[]{"IBM", 200l, 55.6f});
                checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});

                Thread.sleep(1000);

                Assert.assertEquals("Number of success events", 2, inEventCount);
                Assert.assertEquals("Number of remove events", 0, removeEventCount);
                Assert.assertEquals("Event arrived", true, eventArrived);

                executionPlanRuntime.shutdown();

            }
        } catch (SQLException e) {
            log.info("Test case ignored due to DB connection unavailability");
        }

    }

    @Test
    public void insertOverwriteTableTest10() throws InterruptedException {
        log.info("insertOverwriteTableTest10");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setDataSource(RDBMSTestConstants.DATA_SOURCE_NAME, dataSource);

        try {
            if (dataSource.getConnection() != null) {
                DBConnectionHelper.getDBConnectionHelperInstance().clearDatabaseTable(dataSource, RDBMSTestConstants.TABLE_NAME);

                String streams = "" +
                        "define stream StockStream (symbol string, price float, volume long); " +
                        "define stream CheckStockStream (symbol string, volume long, price float); " +
                        "define stream UpdateStockStream (comp string, vol long); " +
                        "@from(eventtable = 'rdbms' , datasource.name = '" + RDBMSTestConstants.DATA_SOURCE_NAME + "' , table.name = '" + RDBMSTestConstants.TABLE_NAME + "') " +
                        "define table StockTable (symbol string, price float, volume long); ";
                String query = "" +
                        "@info(name = 'query1') " +
                        "from StockStream " +
                        "insert into StockTable ;" +
                        "" +
                        "@info(name = 'query2') " +
                        "from UpdateStockStream left outer join StockTable " +
                        "   on UpdateStockStream.comp == StockTable.symbol " +
                        "select comp as symbol, ifThenElse(price is null,5f,price) as price, vol as volume " +
                        "insert overwrite StockTable " +
                        "   on StockTable.symbol==symbol;" +
                        "" +
                        "@info(name = 'query3') " +
                        "from CheckStockStream[(symbol==StockTable.symbol and volume == StockTable.volume and price < StockTable.price) in StockTable] " +
                        "insert into OutStream;";

                ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

                executionPlanRuntime.addCallback("query3", new QueryCallback() {
                    @Override
                    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                        EventPrinter.print(timeStamp, inEvents, removeEvents);
                        if (inEvents != null) {
                            for (Event event : inEvents) {
                                inEventCount++;
                                switch (inEventCount) {
                                    case 1:
                                        Assert.assertArrayEquals(new Object[]{"IBM", 200l, 0f}, event.getData());
                                        break;
                                    case 2:
                                        Assert.assertArrayEquals(new Object[]{"WSO2", 300l, 4.6f}, event.getData());
                                        break;
                                    default:
                                        Assert.assertSame(2, inEventCount);
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

                InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
                InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
                InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

                executionPlanRuntime.start();

                stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
                checkStockStream.send(new Object[]{"IBM", 100l, 155.6f});
                checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});
                updateStockStream.send(new Object[]{"IBM", 200l});
                updateStockStream.send(new Object[]{"WSO2", 300l});
                checkStockStream.send(new Object[]{"IBM", 200l, 0f});
                checkStockStream.send(new Object[]{"WSO2", 300l, 4.6f});

                Thread.sleep(1000);

                Assert.assertEquals("Number of success events", 2, inEventCount);
                Assert.assertEquals("Number of remove events", 0, removeEventCount);
                Assert.assertEquals("Event arrived", true, eventArrived);

                executionPlanRuntime.shutdown();
            }
        } catch (SQLException e) {
            log.info("Test case ignored due to DB connection unavailability");
        }
    }
}
