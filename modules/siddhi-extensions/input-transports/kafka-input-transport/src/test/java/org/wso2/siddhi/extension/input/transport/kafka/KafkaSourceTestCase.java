/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.extension.input.transport.kafka;

import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;
import org.wso2.siddhi.core.util.snapshot.PersistenceReference;
import org.wso2.siddhi.extension.input.mapper.text.TextSourceMapper;
import org.wso2.siddhi.extension.output.mapper.text.TextSinkMapper;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Variable;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaSourceTestCase {
    private static final Logger log = Logger.getLogger(KafkaSourceTestCase.class);
    private static TestingServer zkTestServer;
    private static KafkaServerStartable kafkaServer;
    private static ExecutorService executorService;
    private static final String kafkaLogDir = "tmp_kafka_dir";
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeClass
    public static void init() throws Exception {
        try {
            executorService = Executors.newFixedThreadPool(5);
            cleanLogDir();
            setupKafkaBroker();
            Thread.sleep(3000);
        } catch (Exception e) {
            throw new RemoteException("Exception caught when starting server", e);
        }
    }

    @Before
    public void init2() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testKafkaMultipleTopicPartitionPartitionWiseSubscription() throws InterruptedException {
        try {
            log.info("Creating test for multiple topics and partitions and thread partition wise");
            String topics[] = new String[]{"kafka_topic", "kafka_topic2"};
            createTopic(topics, 2);
            SiddhiManager siddhiManager = new SiddhiManager();
            siddhiManager.setExtension("source.mapper:text", TextSourceMapper.class);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@app:name('TestSiddhiApp') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic='kafka_topic,kafka_topic2', group.id='test', " +
                            "threading.option='partition.wise', bootstrap.servers='localhost:9092', " +
                            "partition.no.list='0,1', " +
                            "@map(type='text'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        System.out.println(event);
                        eventArrived = true;
                        count++;
                        switch (count) {
                            case 1:
                                assertEquals(0, event.getData(2));
                                break;
                            case 2:
                                assertEquals(0, event.getData(2));
                                break;
                            case 3:
                                assertEquals(1, event.getData(2));
                                break;
                            case 4:
                                assertEquals(1, event.getData(2));
                                break;
                            default:
                                org.junit.Assert.fail();
                        }
                    }

                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(2000);
            kafkaPublisher(topics, 2, 2);
            Thread.sleep(5000);
            assertEquals(4, count);
            assertTrue(eventArrived);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test
    public void testAKafkaPauseAndResume() throws InterruptedException {
        try {
            log.info("Test to verify the pause and resume functionality of Kafka source");
            String topics[] = new String[]{"kafka_topic3"};
            createTopic(topics, 2);
            SiddhiManager siddhiManager = new SiddhiManager();
            siddhiManager.setExtension("source.mapper:text", TextSourceMapper.class);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@app:name('TestSiddhiApp') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic='kafka_topic3', group.id='test1', threading" +
                            ".option='partition.wise', " +
                            "bootstrap.servers='localhost:9092', partition.no.list='0,1', " +
                            "@map(type='text'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        System.out.println(event);
                        eventArrived = true;
                        count++;
                    }

                }
            });
            siddhiAppRuntime.start();
            Future eventSender = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    kafkaPublisher(topics, 2, 4);
                }
            });
            while (!eventSender.isDone()) {
                Thread.sleep(1000);
            }
            Thread.sleep(2000);
            assertEquals(4, count);
            assertTrue(eventArrived);

            Collection<List<Source>> sources = siddhiAppRuntime.getSources();
            // pause the transports
            sources.forEach(e -> e.forEach(Source::pause));

            init2();
            eventSender = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    kafkaPublisher(topics, 2, 4);
                }
            });
            while (!eventSender.isDone()) {
                Thread.sleep(1000);
            }
            Thread.sleep(5000);
            assertFalse(eventArrived);

            // resume the transports
            sources.forEach(e -> e.forEach(Source::resume));
            Thread.sleep(2000);
            assertEquals(4, count);
            assertTrue(eventArrived);

            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test
    public void testRecoveryOnFailureOfSingleNodeWithKafka() throws InterruptedException {
        try {
            log.info("Test to verify recovering process of a Siddhi node on a failure when Kafka is the event source");
            String topics[] = new String[]{"kafka_topic4"};
            createTopic(topics, 1);
            PersistenceStore persistenceStore = new InMemoryPersistenceStore();
            SiddhiManager siddhiManager = new SiddhiManager();
            siddhiManager.setPersistenceStore(persistenceStore);
            siddhiManager.setExtension("source.mapper:text", TextSourceMapper.class);

            String query = "@app:name('TestSiddhiApp') " +
                    "define stream BarStream (count long); " +
                    "@info(name = 'query1') " +
                    "@source(type='kafka', topic='kafka_topic4', group.id='test', " +
                    "threading.option='topic.wise', bootstrap.servers='localhost:9092', partition.no.list='0', " +
                    "@map(type='text'))" +
                    "Define stream FooStream (symbol string, price float, volume long);" +
                    "from FooStream select count(symbol) as count insert into BarStream;";
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(query);
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        eventArrived = true;
                        System.out.println(event);
                        count = Math.toIntExact((long) event.getData(0));
                    }

                }
            });

            // start publishing events to Kafka
            Future eventSender = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    kafkaPublisher(topics, 1, 50, 1000);
                }
            });
            Thread.sleep(2000);
            // start the siddhi app
            siddhiAppRuntime.start();

            // wait for some time
            Thread.sleep(28000);
            // initiate a checkpointing task
            PersistenceReference perisistor = siddhiAppRuntime.persist();
            // waits till the checkpointing task is done
            while (!perisistor.getFuture().isDone()) {
                Thread.sleep(100);
            }
            // let few more events to be published
            Thread.sleep(5000);
            // initiate a siddhi app shutdown - to demonstrate a node failure
            siddhiAppRuntime.shutdown();
            // let few events to be published while the siddhi app is down
            Thread.sleep(5000);
            // recreate the siddhi app
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(query);
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        eventArrived = true;
                        System.out.println(event);
                        count = Math.toIntExact((long) event.getData(0));
                    }

                }
            });
            // start the siddhi app
            siddhiAppRuntime.start();
            // immediately trigger a restore from last revision
            siddhiAppRuntime.restoreLastRevision();
            Thread.sleep(5000);

            // waits till all the events are published
            while (!eventSender.isDone()) {
                Thread.sleep(2000);
            }

            Thread.sleep(20000);
            assertTrue(eventArrived);
            // assert the count
            assertEquals(50, count);

            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    //TODO fix event id passing logic
    //@Test
    public void testRecoveryOnFailureOfMultipleNodeWithKafka() throws InterruptedException {
        try {
            log.info("Test to verify recovering process of multiple Siddhi nodes on a failure when Kafka is the event" +
                    " source");
            String topics[] = new String[]{"kafka_topic5", "kafka_topic6"};
            createTopic(topics, 1);
            // 1st node
            PersistenceStore persistenceStore = new InMemoryPersistenceStore();
            SiddhiManager siddhiManager1 = new SiddhiManager();
            siddhiManager1.setPersistenceStore(persistenceStore);
            siddhiManager1.setExtension("inputmapper:text", TextSourceMapper.class);

            // 2nd node
            PersistenceStore persistenceStore1 = new InMemoryPersistenceStore();
            SiddhiManager siddhiManager2 = new SiddhiManager();
            siddhiManager2.setPersistenceStore(persistenceStore1);
            siddhiManager2.setExtension("inputmapper:text", TextSourceMapper.class);

            String query1 = "@app:name('TestSiddhiApp') " +
                    "@sink(type='kafka', topic='kafka_topic6', bootstrap.servers='localhost:9092', partition" +
                    ".no='0', " +
                    "@map(type='text'))" +
                    "define stream BarStream (count long); " +
                    "@source(type='kafka', topic='kafka_topic5', group.id='test', " +
                    "threading.option='topic.wise', bootstrap.servers='localhost:9092', partition.no.list='0', " +
                    "@map(type='text'))" +
                    "Define stream FooStream (symbol string, price float, volume long);" +
                    "@info(name = 'query1') " +
                    "from FooStream select count(symbol) as count insert into BarStream;";

            String query2 = "@app:name('TestSiddhiApp') " +
                    "define stream BarStream (count long); " +
                    "@source(type='kafka', topic='kafka_topic6', " +
                    "threading.option='topic.wise', bootstrap.servers='localhost:9092', partition.no.list='0', " +
                    "@map(type='text'))" +
                    "Define stream FooStream (number long);" +
                    "@info(name = 'query1') " +
                    "from FooStream select count(number) as count insert into BarStream;";

            SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(query1);
            SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(query2);

            siddhiAppRuntime2.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        eventArrived = true;
                        System.out.println(event);
                        count = Math.toIntExact((long) event.getData(0));
                    }

                }
            });

            // start the siddhi app
            siddhiAppRuntime1.start();
            siddhiAppRuntime2.start();
            // let it initialize
            Thread.sleep(2000);

            // start publishing events to Kafka
            Future eventSender = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    kafkaPublisher(new String[]{"kafka_topic5"}, 1, 50, 1000);
                }
            });

            // wait for some time
            Thread.sleep(28000);
            // initiate a checkpointing task
            PersistenceReference perisistor1 = siddhiAppRuntime1.persist();
            PersistenceReference perisistor2 = siddhiAppRuntime2.persist();
            // waits till the checkpointing task is done
            while (!perisistor1.getFuture().isDone() && !perisistor2.getFuture().isDone()) {
                Thread.sleep(100);
            }
            // let few more events to be published
            Thread.sleep(5000);
            // initiate a siddhi app shutdown - to demonstrate a node failure
            siddhiAppRuntime1.shutdown();
            siddhiAppRuntime2.shutdown();
            // let few events to be published while the siddhi app is down
            Thread.sleep(5000);
            // recreate the siddhi app
            siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(query1);
            siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(query2);
            siddhiAppRuntime2.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        eventArrived = true;
                        System.out.println(event);
                        count = Math.toIntExact((long) event.getData(0));
                    }

                }
            });
            // start the siddhi app
            siddhiAppRuntime1.start();
            siddhiAppRuntime2.start();
            // immediately trigger a restore from last revision
            siddhiAppRuntime1.restoreLastRevision();
            siddhiAppRuntime2.restoreLastRevision();
            Thread.sleep(5000);

            // waits till all the events are published
            while (!eventSender.isDone()) {
                Thread.sleep(2000);
            }

            Thread.sleep(20000);
            assertTrue(eventArrived);
            // assert the count
            assertEquals(50, count);

            siddhiAppRuntime1.shutdown();
            siddhiAppRuntime2.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

//    @Test
    public void testKafkaMultipleTopicPartitionTopicWiseSubscription() throws InterruptedException {
        try{
            log.info("Creating test for multiple topics and partitions and thread topic wise");
            SiddhiManager siddhiManager = new SiddhiManager();
            siddhiManager.setExtension("source.mapper:text", TextSourceMapper.class);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@app:name('TestSiddhiApp') " +
                    "define stream BarStream (symbol string, price float, volume long); " +
                    "@info(name = 'query1') " +
                        "@source(type='kafka', topic='kafka_topic,kafka_topic2', group.id='test', threading.option='topic.wise', " +
                            "bootstrap.servers='localhost:9092', partition.no.list='0,1', " +
                            "@map(type='text'))" +
                                "Define stream FooStream (symbol string, price float, volume long);" +
                    "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        System.out.println(event);
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(20000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    //    @Test
    public void testKafkaMultipleTopicPartitionSingleThreadSubscription() throws InterruptedException {
        try {
            log.info("Creating test for multiple topics and partitions on single thread");
            SiddhiManager siddhiManager = new SiddhiManager();
            siddhiManager.setExtension("source.mapper:text", TextSourceMapper.class);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@app:name('TestSiddhiApp') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic='kafka_topic,kafka_topic2', group.id='test', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092', " +
                            "partition.no.list='0,1', " +
                            "@map(type='text'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        System.out.println(event);
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(20000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    //    @Test
    public void testKafkaSingleTopicSubscriptionWithPartition() throws InterruptedException {
        try {
            log.info("Creating test for single topic with multiple partitions on single thread");
            SiddhiManager siddhiManager = new SiddhiManager();
            siddhiManager.setExtension("source.mapper:text", TextSourceMapper.class);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@app:name('TestSiddhiApp') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic='kafka_topic', group.id='test', threading.option='single" +
                            ".thread', " +
                            "bootstrap.servers='localhost:9092', partition.no.list='0,1', " +
                            "@map(type='text'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        System.out.println(event);
                    }
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(20000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    //    @Test
    public void testCreatingKafkaSubscriptionWithoutPartition() throws InterruptedException {
        try {
            log.info("Creating test for multiple topic with no partitions on single thread");
            SiddhiManager siddhiManager = new SiddhiManager();
            siddhiManager.setExtension("source.mapper:text", TextSourceMapper.class);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@app:name('TestSiddhiApp') " +
                            "define stream BarStream (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@source(type='kafka', topic='simple_topic,simple_topic2', group.id='test', " +
                            "threading.option='single.thread', bootstrap.servers='localhost:9092', " +
                            "@map(type='text'))" +
                            "Define stream FooStream (symbol string, price float, volume long);" +
                            "from FooStream select symbol, price, volume insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    EventPrinter.print(events);
                }
            });
            siddhiAppRuntime.start();
            Thread.sleep(20000);
            siddhiAppRuntime.shutdown();
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    //    @Test
    public void testCreatingFullKafkaEventFlow() throws InterruptedException {
        Runnable kafkaReceiver = new KafkaFlow();
        Thread t1 = new Thread(kafkaReceiver);
        t1.start();
        Thread.sleep(35000);
    }

    //---- private methods --------
    private static void setupKafkaBroker() {
        try {
            // mock zookeeper
            zkTestServer = new TestingServer(2181);
            // mock kafka
            Properties props = new Properties();
            props.put("broker.id", "0");
            props.put("host.name", "localhost");
            props.put("port", "9092");
            props.put("log.dir", kafkaLogDir);
            props.put("zookeeper.connect", zkTestServer.getConnectString());
            props.put("replica.socket.timeout.ms", "30000");
            props.put("delete.topic.enable", "true");
            KafkaConfig config = new KafkaConfig(props);
            kafkaServer = new KafkaServerStartable(config);
            kafkaServer.startup();
        } catch (Exception e) {
            log.error("Error running local Kafka broker / Zookeeper", e);
        }
    }

    private void createTopic(String topics[], int numOfPartitions) {
        ZkClient zkClient = new ZkClient(zkTestServer.getConnectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkConnection zkConnection = new ZkConnection(zkTestServer.getConnectString());
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        for (String topic : topics) {
            try {
                AdminUtils.createTopic(zkUtils, topic, numOfPartitions, 1, new Properties());
            } catch (TopicExistsException e) {
                log.warn("topic exists for: " + topic);
            }
        }
        zkClient.close();
    }

    @AfterClass
    public static void stopKafkaBroker() {
        try {
            if (kafkaServer != null) {
                kafkaServer.shutdown();
            }
            Thread.sleep(5000);
            if (zkTestServer != null) {
                zkTestServer.stop();
            }
            Thread.sleep(5000);
            cleanLogDir();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        } catch (IOException e) {
            log.error("Error shutting down Kafka broker / Zookeeper", e);
        }
    }

    private static void cleanLogDir() {
        try {
            File f = new File(kafkaLogDir);
            FileUtils.deleteDirectory(f);
        } catch (IOException e) {
            log.error("Failed to clean up: " + e);
        }
    }

    private void kafkaPublisher(String topics[], int numOfPartitions, int numberOfEvents, long sleep) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < numberOfEvents; i++) {
            String msg = "wso2,12.5," + i;
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
            }
            for (String topic : topics) {
                if (numOfPartitions > 1) {
                    System.out.println("producing: " + msg + " into partition: " + (i % numOfPartitions));
                    producer.send(new ProducerRecord<>(topic, String.valueOf(i % numOfPartitions), msg));
                } else {
                    System.out.println("Produced " + i);
                    producer.send(new ProducerRecord<>(topic, msg));
                }
            }
        }
        producer.close();
    }

    private void kafkaPublisher(String topics[], int numOfPartitions, int numberOfEvents) {
        kafkaPublisher(topics, numOfPartitions, numberOfEvents, 1000);
    }

    private class KafkaFlow implements Runnable {
        @Override
        public void run() {
            try {
                StreamDefinition inputDefinition = StreamDefinition.id("FooStream")
                        .attribute("symbol", Attribute.Type.STRING)
                        .attribute("price", Attribute.Type.FLOAT)
                        .attribute("volume", Attribute.Type.INT)
                        .annotation(Annotation.annotation("source")
                                .element("type", "kafka")
                                .element("topic", "receiver_topic")
                                .element("threads", "1")
                                .element("partition.no.list", "0,1")
                                .element("group.id", "group1")
                                .element("bootstrap.servers", "localhost:9092")
                                .annotation(Annotation.annotation("map")
                                        .element("type", "text")));

                StreamDefinition outputDefinition = StreamDefinition.id("BarStream")
                        .attribute("symbol", Attribute.Type.STRING)
                        .attribute("price", Attribute.Type.FLOAT)
                        .attribute("volume", Attribute.Type.INT)
                        .annotation(Annotation.annotation("sink")
                                .element("type", "kafka")
                                .element("topic", "publisher_topic")
                                .element("partition.no", "0")
                                .element("bootstrap.servers", "localhost:9092")
                                .annotation(Annotation.annotation("map")
                                        .element("type", "text")));

                Query query = Query.query();
                query.from(
                        InputStream.stream("FooStream")
                );
                query.select(
                        Selector.selector().select(new Variable("symbol")).select(new Variable("price")).select(new
                                Variable("volume"))
                );
                query.insertInto("BarStream");

                SiddhiManager siddhiManager = new SiddhiManager();
                siddhiManager.setExtension("source.mapper:text", TextSourceMapper.class);
                siddhiManager.setExtension("sink.mapper:text", TextSinkMapper.class);

                SiddhiApp siddhiApp = new SiddhiApp("ep1");
                siddhiApp.defineStream(inputDefinition);
                siddhiApp.defineStream(outputDefinition);
                siddhiApp.addQuery(query);
                SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

                siddhiAppRuntime.addCallback("FooStream", new StreamCallback() {
                    @Override
                    public void receive(Event[] events) {
                        System.out.println("Printing received events !!");
                        EventPrinter.print(events);
                    }
                });
                siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                    @Override
                    public void receive(Event[] events) {
                        System.out.println("Printing publishing events !!");
                        EventPrinter.print(events);
                    }
                });
                siddhiAppRuntime.start();
                Thread.sleep(30000);
                siddhiAppRuntime.shutdown();
            } catch (ZkTimeoutException ex) {
                log.warn("No zookeeper may not be available.", ex);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

