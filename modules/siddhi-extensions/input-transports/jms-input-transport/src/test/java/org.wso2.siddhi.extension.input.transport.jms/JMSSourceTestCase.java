/*
 *  Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.siddhi.extension.input.transport.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.input.transport.jms.client.JMSClient;

import java.util.ArrayList;
import java.util.List;

public class JMSSourceTestCase {
    private final String PROVIDER_URL = "vm://localhost?broker.persistent=false";
    private List<String> receivedEventNameList;

    @Test
    public void TestJMSTopicSource() throws InterruptedException {
        receivedEventNameList = new ArrayList<>(2);

        // starting the ActiveMQ broker
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(PROVIDER_URL);

        // deploying the siddhi app
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@source(type='jms', @map(type='text'), "
                + "factory.initial='org.apache.activemq.jndi.ActiveMQInitialContextFactory', "
                + "provider.url='vm://localhost',"
                + "destination='DAS_JMS_TEST', "
                + "connection.factory.type='topic',"
                + "connection.factory.jndi.name='TopicConnectionFactory',"
                + "transport.jms.SubscriptionDurable='true', "
                + "transport.jms.DurableSubscriberClientID='wso2dasclient1'"
                + ")" +
                "define stream inputStream (name string, age int, country string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    receivedEventNameList.add(event.getData(0).toString());
                }
            }
        });
        siddhiAppRuntime.start();

        // publishing events
        publishEvents("DAS_JMS_TEST", null, "activemq", "text", "src/test/resources/events/events_text.txt");

        List<String> expected = new ArrayList<>(2);
        expected.add("\nJohn");
        expected.add("\nMike");
        Assert.assertEquals("JMS Source expected input not received", expected, receivedEventNameList);
    }

    private void publishEvents(String topicName, String queueName, String broker, String format, String filePath)
            throws InterruptedException {
        JMSClient jmsClient = new JMSClient();
        jmsClient.sendJMSEvents(filePath, topicName, queueName, format, broker, PROVIDER_URL);
        Thread.sleep(5000);
    }
}
