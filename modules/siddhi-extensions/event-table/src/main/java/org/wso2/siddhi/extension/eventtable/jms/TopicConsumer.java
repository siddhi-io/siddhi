/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.extension.eventtable.jms;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.extension.eventtable.SyncEventTable;
import org.wso2.siddhi.extension.eventtable.sync.SyncTableHandler;

import javax.jms.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class TopicConsumer implements Runnable {

    private TopicConnectionFactory topicConnectionFactory;
    private String topicName;
    private boolean active = true;
    private SyncTableHandler syncTableHandler;
    private SyncEventTable syncEventTable;
    private static Log log = LogFactory.getLog(TopicConsumer.class);

    public TopicConsumer(TopicConnectionFactory topicConnectionFactory, String topicName, SyncTableHandler syncTableHandler, SyncEventTable syncEventTable) {
        this.topicConnectionFactory = topicConnectionFactory;
        this.topicName = topicName;
        this.syncTableHandler = syncTableHandler;
        this.syncEventTable = syncEventTable;
    }

    public void run() {
        // create topic connection
        TopicConnection topicConnection = null;
        try {
            topicConnection = topicConnectionFactory.createTopicConnection();
            topicConnection.start();
        } catch (JMSException e) {
            log.error("Can not create topic connection." + e.getMessage(), e);
            return;
        }
        Session session = null;
        try {
            session = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic(topicName);
            MessageConsumer consumer = session.createConsumer(destination);
            log.info("Listening for messages");
            while (active) {
                Message message = consumer.receive();
                if (message != null) {
                    if (message instanceof MapMessage) {
                        MapMessage mapMessage = (MapMessage) message;
                        Map<String, Object> map = new HashMap<String, Object>();
                        Enumeration enumeration = mapMessage.getMapNames();
                        while (enumeration.hasMoreElements()) {
                            String key = (String) enumeration.nextElement();
                            map.put(key, mapMessage.getObject(key));
                        }

                        String throttleKey = map.get("throttle_key").toString();
                        String throttleState = map.get("isThrottled").toString();
                        if (throttleState.equals("true")) {
                            syncTableHandler.addToBloomFilters(throttleKey);
                            StreamEvent streamEvent = new StreamEvent(0, 0, 1);
                            streamEvent.setOutputData(new Object[]{throttleKey});
                            syncEventTable.addToInMemoryEventMap(throttleKey, streamEvent);
                        } else {
                            syncTableHandler.removeFromBloomFilters(throttleKey);
                            syncEventTable.removeFromMemoryEventMap(throttleKey);
                        }

                    } else {
                        log.error("Incorrect message format received. Expecting javax.jms.MapMessage");
                    }


                }
            }
            log.info("Finished listening for messages.");
            session.close();
            topicConnection.stop();
            topicConnection.close();
        } catch (JMSException e) {
            log.error("Can not subscribe." + e.getMessage(), e);
        }
    }

    public void shutdown() {
        active = false;
    }
}
