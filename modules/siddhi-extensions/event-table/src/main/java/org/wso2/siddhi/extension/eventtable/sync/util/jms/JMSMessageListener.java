/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.siddhi.extension.eventtable.sync.util.jms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.extension.eventtable.SyncEventTable;
import org.wso2.siddhi.extension.eventtable.sync.SyncTableHandler;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class JMSMessageListener implements MessageListener {
    private static final Log log = LogFactory.getLog(JMSMessageListener.class);
    SyncTableHandler syncTableHandler;
    SyncEventTable syncEventTable;


    public JMSMessageListener(SyncTableHandler syncTableHandler, SyncEventTable syncEventTable) {
        this.syncTableHandler = syncTableHandler;
        this.syncEventTable = syncEventTable;

    }

    public void onMessage(Message message) {
        try {

            if (message != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Event received in JMS Event Receiver - " + message);
                } else if (message instanceof MapMessage) {
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
                    log.warn("Event dropped due to unsupported message type");
                }
            } else {
                log.warn("Dropping the empty/null event received through jms receiver");
            }
        } catch (JMSException e) {
            log.error(e);
        }
    }
}
