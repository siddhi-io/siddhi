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

import kafka.consumer.ConsumerConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.subscription.InputCallback;
import org.wso2.siddhi.core.subscription.InputTransport;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class KafkaInputTransport extends InputTransport {

    private InputCallback inputCallback;
    private ScheduledExecutorService executorService;
    private static ThreadPoolExecutor threadPoolExecutor;
    private Map<String, String> transportOptions;
    private ConsumerKafkaAdaptor consumerKafkaAdaptor;

    //    public static final int ADAPTER_CORE_POOL_SIZE = 8;
//    public final static String ADAPTER_CORE_POOL_SIZE_NAME = "kafka";
    public final static String ADAPTER_NAME = "adapter.name";
    public static final String EVENTS_DUPLICATED_IN_CLUSTER_NAME = "events.duplicated.in.cluster";
    public static final boolean EVENTS_DUPLICATED_IN_CLUSTER = false;
    public final static String ADAPTOR_SUSCRIBER_TOPIC = "topic";
    public final static String ADAPTOR_SUSCRIBER_GROUP_ID = "group.id";
    public final static String ADAPTOR_SUSCRIBER_ZOOKEEPER_CONNECT = "zookeeper.connect";
    public final static String ADAPTOR_SUSCRIBER_THREADS = "threads";
    public final static String ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES = "optional.configuration";
    public static final int AXIS_TIME_INTERVAL_IN_MILLISECONDS = 10000;

    private static final Logger log = Logger.getLogger(KafkaInputTransport.class);

    @Override
    public void init(Map<String, String> transportOptions, InputCallback inputCallback) {
        Logger.getLogger("kafka").setLevel(Level.WARN); //todo Ramindu
        this.inputCallback = inputCallback;
        this.transportOptions = transportOptions;
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        createKafkaAdaptorListener();
    }

    @Override
    public void disconnect() {
        if (consumerKafkaAdaptor != null) {
            consumerKafkaAdaptor.shutdown();
            String topic = transportOptions.get(ADAPTOR_SUSCRIBER_TOPIC);
            log.debug("Adapter " + transportOptions.get(ADAPTER_NAME) + " disconnected " + topic);
        }
    }

    @Override
    public void destroy() {

    }

    @Override
    public boolean isEventDuplicatedInCluster() {
        return (transportOptions.get(EVENTS_DUPLICATED_IN_CLUSTER_NAME) != null)
                ? Boolean.parseBoolean(transportOptions.get(EVENTS_DUPLICATED_IN_CLUSTER_NAME))
                : EVENTS_DUPLICATED_IN_CLUSTER;
    }

    @Override
    public boolean isPolling(){
        return true;
    }


    private void createKafkaAdaptorListener() throws ConnectionUnavailableException{

        String zkConnect = transportOptions.get(ADAPTOR_SUSCRIBER_ZOOKEEPER_CONNECT);
        String groupID = transportOptions.get(ADAPTOR_SUSCRIBER_GROUP_ID);
        String threadsStr = transportOptions.get(ADAPTOR_SUSCRIBER_THREADS);
        String optionalConfiguration = transportOptions.get(ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES);
        int threads = Integer.parseInt(threadsStr);
        String topic = transportOptions.get(ADAPTOR_SUSCRIBER_TOPIC);

//        consumerKafkaAdaptor = new ConsumerKafkaAdaptor(topic,
//                KafkaInputTransport.createConsumerConfig(zkConnect, groupID, optionalConfiguration));
        consumerKafkaAdaptor = new ConsumerKafkaAdaptor(topic, KafkaInputTransport.createConsumerConfig(zkConnect, groupID));
        consumerKafkaAdaptor.run(threads, inputCallback);
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    private static ConsumerConfig createConsumerConfig2(String zookeeper, String groupId,
                                                       String optionalConfigs) throws ConnectionUnavailableException{
        try {
            Properties props = new Properties();
            props.put(ADAPTOR_SUSCRIBER_ZOOKEEPER_CONNECT, zookeeper);
            props.put(ADAPTOR_SUSCRIBER_GROUP_ID, groupId);
            if (optionalConfigs != null) {
                String[] optionalProperties = optionalConfigs.split(",");
                if (optionalProperties != null) {
                    for (String header : optionalProperties) {
                        String[] configPropertyWithValue = header.split(":", 2);
                        if (configPropertyWithValue.length == 2) {
                            props.put(configPropertyWithValue[0], configPropertyWithValue[1]);
                        } else {
                            log.warn("Optional configuration property not defined in the correct format.\nRequired - property_name1:property_value1,property_name2:property_value2\nFound - " + optionalConfigs);
                        }
                    }
                }
            }
            return new ConsumerConfig(props);
        } catch (NoClassDefFoundError e) {
            throw new ConnectionUnavailableException("Cannot access kafka context due to missing jars", e);
        }
    }
}