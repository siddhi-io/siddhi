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

package org.wso2.siddhi.extension.output.transport.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.OutputTransportException;
import org.wso2.siddhi.core.exception.TestConnectionNotSupportedException;
import org.wso2.siddhi.core.publisher.MessageType;
import org.wso2.siddhi.core.publisher.OutputTransport;
import org.wso2.siddhi.query.api.execution.io.Transport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KafkaOutputTransport extends OutputTransport {

    public static final int ADAPTER_MIN_THREAD_POOL_SIZE = 8;
    public static final int ADAPTER_MAX_THREAD_POOL_SIZE = 100;
    public static final int ADAPTER_EXECUTOR_JOB_QUEUE_SIZE = 2000;
    public static final long DEFAULT_KEEP_ALIVE_TIME_IN_MILLIS = 20000;
    public static final String ADAPTER_MIN_THREAD_POOL_SIZE_NAME = "minThread";
    public static final String ADAPTER_MAX_THREAD_POOL_SIZE_NAME = "maxThread";
    public static final String ADAPTER_KEEP_ALIVE_TIME_NAME = "keepAliveTimeInMillis";
    public static final String ADAPTER_EXECUTOR_JOB_QUEUE_SIZE_NAME = "jobQueueSize";
    public final static String ADAPTOR_PUBLISH_TOPIC = "topic";
    public final static String ADAPTOR_META_BROKER_LIST = "meta.broker.list";
    public final static String ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES = "optional.configuration";
    public static final String HEADER_SEPARATOR = ",";
    public static final String ENTRY_SEPARATOR = ":";

    private static final Logger log = Logger.getLogger(KafkaOutputTransport.class);

    private static ThreadPoolExecutor threadPoolExecutor;
    private ProducerConfig config;
    private Producer<String, Object> producer;
    private Map<String, String> options;

    @Override
    public void init(Transport transportOptions, Map<String, String> unmappedDynamicOptions) throws OutputTransportException {
        //ThreadPoolExecutor will be assigned  if it is null
        Logger.getLogger("kafka").setLevel(Level.WARN); // TODO: 1/9/17 Ramindu
        if (threadPoolExecutor == null) {
            int minThread;
            int maxThread;
            int jobQueSize;
            long defaultKeepAliveTime;
            options = transportOptions.getOptions();
            //If global properties are available those will be assigned else constant values will be assigned
            minThread = (options.get(ADAPTER_MIN_THREAD_POOL_SIZE_NAME) != null)
                    ? Integer.parseInt(options.get(ADAPTER_MIN_THREAD_POOL_SIZE_NAME))
                    : ADAPTER_MIN_THREAD_POOL_SIZE;

            maxThread = (options.get(ADAPTER_MAX_THREAD_POOL_SIZE_NAME) != null)
                    ? Integer.parseInt(options.get(ADAPTER_MAX_THREAD_POOL_SIZE_NAME))
                    : ADAPTER_MAX_THREAD_POOL_SIZE;

            defaultKeepAliveTime = (options.get(ADAPTER_KEEP_ALIVE_TIME_NAME) != null)
                    ? Integer.parseInt(options.get(ADAPTER_KEEP_ALIVE_TIME_NAME))
                    : DEFAULT_KEEP_ALIVE_TIME_IN_MILLIS;

            jobQueSize = (options.get(ADAPTER_EXECUTOR_JOB_QUEUE_SIZE_NAME) != null)
                    ? Integer.parseInt(options.get(ADAPTER_EXECUTOR_JOB_QUEUE_SIZE_NAME))
                    : ADAPTER_EXECUTOR_JOB_QUEUE_SIZE;

            threadPoolExecutor = new ThreadPoolExecutor(minThread, maxThread, defaultKeepAliveTime,
                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(jobQueSize));
        }
    }

    @Override
    public void testConnect() throws TestConnectionNotSupportedException, ConnectionUnavailableException {
        throw new TestConnectionNotSupportedException("Test connection is not available");
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        String kafkaConnect = options.get(ADAPTOR_META_BROKER_LIST);
        String optionalConfigs = options.get(ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES);
        Properties props = new Properties();
        props.put("metadata.broker.list", kafkaConnect);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        if (optionalConfigs != null) {
            String[] optionalProperties = optionalConfigs.split(HEADER_SEPARATOR);
            if (optionalProperties != null && optionalProperties.length > 0) {
                for (String header : optionalProperties) {
                    try {
                        String[] configPropertyWithValue = header.split(ENTRY_SEPARATOR, 2);
                        props.put(configPropertyWithValue[0], configPropertyWithValue[1]);
                    } catch (Exception e) {
                        log.warn("Optional property '" + header + "' is not defined in the correct format.", e);
                    }
                }
            }
        }
        config = new ProducerConfig(props);
        producer = new Producer<String, Object>(config);
    }

    @Override
    public void publish(Object event, Map<String, String> dynamicOptions) throws ConnectionUnavailableException {
        //By default auto.create.topics.enable is true, then no need to create topic explicitly
        String topic = dynamicOptions.get(ADAPTOR_PUBLISH_TOPIC);
        topic = "page_visits";
        try {
            threadPoolExecutor.submit(new KafkaSender(topic, event));
        } catch (RejectedExecutionException e) {
//            EventAdapterUtil.logAndDrop(eventAdapterConfiguration.getName(), event, "Job queue is full", e, log, tenantId);
            log.error("Job queue is full : " + e.getMessage(), e);
        }
    }

    @Override
    public void disconnect() {
        //close producer
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public void destroy() {
        //not required
    }

    @Override
    public boolean isPolled() {
        return false;
    }

    @Override
    public List<String> getSupportedMessageFormats() {
        return new ArrayList<String>() {{
            add(MessageType.TEXT);
            add(MessageType.XML);
            add(MessageType.JSON);
        }};
    }

    private class KafkaSender implements Runnable {

        String topic;
        Object message;

        KafkaSender(String topic, Object message) {
            this.topic = topic;
            this.message = message;
        }

        @Override
        public void run() {
            try {
                KeyedMessage<String, Object> data = new KeyedMessage<String, Object>(topic, message.toString());
                producer.send(data);
            } catch (Throwable e) {
                log.error("Unexpected error when sending event via Kafka Output Adatper:" + e.getMessage(), e);
            }
        }
    }
}
