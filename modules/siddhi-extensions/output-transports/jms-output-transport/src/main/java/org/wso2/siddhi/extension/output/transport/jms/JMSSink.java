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
package org.wso2.siddhi.extension.output.transport.jms;

import org.wso2.carbon.transport.jms.sender.JMSClientConnector;
import org.wso2.carbon.transport.jms.utils.JMSConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.extension.output.transport.jms.util.JMSOptionsMapper;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@Extension(
        name = "jms",
        namespace = "sink",
        description = "JMS Output Transport",
        examples = @Example(description = "TBD", syntax = "TBD")
)
/**
 * JMS output transport class.
 * Dynamic options: destination
 */
public class JMSSink extends Sink {
    private OptionHolder optionHolder;
    private JMSClientConnector clientConnector;
    private Option destination;
    private Map<String, String> jmsStaticProperties;
    private ExecutorService executorService;

    @Override
    protected void init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                        ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        this.optionHolder = optionHolder;
        this.destination = optionHolder.getOrCreateOption(JMSConstants.DESTINATION_PARAM_NAME, null);
        this.jmsStaticProperties = initJMSProperties();
        this.executorService = siddhiAppContext.getExecutorService();
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        this.clientConnector = new JMSClientConnector();
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }

    @Override
    public void publish(Object payload, DynamicOptions transportOptions) throws ConnectionUnavailableException {
        String topicQueueName = destination.getValue(transportOptions);
        executorService.submit(new JMSPublisher(topicQueueName, jmsStaticProperties, clientConnector, payload));
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, Map.class, Byte[].class};
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{JMSConstants.DESTINATION_PARAM_NAME};
    }

    @Override
    public void disconnect() {

    }

    @Override
    public void destroy() {

    }

    /**
     * Initializing JMS properties.
     * The properties in the required options list are mandatory.
     * Other JMS options can be passed in as key value pairs, key being in the JMS spec or the broker spec.
     *
     * @return all the options map.
     */
    private Map<String, String> initJMSProperties() {
        List<String> requiredOptions = JMSOptionsMapper.getRequiredOptions();
        Map<String, String> customPropertyMapping = JMSOptionsMapper.getCustomPropertyMapping();
        // getting the required values
        Map<String, String> transportProperties = new HashMap<>();
        requiredOptions.forEach(requiredOption ->
                transportProperties.put(customPropertyMapping.get(requiredOption),
                        optionHolder.validateAndGetStaticValue(requiredOption)));
        // getting optional values
        optionHolder.getStaticOptionsKeys().stream()
                .filter(option -> !requiredOptions.contains(option) && !option.equals("type")).forEach(option ->
                transportProperties.put(option, optionHolder.validateAndGetStaticValue(option)));
        return transportProperties;
    }
}
