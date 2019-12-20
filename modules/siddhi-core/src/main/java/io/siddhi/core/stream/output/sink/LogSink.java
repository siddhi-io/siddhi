/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.stream.output.sink;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.util.Arrays;

/**
 * Implementation of {@link Sink} which can be used as a logger. This will log the output events in the output stream
 * with user specified priority and a prefix
 */
@Extension(
        name = "log",
        namespace = "sink",
        description = "This is a sink that can be used as a logger. This will log the output events in the output " +
                "stream with user specified priority and a prefix",
        parameters = {
                @Parameter(
                        name = "priority",
                        type = DataType.STRING,
                        description = "This will set the logger priority i.e log level. Accepted values are INFO, " +
                                "DEBUG, WARN, FATAL, ERROR, OFF, TRACE",
                        optional = true,
                        defaultValue = "INFO"
                ),
                @Parameter(
                        name = "prefix",
                        type = DataType.STRING,
                        description = "This will be the prefix to the output message. If the output stream has event " +
                                "[2,4] and the prefix is given as \"Hello\" then the log will show \"Hello : [2,4]\"",
                        optional = true,
                        defaultValue = "default prefix will be <Siddhi App Name> : <Stream Name>"
                )
        },
        parameterOverloads = {
                @ParameterOverload(),
                @ParameterOverload(parameterNames = {"priority"}),
                @ParameterOverload(parameterNames = {"prefix"}),
                @ParameterOverload(parameterNames = {"priority", "prefix"})
        },
        examples = {
                @Example(
                        syntax = "@sink(type='log', prefix='My Log', priority='DEBUG') \n" +
                                "define stream BarStream (symbol string, price float, volume long)",
                        description = "In this example BarStream uses log sink and the prefix is given as My Log. " +
                                "Also the priority is set to DEBUG."
                ),
                @Example(
                        syntax = "@sink(type='log', priority='DEBUG') \n" +
                                "define stream BarStream (symbol string, price float, volume long)",
                        description = "In this example BarStream uses log sink and the priority is set to DEBUG. " +
                                "User has not specified prefix so the default prefix will be in the form " +
                                "<Siddhi App Name> : <Stream Name>"
                ),
                @Example(
                        syntax = "@sink(type='log', prefix='My Log') \n" +
                                "define stream BarStream (symbol string, price float, volume long)",
                        description = "In this example BarStream uses log sink and the prefix is given as My Log. " +
                                "User has not given a priority so it will be set to default INFO."
                ),
                @Example(
                        syntax = "@sink(type='log') \n" +
                                "define stream BarStream (symbol string, price float, volume long)",
                        description = "In this example BarStream uses log sink. The user has not given prefix or " +
                                "priority so they will be set to their default values."
                )
        }
)

public class LogSink extends Sink {
    private static final Logger logger = Logger.getLogger(LogSink.class);
    private static final String PREFIX = "prefix";
    private static final String PRIORITY = "priority";
    private String logPrefix;
    private LogPriority logPriority;

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{Object.class};
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    @Override
    protected StateFactory<State> init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                                       ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        String defaultPrefix = siddhiAppContext.getName() + " : " + outputStreamDefinition.getId();
        logPrefix = optionHolder.validateAndGetStaticValue(PREFIX, defaultPrefix);
        logPriority = LogPriority.valueOf(optionHolder.validateAndGetStaticValue(PRIORITY, "INFO")
                .toUpperCase());
        return null;
    }

    @Override
    public void publish(Object payload, DynamicOptions transportOptions, State s)
            throws ConnectionUnavailableException {
        String message;
        if (payload instanceof Object[]) {
            message = logPrefix + " : " + Arrays.deepToString((Object[]) payload);
        } else {
            message = logPrefix + " : " + payload;
        }
        logMessage(logPriority, message);
    }

    private void logMessage(LogSink.LogPriority logPriority, String message) {
        switch (logPriority) {
            case INFO:
                logger.info(message);
                break;
            case DEBUG:
                logger.debug(message);
                break;
            case WARN:
                logger.warn(message);
                break;
            case FATAL:
                logger.fatal(message);
                break;
            case ERROR:
                logger.error(message);
                break;
            case OFF:
                break;
            case TRACE:
                logger.trace(message);
                break;
        }
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        // do nothing
    }

    @Override
    public void disconnect() {
        // do nothing
    }

    @Override
    public void destroy() {
        // do nothing
    }

    private enum LogPriority {
        INFO,
        DEBUG,
        WARN,
        FATAL,
        ERROR,
        OFF,
        TRACE
    }
}
