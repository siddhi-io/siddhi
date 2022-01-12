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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.query.processor.stream;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Input attributes to log is (priority (String), log.message (String), is.event.logged (Bool))
 */
@Extension(
        name = "log",
        namespace = "",
        description = "Logs the message on the given priority with or without the processed event.",
        parameters = {
                @Parameter(name = "priority",
                        description = "The priority/type of this log message (INFO, DEBUG, WARN," +
                                " FATAL, ERROR, OFF, TRACE).",
                        type = {DataType.STRING},
                        defaultValue = "INFO", optional = true),
                @Parameter(name = "log.message",
                        description = "This message will be logged.",
                        defaultValue = "<siddhi app name> :", optional = true,
                        dynamic = true,
                        type = {DataType.STRING}),
                @Parameter(name = "is.event.logged",
                        description = "To log the processed event.",
                        type = {DataType.BOOL},
                        defaultValue = "true", optional = true)
        },
        parameterOverloads = {
                @ParameterOverload(),
                @ParameterOverload(parameterNames = {"log.message"}),
                @ParameterOverload(parameterNames = {"is.event.logged"}),
                @ParameterOverload(parameterNames = {"log.message", "is.event.logged"}),
                @ParameterOverload(parameterNames = {"priority", "log.message"}),
                @ParameterOverload(parameterNames = {"priority", "log.message", "is.event.logged"})
        },
        examples = {
                @Example(
                        syntax = "from FooStream#log()\n" +
                                "select *\n" +
                                "insert into BarStream;",
                        description = "Logs events with SiddhiApp name message prefix on default log level INFO."
                ),
                @Example(
                        syntax = "from FooStream#log(\"Sample Event :\")\n" +
                                "select *\n" +
                                "insert into BarStream;",
                        description = "Logs events with the message prefix \"Sample Event :\" on default " +
                                "log level INFO."
                ),
                @Example(
                        syntax = "from FooStream#log(\"DEBUG\", \"Sample Event :\", true)\n" +
                                "select *\n" +
                                "insert into BarStream;",
                        description = "Logs events with the message prefix \"Sample Event :\" on log level DEBUG."
                ),
                @Example(
                        syntax = "from FooStream#log(\"Event Arrived\", false)\n" +
                                "select *\n" +
                                "insert into BarStream;",
                        description = "For each event logs a message \"Event Arrived\" on default log level INFO."
                ),
                @Example(
                        syntax = "from FooStream#log(\"Sample Event :\", true)\n" +
                                "select *\n" +
                                "insert into BarStream;",
                        description = "Logs events with the message prefix \"Sample Event :\" on default " +
                                "log level INFO."
                ),
                @Example(
                        syntax = "from FooStream#log(true)\n" +
                                "select *\n" +
                                "insert into BarStream;",
                        description = "Logs events with on default log level INFO."
                )
        }
)
public class LogStreamProcessor extends StreamProcessor<State> {
    private static final Logger log = LogManager.getLogger(LogStreamProcessor.class);
    private ExpressionExecutor isLogEventExpressionExecutor = null;
    private ExpressionExecutor logMessageExpressionExecutor = null;
    private ExpressionExecutor logPriorityExpressionExecutor = null;
    private LogPriority logPriority = LogPriority.INFO;
    private String logPrefix;

    /**
     * The init method of the StreamFunction
     *
     * @param metaStreamEvent              the  stream event meta
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors for the function parameters
     * @param configReader                 this hold the {@link LogStreamProcessor} configuration reader.
     * @param streamEventClonerHolder      streamEventCloner Holder
     * @param findToBeExecuted             find will be executed
     * @param siddhiQueryContext           current siddhi query context
     * @return the additional output attributes introduced by the function
     */
    @Override
    protected StateFactory init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                StreamEventClonerHolder streamEventClonerHolder,
                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                SiddhiQueryContext siddhiQueryContext) {
        int inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength == 1) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                logMessageExpressionExecutor = attributeExpressionExecutors[0];
            } else if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.BOOL) {
                isLogEventExpressionExecutor = attributeExpressionExecutors[0];
            } else {
                throw new SiddhiAppValidationException("Input attribute is expected to be 'isEventLogged (Bool)' " +
                        "or 'logMessage (String)' or 'isEventLogged (Bool), logMessage (String)' or 'priority " +
                        "(String), isEventLogged (Bool), logMessage (String)', but its 1st attribute is " +
                        attributeExpressionExecutors[0].getReturnType());
            }
        } else if (inputExecutorLength == 2) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING &&
                    attributeExpressionExecutors[1].getReturnType() == Attribute.Type.BOOL) {
                logMessageExpressionExecutor = attributeExpressionExecutors[0];
                isLogEventExpressionExecutor = attributeExpressionExecutors[1];
            } else if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING &&
                    attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING) {
                if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                    logPriority = LogPriority.valueOf(((String) attributeExpressionExecutors[0].execute(null))
                            .toUpperCase());
                } else {
                    logPriorityExpressionExecutor = attributeExpressionExecutors[0];
                }
                logMessageExpressionExecutor = attributeExpressionExecutors[1];
            } else {
                throw new SiddhiAppValidationException("Input attribute is expected to be 'logMessage (String), " +
                        "isEventLogged (Bool)' or 'priority (String), logMessage (String)', but its returning are '"
                        + attributeExpressionExecutors[0].getReturnType() + ", " + attributeExpressionExecutors[1]
                        .getReturnType() + "'");
            }
        } else if (inputExecutorLength == 3) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                    logPriority = LogPriority.valueOf(((String) attributeExpressionExecutors[0].execute(null))
                            .toUpperCase());
                } else {
                    logPriorityExpressionExecutor = attributeExpressionExecutors[0];
                }
            } else {
                throw new SiddhiAppValidationException("Input attribute is expected to be 'priority (String), " +
                        "logMessage (String), isEventLogged (Bool)', but its 1st attribute is returning " +
                        attributeExpressionExecutors[0].getReturnType());
            }
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING) {
                logMessageExpressionExecutor = attributeExpressionExecutors[1];
            } else {
                throw new SiddhiAppValidationException("Input attribute is expected to be 'priority (String), " +
                        "logMessage (String), isEventLogged (Bool)', but its 2nd attribute is returning " +
                        attributeExpressionExecutors[1].getReturnType());
            }
            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.BOOL) {
                isLogEventExpressionExecutor = attributeExpressionExecutors[2];
            } else {
                throw new SiddhiAppValidationException("Input attribute is expected to be 'priority (String), " +
                        "logMessage (String), isEventLogged (Bool)', but its 3rd attribute is returning " +
                        attributeExpressionExecutors[2].getReturnType());
            }
        } else if (inputExecutorLength > 3) {
            throw new SiddhiAppValidationException("Input parameters for Log can be logMessage (String), " +
                    "isEventLogged (Bool), but there are " + attributeExpressionExecutors.length + " in the input!");
        }
        logPrefix = siddhiQueryContext.getSiddhiAppContext().getName() + ": ";
        return null;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return new ArrayList<Attribute>();
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            switch (attributeExpressionLength) {
                case 0:
                    log.info(logPrefix + streamEvent.toString(1));
                    break;
                case 1:
                    if (isLogEventExpressionExecutor != null) {
                        if ((Boolean) isLogEventExpressionExecutor.execute(streamEvent)) {
                            log.info(logPrefix + streamEvent.toString(1));
                        } else {
                            log.info(logPrefix + "Event Arrived");
                        }
                    } else {
                        log.info(logPrefix + logMessageExpressionExecutor.execute(streamEvent) + ", " +
                                streamEvent.toString(1));
                    }
                    break;
                case 2:
                    if (isLogEventExpressionExecutor != null) {
                        if ((Boolean) isLogEventExpressionExecutor.execute(streamEvent)) {
                            log.info(logPrefix + logMessageExpressionExecutor.execute(streamEvent) + ", " +
                                    streamEvent.toString(1));
                        } else {
                            log.info(logPrefix + logMessageExpressionExecutor.execute(streamEvent));
                        }
                    } else {
                        LogPriority tempLogPriority = logPriority;
                        if (logPriorityExpressionExecutor != null) {
                            tempLogPriority = LogPriority.valueOf((String) logPriorityExpressionExecutor.
                                    execute(streamEvent));
                        }
                        String message = logPrefix + logMessageExpressionExecutor.execute(streamEvent) + ", " +
                                streamEvent.toString(1);
                        logMessage(tempLogPriority, message);
                    }
                    break;
                default:
                    String message;
                    if ((Boolean) isLogEventExpressionExecutor.execute(streamEvent)) {
                        message = logPrefix + logMessageExpressionExecutor.execute(streamEvent) + ", " +
                                streamEvent.toString(1);
                    } else {
                        message = logPrefix + logMessageExpressionExecutor.execute(streamEvent);
                    }
                    LogPriority tempLogPriority = logPriority;
                    if (logPriorityExpressionExecutor != null) {
                        tempLogPriority = LogPriority.valueOf((String) logPriorityExpressionExecutor.
                                execute(streamEvent));
                    }
                    logMessage(tempLogPriority, message);
            }
        }
        nextProcessor.process(streamEventChunk);
    }


    private void logMessage(LogPriority logPriority, String message) {
        switch (logPriority) {
            case INFO:
                log.info(message);
                break;
            case DEBUG:
                log.debug(message);
                break;
            case WARN:
                log.warn(message);
                break;
            case FATAL:
                log.fatal(message);
                break;
            case ERROR:
                log.error(message);
                break;
            case OFF:
                break;
            case TRACE:
                log.trace(message);
                break;
        }
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    enum LogPriority {
        INFO,
        DEBUG,
        WARN,
        FATAL,
        ERROR,
        OFF,
        TRACE
    }


}
