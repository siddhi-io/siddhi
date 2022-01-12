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
package io.siddhi.core.query.processor.stream.window;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.SnapshotStateList;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Window operating based on a cron expression.
 */
@Extension(
        name = "cron",
        namespace = "",
        description = "This window outputs the arriving events as and when they arrive, and resets (expires) " +
                "the window periodically based on the given cron expression.",
        parameters = {
                @Parameter(name = "cron.expression",
                        description = "The cron expression that resets the window.",
                        type = {DataType.STRING})
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"cron.expression"})
        },
        examples = {
                @Example(
                        syntax = "define stream InputEventStream (symbol string, price float, volume int);\n\n" +
                                "@info(name = 'query1')\n" +
                                "from InputEventStream#cron('*/5 * * * * ?')\n" +
                                "select symbol, sum(price) as totalPrice \n" +
                                "insert into OutputStream;",
                        description = "This let the totalPrice to gradually increase and resets to zero " +
                                "as a batch every 5 seconds."
                ),
                @Example(
                        syntax = "define stream StockEventStream (symbol string, price float, volume int)\n" +
                                "define window StockEventWindow (symbol string, price float, volume int) " +
                                "cron('*/5 * * * * ?');\n\n" +
                                "@info(name = 'query0')\n" +
                                "from StockEventStream\n" +
                                "insert into StockEventWindow;\n\n" +
                                "@info(name = 'query1')\n" +
                                "from StockEventWindow \n" +
                                "select symbol, sum(price) as totalPrice\n" +
                                "insert into OutputStream ;",
                        description = "The defined window will let the totalPrice to gradually increase and " +
                                "resets to zero as a batch every 5 seconds."
                )
        }
)
//todo fix support find and optimize data storage
public class CronWindowProcessor extends BatchingWindowProcessor<CronWindowProcessor.WindowState> implements Job {
    private static final Logger log = LogManager.getLogger(CronWindowProcessor.class);
    private final String jobGroup = "CronWindowGroup";
    private Scheduler scheduler;
    private String jobName;
    private String cronString;
    private StreamEventClonerHolder streamEventClonerHolder;
    private String id;

    @Override
    protected StateFactory<WindowState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                             ConfigReader configReader,
                                             StreamEventClonerHolder streamEventClonerHolder,
                                             boolean outputExpectsExpiredEvents,
                                             boolean findToBeExecuted,
                                             SiddhiQueryContext siddhiQueryContext) {
        this.streamEventClonerHolder = streamEventClonerHolder;
        this.id = siddhiQueryContext.getName() + "_" + siddhiQueryContext.generateNewId();
        if (attributeExpressionExecutors != null) {
            cronString = (String) (((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue());
        }
        return () -> new WindowState(streamEventClonerHolder);
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState state) {
        synchronized (state) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                state.currentEventQueue.add(clonedStreamEvent);
                streamEventChunk.remove();
            }
        }
    }

    @Override
    public synchronized void start() {
        if (scheduler == null) {
            scheduleCronJob(cronString);
        }
    }

    @Override
    public void stop() {
        try {
            if (scheduler != null) {
                scheduler.deleteJob(new JobKey(jobName, jobGroup));
            }
        } catch (SchedulerException e) {
            log.error(ExceptionUtil.getMessageWithContext(e, siddhiQueryContext.getSiddhiAppContext()) +
                    " Error while removing the cron job '" + jobGroup + ":'" + jobName + "'.", e);
        }
    }

    private void scheduleCronJob(String cronString) {
        try {
            SchedulerFactory schedFact = new StdSchedulerFactory();
            scheduler = schedFact.getScheduler();
            jobName = siddhiQueryContext.getName() + "_EventRemoverJob_" + siddhiQueryContext.generateNewId();
            JobKey jobKey = new JobKey(jobName, jobGroup);

            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
            }
            scheduler.start();
            JobDataMap dataMap = new JobDataMap();
            dataMap.put("windowProcessor", this);

            JobDetail job = org.quartz.JobBuilder.newJob(CronWindowProcessor.class)
                    .withIdentity(jobName, jobGroup)
                    .usingJobData(dataMap)
                    .build();

            Trigger trigger = org.quartz.TriggerBuilder.newTrigger()
                    .withIdentity("EventRemoverTrigger_" + id, jobGroup)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cronString))
                    .build();

            scheduler.scheduleJob(job, trigger);

        } catch (SchedulerException e) {
            log.error("Error while instantiating quartz scheduler", e);
        }
    }

    public void dispatchEvents() {
        Map<String, Map<String, WindowState>> allStates = stateHolder.getAllStates();
        try {
            for (Map.Entry<String, Map<String, WindowState>> allStatesEntry : allStates.entrySet()) {
                for (Map.Entry<String, WindowState> stateEntry : allStatesEntry.getValue().entrySet()) {
                    WindowState windowState = stateEntry.getValue();
                    ComplexEventChunk<StreamEvent> streamEventChunk = new ComplexEventChunk<StreamEvent>();
                    synchronized (windowState) {
                        if (windowState.currentEventQueue.getFirst() != null) {
                            long currentTime = siddhiQueryContext.getSiddhiAppContext().
                                    getTimestampGenerator().currentTime();
                            while (windowState.expiredEventQueue.hasNext()) {
                                StreamEvent expiredEvent = windowState.expiredEventQueue.next();
                                expiredEvent.setTimestamp(currentTime);
                            }
                            if (windowState.expiredEventQueue.getFirst() != null) {
                                streamEventChunk.add(windowState.expiredEventQueue.getFirst());
                            }
                            windowState.expiredEventQueue.clear();
                            while (windowState.currentEventQueue.hasNext()) {
                                StreamEvent currentEvent = windowState.currentEventQueue.next();
                                StreamEvent toExpireEvent =
                                        streamEventClonerHolder.getStreamEventCloner().copyStreamEvent(currentEvent);
                                toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                                windowState.expiredEventQueue.add(toExpireEvent);
                            }

                            streamEventChunk.add(windowState.currentEventQueue.getFirst());
                            windowState.currentEventQueue.clear();
                        }
                    }
                    SiddhiAppContext.startPartitionFlow(allStatesEntry.getKey());
                    SiddhiAppContext.startGroupByFlow(stateEntry.getKey());
                    try {
                        if (streamEventChunk.getFirst() != null) {
                            nextProcessor.process(streamEventChunk);
                        }
                    } finally {
                        SiddhiAppContext.stopGroupByFlow();
                        SiddhiAppContext.stopPartitionFlow();
                    }
                }
            }
        } finally {
            stateHolder.returnAllStates(allStates);
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        if (log.isDebugEnabled()) {
            log.debug("Running Event Remover Job");
        }

        JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        CronWindowProcessor windowProcessor = (CronWindowProcessor) dataMap.get("windowProcessor");
        windowProcessor.dispatchEvents();

    }

    class WindowState extends State {
        private SnapshotableStreamEventQueue currentEventQueue;
        private SnapshotableStreamEventQueue expiredEventQueue;

        WindowState(StreamEventClonerHolder streamEventClonerHolder) {
            currentEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
            expiredEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("CurrentEventQueue", currentEventQueue.getSnapshot());
            state.put("ExpiredEventQueue", expiredEventQueue.getSnapshot());
            return state;
        }

        public void restore(Map<String, Object> state) {
            currentEventQueue.restore((SnapshotStateList) state.get("CurrentEventQueue"));
            expiredEventQueue.restore((SnapshotStateList) state.get("ExpiredEventQueue"));
        }

        @Override
        public boolean canDestroy() {
            return expiredEventQueue.getFirst() == null && currentEventQueue.getFirst() == null;
        }
    }
}
