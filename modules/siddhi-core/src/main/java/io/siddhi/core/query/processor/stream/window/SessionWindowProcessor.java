/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.SessionContainer;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link WindowProcessor} which represent a Window operating based on a session.
 */
@Extension(
        name = "session",
        namespace = "",
        description = "Holds events that belong to a session. Events belong to a specific session are " +
                "identified by a session key, and a session gap is determines the time period after which " +
                "the session is considered to be expired. " +
                "To have meaningful aggregation on session windows, the events need to be aggregated based on " +
                "session key via a `group by` clause.",
        parameters = {
                @Parameter(name = "session.gap",
                        description = "The time period after which the session is considered to be expired.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME}),
                @Parameter(name = "session.key",
                        description = "The session identification attribute. Used to group events belonging to a " +
                                "specific session.",
                        type = {DataType.STRING}, optional = true, defaultValue = "default-key", dynamic = true),
                @Parameter(name = "allowed.latency",
                        description = "The time period for which the session window is valid after " +
                                "the expiration of the session, to accept late event arrivals. " +
                                "This time period should be less than " +
                                "the `session.gap` parameter.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME}, optional = true, defaultValue = "0")
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"session.gap"}),
                @ParameterOverload(parameterNames = {"session.gap", "session.key"}),
                @ParameterOverload(parameterNames = {"session.gap", "session.key", "allowed.latency"})

        },
        examples = {
                @Example(
                        syntax = "define stream PurchaseEventStream "
                                + "(user string, item_number int, price float, quantity int);\n"
                                + "\n"
                                + "@info(name='query1) \n"
                                + "from PurchaseEventStream#window.session(5 sec, user) \n"
                                + "select user, sum(quantity) as totalQuantity, sum(price) as totalPrice \n"
                                + "group by user \n"
                                + "insert into OutputStream;",
                        description = "From the events arriving at the PurchaseEventStream, " +
                                "a session window with 5 seconds session gap is processed based on 'user' attribute " +
                                "as the session group identification key. All events falling into the same session " +
                                "are aggregated based on `user` attribute, and outputted to the OutputStream."
                ),
                @Example(
                        syntax = "define stream PurchaseEventStream "
                                + "(user string, item_number int, price float, quantity int);\n"
                                + "\n"
                                + "@info(name='query2) \n"
                                + "from PurchaseEventStream#window.session(5 sec, user, 2 sec) \n"
                                + "select user, sum(quantity) as totalQuantity, sum(price) as totalPrice \n"
                                + "group by user \n"
                                + "insert into OutputStream;",
                        description = "From the events arriving at the PurchaseEventStream, " +
                                "a session window with 5 seconds session gap is processed based on 'user' attribute " +
                                "as the session group identification key. This session window is kept active for " +
                                "2 seconds after the session expiry to capture late (out of order) event arrivals. " +
                                "If the event timestamp falls in to the last session the session is reactivated. " +
                                "Then all events falling into the same session " +
                                "are aggregated based on `user` attribute, and outputted to the OutputStream."
                )
        }
)
public class SessionWindowProcessor extends GroupingFindableWindowProcessor<SessionWindowProcessor.WindowState>
        implements SchedulingProcessor {

    private static final Logger log = LogManager.getLogger(SessionWindowProcessor.class);
    private static final String DEFAULT_KEY = "default-key";
    private long sessionGap = 0;
    private long allowedLatency = 0;
    private VariableExpressionExecutor sessionKeyExecutor;
    private Scheduler scheduler;

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    protected StateFactory<WindowState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                             ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                             SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length >= 1 && attributeExpressionExecutors.length <= 3) {

            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT ||
                        attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                    sessionGap = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
                } else {
                    throw new SiddhiAppValidationException("Session window's session gap parameter should be either "
                            + "int or long, but found " + attributeExpressionExecutors[0].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Session window's 1st parameter, session gap"
                        + " should be a constant parameter attribute but "
                        + "found a dynamic attribute " + attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors.length == 3) {
                if (attributeExpressionExecutors[1] instanceof VariableExpressionExecutor) {
                    if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING) {
                        sessionKeyExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[1];
                    } else {
                        throw new SiddhiAppValidationException("Session window's session key parameter type"
                                + " should be string, but found " + attributeExpressionExecutors[1].getReturnType());
                    }
                } else {
                    throw new SiddhiAppValidationException("Session window's 2nd parameter, session key"
                            + " should be a dynamic parameter attribute but "
                            + "found a constant attribute "
                            + attributeExpressionExecutors[1].getClass().getCanonicalName());
                }

                if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT ||
                            attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                        allowedLatency = (Long) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[2]).getValue();
                        validateAllowedLatency(allowedLatency, sessionGap);
                    } else {
                        throw new SiddhiAppValidationException("Session window's allowedLatency parameter should be "
                                + "either int or long, but found " + attributeExpressionExecutors[2].getReturnType());
                    }
                } else {
                    throw new SiddhiAppValidationException("Session window's 3rd parameter, allowedLatency"
                            + " should be a constant parameter attribute but "
                            + "found a dynamic attribute "
                            + attributeExpressionExecutors[2].getClass().getCanonicalName());
                }
            }

            if (attributeExpressionExecutors.length == 2) {
                if (attributeExpressionExecutors[1] instanceof VariableExpressionExecutor) {
                    if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING) {
                        sessionKeyExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[1];
                    } else {
                        throw new SiddhiAppValidationException("Session window's session key parameter type"
                                + " should be string, but found " + attributeExpressionExecutors[1].getReturnType());
                    }
                } else {
                    if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT ||
                            attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                        allowedLatency = (Long) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[1]).getValue();
                        validateAllowedLatency(allowedLatency, sessionGap);
                    } else {
                        throw new SiddhiAppValidationException("Session window's allowedLatency parameter should be "
                                + "either int or long, but found " + attributeExpressionExecutors[1].getReturnType());
                    }
                }
            }
        } else {
            throw new SiddhiAppValidationException("Session window should only have one to three parameters "
                    + "(<int|long|time> sessionGap, <String> sessionKey, <int|long|time> allowedLatency, "
                    + "but found " + attributeExpressionExecutors.length + " input attributes");

        }

        return () -> new WindowState();
    }

    @Override
    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner, GroupingKeyPopulator groupingKeyPopulater,
                                     WindowState state) {

        String key = DEFAULT_KEY;
        SessionComplexEventChunk<StreamEvent> currentSession;

        synchronized (state) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                long eventTimestamp = streamEvent.getTimestamp();
                long maxTimestamp = eventTimestamp + sessionGap;
                long aliveTimestamp = maxTimestamp + allowedLatency;

                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {

                    if (sessionKeyExecutor != null) {
                        key = (String) sessionKeyExecutor.execute(streamEvent);
                    }

                    //Update event with the grouping key
                    groupingKeyPopulater.populateComplexEvent(streamEvent, key);


                    //get the session configuration based on session key
                    //if the map doesn't contain key, then a new sessionContainer
                    //object needs to be created.
                    if ((state.sessionContainer = state.sessionMap.get(key)) == null) {
                        state.sessionContainer = new SessionContainer(key);
                    }
                    state.sessionMap.put(key, state.sessionContainer);

                    StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    clonedStreamEvent.setType(StreamEvent.Type.EXPIRED);

                    currentSession = state.sessionContainer.getCurrentSession();

                    //if current session is empty
                    if (state.sessionContainer.getCurrentSession().getFirst() == null) {
                        currentSession.add(clonedStreamEvent);
                        currentSession.setTimestamps(eventTimestamp, maxTimestamp, aliveTimestamp);
                        scheduler.notifyAt(maxTimestamp);
                    } else {
                        if (eventTimestamp >= currentSession.getStartTimestamp()) {
                            //check whether the event belongs to the same session
                            if (eventTimestamp <= currentSession.getEndTimestamp()) {
                                currentSession.setTimestamps(currentSession.getStartTimestamp(),
                                        maxTimestamp, aliveTimestamp);
                                currentSession.add(clonedStreamEvent);
                                scheduler.notifyAt(maxTimestamp);
                            } else {
                                //when a new session starts
                                if (allowedLatency > 0) {
                                    moveCurrentSessionToPreviousSession(state);
                                    currentSession.clear();
                                    currentSession.setTimestamps(eventTimestamp, maxTimestamp, aliveTimestamp);
                                    currentSession.add(clonedStreamEvent);
                                    scheduler.notifyAt(maxTimestamp);
                                }
                            }

                        } else {
                            //when a late event arrives
                            addLateEvent(streamEventChunk, eventTimestamp, clonedStreamEvent, state);
                        }
                    }
                } else {
                    currentSessionTimeout(eventTimestamp, state);
                    if (allowedLatency > 0) {
                        previousSessionTimeout(eventTimestamp, state);
                    }
                }
            }
        }

        if (state.expiredEventChunk != null && state.expiredEventChunk.getFirst() != null) {
            streamEventChunk.add((StreamEvent) state.expiredEventChunk.getFirst());
            state.expiredEventChunk.clear();
        }

        nextProcessor.process(streamEventChunk);

    }


    private void validateAllowedLatency(long allowedLatency, long sessionGap) {
        if (allowedLatency > sessionGap) {
            throw new SiddhiAppValidationException("Session window's allowedLatency parameter value "
                    + "should not be greater than the session gap parameter value");

        }
    }

    /**
     * Merge previous window with the next window.
     */
    private void mergeWindows(SessionComplexEventChunk<StreamEvent> previousWindow,
                              SessionComplexEventChunk<StreamEvent> nextWindow) {
        //merge with the next window
        if (previousWindow.getFirst() != null &&
                previousWindow.getEndTimestamp() >= (nextWindow.getStartTimestamp() - sessionGap)) {

            if (nextWindow.hasNext()) {
                nextWindow.next();
            }

            nextWindow.insertBeforeCurrent(previousWindow.getFirst());
            nextWindow.setStartTimestamp(previousWindow.getStartTimestamp());
            previousWindow.clear();
        }
    }

    /**
     * Moves the events in the current session into previous window.
     *
     * @param state
     */
    private void moveCurrentSessionToPreviousSession(WindowState state) {

        SessionComplexEventChunk<StreamEvent> currentSession = state.sessionContainer.getCurrentSession();
        SessionComplexEventChunk<StreamEvent> previousSession = state.sessionContainer.getPreviousSession();

        if (previousSession.getFirst() == null) {
            previousSession.add(currentSession.getFirst());

        } else {
            state.expiredEventChunk.setKey(previousSession.getKey());
            state.expiredEventChunk.setTimestamps(previousSession.getStartTimestamp(),
                    previousSession.getEndTimestamp(), previousSession.getAliveTimestamp());
            state.expiredEventChunk.add(previousSession.getFirst());

            previousSession.clear();
            previousSession.add(currentSession.getFirst());

        }
        previousSession.setTimestamps(currentSession.getStartTimestamp(),
                currentSession.getEndTimestamp(),
                currentSession.getAliveTimestamp());
        scheduler.notifyAt(currentSession.getAliveTimestamp());

    }

    /**
     * Handles when the late event arrives to the system.
     */
    private void addLateEvent(ComplexEventChunk<StreamEvent> streamEventChunk,
                              long eventTimestamp, StreamEvent streamEvent, WindowState state) {

        SessionComplexEventChunk<StreamEvent> currentSession = state.sessionContainer.getCurrentSession();
        SessionComplexEventChunk<StreamEvent> previousSession = state.sessionContainer.getPreviousSession();

        if (allowedLatency > 0) {

            if (eventTimestamp >= (currentSession.getStartTimestamp() - sessionGap)) {
                if (currentSession.hasNext()) {
                    currentSession.next();
                }
                currentSession.insertBeforeCurrent(streamEvent);
                currentSession.setStartTimestamp(eventTimestamp);
                mergeWindows(previousSession, currentSession);
            } else {

                if (previousSession.getFirst() == null &&
                        eventTimestamp < (currentSession.getStartTimestamp() - sessionGap)) {
                    streamEventChunk.remove();
                    log.info("The event, " + streamEvent + " is late and it's session window has been timeout");

                } else {
                    if (eventTimestamp >= (previousSession.getStartTimestamp() - sessionGap)) {
                        previousSession.add(streamEvent);
                        //when this condition true, previous session will not merge with the current session
                        if (eventTimestamp <= (previousSession.getEndTimestamp() - sessionGap) &&
                                eventTimestamp < previousSession.getStartTimestamp()) {

                            previousSession.setStartTimestamp(eventTimestamp);

                        } else {
                            previousSession.setEndTimestamp(eventTimestamp + sessionGap);
                            previousSession.setAliveTimestamp(eventTimestamp + sessionGap + allowedLatency);
                            mergeWindows(previousSession, currentSession);
                        }

                    } else {
                        //late event does not belong to the previous session
                        streamEventChunk.remove();
                        log.info("The event, " + streamEvent + " is late and it's session window has been timeout");
                    }
                }
            }
        } else {
            //no allowedLatency time
            //check the late event belongs to the same session
            if (eventTimestamp >= (currentSession.getStartTimestamp() - sessionGap)) {
                if (currentSession.hasNext()) {
                    currentSession.next();
                }
                currentSession.insertBeforeCurrent(streamEvent);
                currentSession.setStartTimestamp(eventTimestamp);
            } else {
                streamEventChunk.remove();
                log.info("The event, " + streamEvent + " is late and it's session window has been timeout");
            }
        }

    }

    /**
     * Checks all the sessions and get the expired session.
     */
    private void currentSessionTimeout(long eventTimestamp, WindowState state) {
        Map<String, Long> currentEndTimestamps = findAllCurrentEndTimestamps(state);

        //sort on endTimestamps
        if (currentEndTimestamps.size() > 1) {
            List<Map.Entry<String, Long>> toSort = new ArrayList<>();
            toSort.addAll(currentEndTimestamps.entrySet());
            toSort.sort(Map.Entry.comparingByValue());
            Map<String, Long> map = new LinkedHashMap<>();
            for (Map.Entry<String, Long> e : toSort) {
                map.putIfAbsent(e.getKey(), e.getValue());
            }
            currentEndTimestamps = map;
        }

        for (Map.Entry<String, Long> entry : currentEndTimestamps.entrySet()) {
            long sessionEndTime = entry.getValue();
            SessionComplexEventChunk<StreamEvent> currentSession = state.sessionMap.get(entry.getKey())
                    .getCurrentSession();
            SessionComplexEventChunk<StreamEvent> previousSession = state.sessionMap.get(entry.getKey())
                    .getPreviousSession();
            if (currentSession.getFirst() != null && eventTimestamp >= sessionEndTime) {

                if (allowedLatency > 0) {
                    //move current session to previous session
                    previousSession.add(currentSession.getFirst());
                    previousSession.setTimestamps(currentSession.getStartTimestamp(),
                            currentSession.getEndTimestamp(),
                            currentSession.getAliveTimestamp());
                    scheduler.notifyAt(currentSession.getAliveTimestamp());
                    currentSession.clear();
                } else {
                    state.expiredEventChunk.setKey(currentSession.getKey());
                    state.expiredEventChunk.setTimestamps(currentSession.getStartTimestamp(),
                            currentSession.getEndTimestamp(),
                            currentSession.getAliveTimestamp());
                    state.expiredEventChunk.add(currentSession.getFirst());
                    currentSession.clear();
                }
            } else {
                break;
            }
        }
    }

    /**
     * Checks all the previous sessions and get the expired sessions.
     */
    private void previousSessionTimeout(long eventTimestamp, WindowState state) {

        Map<String, Long> previousEndTimestamps = findAllPreviousEndTimestamps(state);
        SessionComplexEventChunk<StreamEvent> previousSession;

        //sort on endTimestamps
        if (previousEndTimestamps.size() > 1) {
            List<Map.Entry<String, Long>> toSort = new ArrayList<>();
            for (Map.Entry<String, Long> e : previousEndTimestamps.entrySet()) {
                toSort.add(e);
            }
            toSort.sort(Map.Entry.comparingByValue());
            LinkedHashMap<String, Long> map = new LinkedHashMap<>();
            for (Map.Entry<String, Long> e : toSort) {
                map.putIfAbsent(e.getKey(), e.getValue());
            }
            previousEndTimestamps = map;
        }

        for (Map.Entry<String, Long> entry : previousEndTimestamps.entrySet()) {
            previousSession = state.sessionMap.get(entry.getKey()).getPreviousSession();

            if (previousSession != null && previousSession.getFirst() != null &&
                    eventTimestamp >= previousSession.getAliveTimestamp()) {

                state.expiredEventChunk.setKey(previousSession.getKey());
                state.expiredEventChunk.setTimestamps(previousSession.getStartTimestamp(),
                        previousSession.getEndTimestamp(), previousSession.getAliveTimestamp());

                state.expiredEventChunk.add(previousSession.getFirst());
                previousSession.clear();
            } else {
                break;
            }

        }

    }

    /**
     * Gets all end timestamps of current sessions.
     *
     * @param state current state
     * @return map with the values of each current session's end timestamp and with the key as the session key
     */
    private Map<String, Long> findAllCurrentEndTimestamps(WindowState state) {

        Collection<SessionContainer> sessionContainerList = state.sessionMap.values();

        if (!state.sessionKeyEndTimeMap.isEmpty()) {
            state.sessionKeyEndTimeMap.clear();
        }

        for (SessionContainer sessionContainer : sessionContainerList) {
            //not getting empty session details
            if (sessionContainer.getCurrentSessionEndTimestamp() != -1) {
                state.sessionKeyEndTimeMap.put(sessionContainer.getKey(),
                        sessionContainer.getCurrentSessionEndTimestamp());
            }
        }

        return state.sessionKeyEndTimeMap;
    }

    /**
     * Gets all the end timestamps of previous sessions.
     *
     * @param state
     * @return map with the values of each previous session's end timestamp and with the key as the sesssio key
     */
    private Map<String, Long> findAllPreviousEndTimestamps(WindowState state) {

        Collection<SessionContainer> sessionContainerList = state.sessionMap.values();

        if (!state.sessionKeyEndTimeMap.isEmpty()) {
            state.sessionKeyEndTimeMap.clear();
        }

        for (SessionContainer sessionContainer : sessionContainerList) {
            //not getting empty session details
            if (sessionContainer.getPreviousSessionEndTimestamp() != -1) {
                state.sessionKeyEndTimeMap.put(sessionContainer.getKey(),
                        sessionContainer.getPreviousSessionEndTimestamp());
            }
        }

        return state.sessionKeyEndTimeMap;
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, WindowState state,
                                              SiddhiQueryContext siddhiQueryContext) {
        return OperatorParser.constructOperator(state.expiredEventChunk, condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            StreamEventCloner streamEventCloner, WindowState state) {
        return ((Operator) compiledCondition).find(matchingEvent, state.expiredEventChunk, streamEventCloner);
    }

    /**
     * Collection used to manage session windows.
     *
     * @param <E> sub types of ComplexEvent such as StreamEvent and StateEvent.
     */
    public static class SessionComplexEventChunk<E extends ComplexEvent> extends ComplexEventChunk {

        private String key;
        private long startTimestamp;
        private long endTimestamp;
        private long aliveTimestamp;

        public SessionComplexEventChunk(String key) {
            this.key = key;
        }

        public SessionComplexEventChunk() {
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public long getEndTimestamp() {
            return endTimestamp;
        }

        public void setEndTimestamp(long endTimestamp) {
            this.endTimestamp = endTimestamp;
        }

        public long getAliveTimestamp() {
            return aliveTimestamp;
        }

        public void setAliveTimestamp(long aliveTimestamp) {
            this.aliveTimestamp = aliveTimestamp;
        }

        public void setTimestamps(long startTimestamp, long endTimestamp, long aliveTimestamp) {
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
            this.aliveTimestamp = aliveTimestamp;
        }
    }

    class WindowState extends State {

        private Map<String, SessionContainer> sessionMap;
        private Map<String, Long> sessionKeyEndTimeMap;
        private SessionContainer sessionContainer;
        private SessionComplexEventChunk<StreamEvent> expiredEventChunk;

        public WindowState() {
            this.sessionMap = new ConcurrentHashMap<>();
            this.sessionKeyEndTimeMap = new HashMap<>();
            this.sessionContainer = new SessionContainer();
            this.expiredEventChunk = new SessionComplexEventChunk<>();
        }

        @Override
        public boolean canDestroy() {
            return sessionMap.isEmpty() && expiredEventChunk.getFirst() == null
                    && sessionContainer.getCurrentSession().getFirst() == null
                    && sessionContainer.getPreviousSession().getFirst() == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("sessionMap", sessionMap);
            state.put("sessionContainer", sessionContainer);
            state.put("expiredEventChunk", expiredEventChunk);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            sessionMap = (ConcurrentHashMap<String, SessionContainer>) state.get("sessionMap");
            sessionContainer = (SessionContainer) state.get("sessionContainer");
            expiredEventChunk = (SessionComplexEventChunk<StreamEvent>) state.get("expiredEventChunk");
        }
    }
}
