/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.core.query.selector.attribute.processor.executor;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.QuerySelector;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GroupByAggregationAttributeExecutor extends AbstractAggregationAttributeExecutor {
    public static final int DEFAULT_AGGREGATOR_CLEAN_INTERVAL = 60;

    protected Map<String, AttributeAggregator> aggregatorMap = new HashMap<String, AttributeAggregator>();
    protected ExpiredAggregatorTracker expiredAggregatorTracker;

    private static Map<String, List<ExpiredAggregatorTracker>> allExpiredTrackers = new HashMap<String, List<ExpiredAggregatorTracker>>();
    private static ScheduledExecutorService aggregatorCleanTimer = null;


    public GroupByAggregationAttributeExecutor(AttributeAggregator attributeAggregator,
                                               ExpressionExecutor[] attributeExpressionExecutors,
                              ExecutionPlanContext executionPlanContext, String queryName) {
        super(attributeAggregator, attributeExpressionExecutors, executionPlanContext, queryName);
        if (executionPlanContext.getCleanAggregators()) {
            expiredAggregatorTracker = new ExpiredAggregatorTracker(executionPlanContext.getCleanAggregatorInterval());

            if (executionPlanContext.getCleanAggregatorInterval() == DEFAULT_AGGREGATOR_CLEAN_INTERVAL) {
                List<ExpiredAggregatorTracker> expiredTrackers = allExpiredTrackers.get(executionPlanContext.getName());
                if (expiredTrackers == null) {
                    expiredTrackers = new ArrayList<ExpiredAggregatorTracker>();
                    allExpiredTrackers.put(executionPlanContext.getName(), expiredTrackers);
                }
                expiredTrackers.add(expiredAggregatorTracker);

                if (aggregatorCleanTimer == null) {
                    createDefaultAggregatorCleanTimer();
                }
            } else {
                // Remove from default cleaning if it's re-deployed with custom interval    `
                allExpiredTrackers.remove(executionPlanContext.getName());
            }
        }
    }

    private void createDefaultAggregatorCleanTimer(){
        aggregatorCleanTimer = Executors.newSingleThreadScheduledExecutor();
        aggregatorCleanTimer.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for (List<ExpiredAggregatorTracker> trackers : allExpiredTrackers.values()) {
                    for (ExpiredAggregatorTracker tracker: trackers){
                        tracker.clear();
                    }
                }
            }
        }, DEFAULT_AGGREGATOR_CLEAN_INTERVAL, DEFAULT_AGGREGATOR_CLEAN_INTERVAL, TimeUnit.MINUTES);
    }

    @Override
    public synchronized Object execute(ComplexEvent event) {
        if (event.getType() == ComplexEvent.Type.RESET) {
            Object aOutput = null;
            for (AttributeAggregator attributeAggregator: aggregatorMap.values()) {
                aOutput = attributeAggregator.process(event);
            }

            if (expiredAggregatorTracker != null) {
                expiredAggregatorTracker.addAll(aggregatorMap);
                aggregatorMap.clear();
            }
            return aOutput;
        }

        String key = QuerySelector.getThreadLocalGroupByKey();
        AttributeAggregator currentAttributeAggregator = aggregatorMap.get(key);
        if (currentAttributeAggregator == null) {
            if (expiredAggregatorTracker != null) {
                currentAttributeAggregator = expiredAggregatorTracker.remove(key);
            }

            if (currentAttributeAggregator == null) {
                currentAttributeAggregator = attributeAggregator.cloneAggregator(key);
                currentAttributeAggregator.initAggregator(attributeExpressionExecutors, executionPlanContext);
            }
            currentAttributeAggregator.start();
            aggregatorMap.put(key, currentAttributeAggregator);
        }
        return currentAttributeAggregator.process(event);
    }

    public ExpressionExecutor cloneExecutor(String key) {
        return new GroupByAggregationAttributeExecutor(attributeAggregator.cloneAggregator(key), attributeExpressionExecutors, executionPlanContext, queryName);
    }


    @Override
    public Object[] currentState() {
        HashMap<String, Object[]> data = new HashMap<String, Object[]>();
        for (Map.Entry<String, AttributeAggregator> entry : aggregatorMap.entrySet()) {
            data.put(entry.getKey(), entry.getValue().currentState());
        }
        return new Object[]{new AbstractMap.SimpleEntry<String, Object>("Data", data)};
    }

    @Override
    public void restoreState(Object[] state) {
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        HashMap<String, Object[]> data = (HashMap<String, Object[]>) stateEntry.getValue();

        for (Map.Entry<String, Object[]> entry : data.entrySet()) {
            String key = entry.getKey();
            AttributeAggregator aAttributeAggregator = attributeAggregator.cloneAggregator(key);
            aAttributeAggregator.initAggregator(attributeExpressionExecutors, executionPlanContext);
            aAttributeAggregator.start();
            aAttributeAggregator.restoreState(entry.getValue());
            aggregatorMap.put(key, aAttributeAggregator);
        }
    }

    class ExpiredAggregatorTracker {
        private Map<String, AttributeAggregator> expiredAggregators = null;
        private ScheduledExecutorService service = null;
        private int cleanInterval;

        public ExpiredAggregatorTracker(int cleanInterval){
            this.cleanInterval = cleanInterval;
        }

        private void init() {
            expiredAggregators = new HashMap<String, AttributeAggregator>();
            if (cleanInterval != DEFAULT_AGGREGATOR_CLEAN_INTERVAL) {
                service = Executors.newSingleThreadScheduledExecutor();
                service.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        clear();
                    }
                }, cleanInterval, cleanInterval, TimeUnit.MINUTES);
            }
        }

        public synchronized void add(String key, AttributeAggregator aggregator) {
            if (expiredAggregators == null) {
                init();
            }
            expiredAggregators.put(key, aggregator);
        }

        public synchronized void addAll(Map<String, AttributeAggregator> aggregatorMap){
            if (expiredAggregators == null) {
                init();
            }
            expiredAggregators.putAll(aggregatorMap);
        }

        public synchronized AttributeAggregator remove(String key) {
            return  (expiredAggregators == null) ? null : expiredAggregators.remove(key);
        }

        public synchronized void clear() {
            if (expiredAggregators != null) {
                expiredAggregators.clear();
                expiredAggregators = null;
                service.shutdown();
                service = null;
            }
        }
    }
}
