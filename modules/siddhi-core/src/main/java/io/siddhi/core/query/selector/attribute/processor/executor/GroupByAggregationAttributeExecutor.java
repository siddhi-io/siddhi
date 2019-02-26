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
package io.siddhi.core.query.selector.attribute.processor.executor;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.timestamp.TimestampGenerator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Executor class for aggregations with group by configuration.
 */
public class GroupByAggregationAttributeExecutor extends AbstractAggregationAttributeExecutor {

    private static final ThreadLocal<String> keyThreadLocal = new ThreadLocal<String>();
    private final ConfigReader configReader;
    private final TimestampGenerator timestampGenerator;
    protected Map<String, AttributeAggregator> aggregatorMap = new HashMap<String, AttributeAggregator>();
    protected Set<String> obsoleteAggregatorKeys = new HashSet<>();
    protected long lastCleanupTimestamp = 0;

    public GroupByAggregationAttributeExecutor(AttributeAggregator attributeAggregator,
                                               ExpressionExecutor[] attributeExpressionExecutors,
                                               ConfigReader configReader, SiddhiQueryContext siddhiQueryContext) {
        super(attributeAggregator, attributeExpressionExecutors, siddhiQueryContext);
        this.configReader = configReader;
        timestampGenerator = this.siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator();
        lastCleanupTimestamp = timestampGenerator.currentTime();
    }

    public static ThreadLocal<String> getKeyThreadLocal() {
        return keyThreadLocal;
    }

    @Override
    public Object execute(ComplexEvent event) {

        long currentTime = timestampGenerator.currentTime();
        boolean canClean = false;
        if (lastCleanupTimestamp + 5000 < currentTime || obsoleteAggregatorKeys.size() > 25) {
            lastCleanupTimestamp = currentTime;
            canClean = true;
        }

        if (event.getType() == ComplexEvent.Type.RESET) {
            Object aOutput = null;
            if (canClean) {
                for (Map.Entry<String, AttributeAggregator> attributeAggregatorEntry : aggregatorMap.entrySet()) {
                    aOutput = attributeAggregatorEntry.getValue().process(event);
                    attributeAggregatorEntry.getValue().clean();
                }
                aggregatorMap.clear();
                obsoleteAggregatorKeys.clear();
            } else {
                for (Map.Entry<String, AttributeAggregator> attributeAggregatorEntry : aggregatorMap.entrySet()) {
                    aOutput = attributeAggregatorEntry.getValue().process(event);
                }
            }
            return aOutput;
        }

        String key = keyThreadLocal.get();
        AttributeAggregator currentAttributeAggregator = aggregatorMap.get(key);
        if (currentAttributeAggregator == null) {
            currentAttributeAggregator = attributeAggregator.cloneAggregator(key);
            aggregatorMap.put(key, currentAttributeAggregator);
        }
        Object results = currentAttributeAggregator.process(event);
        if (event.getType() == ComplexEvent.Type.EXPIRED && currentAttributeAggregator.canDestroy()) {
            obsoleteAggregatorKeys.add(key);
        }
        if (canClean) {
            destroyObsoleteAggregators();
        }
        return results;
    }

    public ExpressionExecutor cloneExecutor(String key) {
        return new GroupByAggregationAttributeExecutor(attributeAggregator.cloneAggregator(key),
                attributeExpressionExecutors, configReader,
                siddhiQueryContext);
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        for (Map.Entry<String, AttributeAggregator> entry : aggregatorMap.entrySet()) {
            state.put(entry.getKey(), entry.getValue().currentState());
        }
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        for (HashMap.Entry<String, Object> item : state.entrySet()) {
            String key = item.getKey();
            AttributeAggregator aAttributeAggregator = attributeAggregator.cloneAggregator(key);
            aAttributeAggregator.restoreState((Map<String, Object>) item.getValue());
            aggregatorMap.put(key, aAttributeAggregator);
        }
    }

    private void destroyObsoleteAggregators() {
        for (String obsoleteKey : obsoleteAggregatorKeys) {
            AttributeAggregator attributeAggregator = aggregatorMap.get(obsoleteKey);
            if (attributeAggregator != null && attributeAggregator.canDestroy()) {
                attributeAggregator.clean();
                aggregatorMap.remove(obsoleteKey);
            }
        }
        obsoleteAggregatorKeys.clear();
    }
}
