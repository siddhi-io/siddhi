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
package org.wso2.siddhi.core.query.selector.attribute.aggregator;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.QuerySelector;
import org.wso2.siddhi.core.util.kvstore.KeyValueStoreClient;
import org.wso2.siddhi.core.util.kvstore.KeyValueStoreManager;
import org.wso2.siddhi.core.util.kvstore.KeyValueStoreException;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class CountAttributeAggregator extends AttributeAggregator {

    private static final Logger log = LoggerFactory.getLogger(CountAttributeAggregator.class);
    private static Attribute.Type type = Attribute.Type.LONG;
    private KeyValueStoreClient kvStoreClient;
    private String key;
    private final AtomicLong localCounter = new AtomicLong(0L);
    private final AtomicLong pendingDelta = new AtomicLong(0L);
    private static final ConcurrentHashMap<String, CountAttributeAggregator> ACTIVE_AGGREGATORS =
            new ConcurrentHashMap<>();

    //Distributed setup configs
    private static final boolean DISTRIBUTED_THROTTLE_ENABLED = Boolean.parseBoolean(
            System.getProperty("distributed.throttle.enabled"));
    private static final String KV_STORE_TYPE = System.getProperty("distributed.throttle.type");
    private static final int CORE_POOL_SIZE = getIntProperty("distributed.throttle.core.pool.size", 10);
    private static final int KV_STORE_SYNC_INTERVAL_MILLISECONDS = getIntProperty("distributed.throttle.sync.interval", 200);

    // Static shared scheduler for all aggregators
    private static final ScheduledExecutorService kvStoreSyncScheduler =
            Executors.newScheduledThreadPool(CORE_POOL_SIZE, r -> {
                Thread t = new Thread(r, "KVStore-Sync-Thread");
                t.setDaemon(true);
                return t;
            });

    // Start periodic sync task only if distributed throttling is enabled
    static {
        if (DISTRIBUTED_THROTTLE_ENABLED) {
            kvStoreSyncScheduler.scheduleAtFixedRate(() -> {
                for (CountAttributeAggregator aggregator : ACTIVE_AGGREGATORS.values()) {
                    aggregator.syncWithKVStore();
                }
            }, KV_STORE_SYNC_INTERVAL_MILLISECONDS, KV_STORE_SYNC_INTERVAL_MILLISECONDS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.key = QuerySelector.getThreadLocalGroupByKey();
        if (DISTRIBUTED_THROTTLE_ENABLED && this.key != null) {
            try {
                this.kvStoreClient = KeyValueStoreManager.getClient();
                if (this.kvStoreClient != null) {
                    initializeFromKVStore();
                    ACTIVE_AGGREGATORS.put(key, this);
                }
            } catch (KeyValueStoreException e) {
                log.error("Failed to initialize KeyValueStoreClient for aggregator with key '{}'. Reason: {}. Operating in fallback mode.",
                        key, e.getMessage(), e);
                this.kvStoreClient = null;
            } catch (Exception e) {
                log.error("Unexpected error initializing KeyValueStoreClient for aggregator with key '{}'. Operating in fallback mode.",
                        key, e);
                this.kvStoreClient = null;
            }
        }
    }

    private void initializeFromKVStore() {
        try {
            String kvStoreValue = kvStoreClient.get(key);
            if (kvStoreValue != null) {
                long initialValue = Long.parseLong(kvStoreValue);
                localCounter.set(initialValue);
            } else {
                localCounter.set(0L);
                kvStoreClient.set(key, "0");
            }
        } catch (Exception e) {
            log.error("Error initializing from kvStore for key '{}'. Starting with local value 0. Error: {}",
                    key, e.getMessage());
            localCounter.set(0L);
        }
    }

    private void syncWithKVStore() {
        if (kvStoreClient == null || key == null) {
            return;
        }
        long delta = pendingDelta.getAndSet(0L);
        if (delta == 0) {
            localCounter.set(Long.parseLong(kvStoreClient.get(key)));
            return;
        }
        try {
            if (delta > 0) {
                localCounter.set(kvStoreClient.incrementBy(key, delta));
            } else {
                localCounter.set(kvStoreClient.decrementBy(key, Math.abs(delta)));
            }
        } catch (KeyValueStoreException e) {
            log.error("Error syncing delta {} to kvStore for key '{}'. Will retry on next sync. Error: {}",
                    delta, key, e.getMessage());
            pendingDelta.addAndGet(delta);
        }
    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object data) {
        try {
            localCounter.incrementAndGet();
            if (DISTRIBUTED_THROTTLE_ENABLED && kvStoreClient != null && key != null) {
                pendingDelta.incrementAndGet();
            }
            return localCounter.get();
        } catch (Exception e) {
            log.error("Error in processAdd for key '{}'. Error: {}", key, e.getMessage());
            return localCounter.get();
        }
    }

    @Override
    public Object processAdd(Object[] data) {
        return processAdd((Object) data);
    }

    @Override
    public Object processRemove(Object data) {
        try {
            localCounter.decrementAndGet();
            if (DISTRIBUTED_THROTTLE_ENABLED && kvStoreClient != null && key != null) {
                pendingDelta.decrementAndGet();
            }
            return localCounter.get();

        } catch (Exception e) {
            log.error("Error in processRemove for key '{}'. Error: {}", key, e.getMessage());
            return localCounter.get();
        }
    }

    @Override
    public Object processRemove(Object[] data) {
        return processRemove((Object) data);
    }

    @Override
    public Object reset() {
        try {
            localCounter.set(0L);
            if (DISTRIBUTED_THROTTLE_ENABLED && kvStoreClient != null && key != null) {
                kvStoreClient.set(key, "0");
                pendingDelta.set(0L); // Clear pending changes
            }
            return 0L;

        } catch (KeyValueStoreException e) {
            log.error("Error resetting counter for key '{}'. Local counter reset successfully. Error: {}",
                    key, e.getMessage());
            return 0L;
        }
    }

    @Override
    public void start() {
        //Nothing to start
    }

    @Override
    public void stop() {
         try {
            // Only remove if key is not null and distributed throttling is enabled
            if (DISTRIBUTED_THROTTLE_ENABLED && key != null) {
                ACTIVE_AGGREGATORS.remove(key);
                if (kvStoreClient != null) {
                    syncWithKVStore();
                }
            }
        } catch (Exception e) {
            log.error("Error during stop for key '{}'. Error: {}", key, e.getMessage());
        }
    }

    @Override
    public Object[] currentState() {
        if (DISTRIBUTED_THROTTLE_ENABLED && kvStoreClient != null && key != null) {
            try {
                syncWithKVStore();
            } catch (Exception e) {
                log.warn("Could not sync with kvStore before returning state for key '{}'. Using local value. Error: {}",
                        key, e.getMessage());
            }
        }
        return new Object[]{new AbstractMap.SimpleEntry<String, Object>("Value", localCounter.get())};
    }

    @Override
    public void restoreState(Object[] state) {
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        long restoredValue = (Long) stateEntry.getValue();

        localCounter.set(restoredValue);
        pendingDelta.set(0L);

        if (DISTRIBUTED_THROTTLE_ENABLED && kvStoreClient != null && key != null) {
            try {
                kvStoreClient.set(key, String.valueOf(restoredValue));
            } catch (KeyValueStoreException e) {
                log.error("Error restoring state to kvStore for key '{}'. State restored to local counter only. Error: {}",
                        key, e.getMessage());
            }
        }
    }

    // Helper method to safely parse integer system properties
    private static int getIntProperty(String propertyName, int defaultValue) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue == null || propertyValue.trim().isEmpty()) {
            log.warn("System property '{}' is not set or empty. Using default value: {}", propertyName, defaultValue);
            return defaultValue;
        }
        try {
            return Integer.parseInt(propertyValue.trim());
        } catch (NumberFormatException e) {
            log.error("Invalid value '{}' for system property '{}'. Using default value: {}. Error: {}",
                    propertyValue, propertyName, defaultValue, e.getMessage());
            return defaultValue;
        }
    }
}
