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
    private String kvStoreType;
    private String key;
    private final AtomicLong localCounter = new AtomicLong(0L);
    private final AtomicLong pendingDelta = new AtomicLong(0L);

    // Static shared scheduler for all aggregators
    private static final ScheduledExecutorService redisSyncScheduler =
            Executors.newScheduledThreadPool(2000, r -> {
                Thread t = new Thread(r, "Redis-Sync-Thread");
                t.setDaemon(true);
                return t;
            });

    // Track all active aggregators for periodic sync
    private static final ConcurrentHashMap<String, CountAttributeAggregator> activeAggregators =
            new ConcurrentHashMap<>();

    // Sync interval in seconds
    private static final int REDIS_SYNC_INTERVAL_MILLISECONDS = 1;

    static {
        // Start periodic Redis sync task
        redisSyncScheduler.scheduleAtFixedRate(() -> {
            for (CountAttributeAggregator aggregator : activeAggregators.values()) {
                aggregator.syncWithRedis();
                log.error("Redis sync scheduler started with interval: {} seconds", REDIS_SYNC_INTERVAL_MILLISECONDS);
            }
        }, REDIS_SYNC_INTERVAL_MILLISECONDS, REDIS_SYNC_INTERVAL_MILLISECONDS, TimeUnit.SECONDS);

        log.info("Redis sync scheduler started with interval: {} seconds", REDIS_SYNC_INTERVAL_MILLISECONDS);
    }

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        //        this.key = QuerySelector.getThreadLocalGroupByKey();

        this.kvStoreType = System.getProperty(
                KeyValueStoreManager.KEYVALUE_STORE_TYPE_PROPERTY,
                KeyValueStoreManager.DEFAULT_KV_STORE_TYPE
        ).toLowerCase();

        try {
            this.kvStoreClient = KeyValueStoreManager.getClient();
            if (this.kvStoreClient != null) {
                log.info("KeyValueStoreClient of type '{}' initialized and connected for aggregator with key '{}'.",
                        kvStoreType, key);
            } else {
                log.warn("KeyValueStoreClient obtained for type '{}', but isConnected() is false for key '{}'. Aggregator will use fallback.",
                        kvStoreType, key);
                this.kvStoreClient = null;
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

    // Add a lazy initialization method
    private void ensureInitialized() {
        if (this.key == null) {
            this.key = QuerySelector.getThreadLocalGroupByKey();

            if (this.key != null) {
                log.info("Lazy initialization for key '{}'", this.key);

                if (kvStoreClient != null) {
                    initializeFromRedis();
                    activeAggregators.put(key, this);
                }
            } else {
                log.warn("Key is still null during lazy initialization. Query context may not be properly set.");
            }
        }
    }

    private void initializeFromRedis() {
        if (kvStoreClient != null) {
            try {
                String redisValue = kvStoreClient.get(key);
                if (redisValue != null) {
                    long initialValue = Long.parseLong(redisValue);
                    localCounter.set(initialValue);
                    log.info("Initialized local counter for key '{}' from Redis value: {}", key, initialValue);
                } else {
                    // Key doesn't exist in Redis, start with 0
                    localCounter.set(0L);
                    kvStoreClient.set(key, "0");
                    log.info("Key '{}' not found in Redis. Initialized with value: 0", key);
                }
            } catch (Exception e) {
                log.error("Error initializing from Redis for key '{}'. Starting with local value 0. Error: {}",
                        key, e.getMessage());
                localCounter.set(0L);
            }
        }
    }

    private void syncWithRedis() {
        if (kvStoreClient == null || key == null) {
            return;
        }
        // Get and reset pending delta atomically
        long delta = pendingDelta.getAndSet(0L);
        if (delta == 0) {
            //todo -> check if key exists in redis
            localCounter.set(Long.parseLong(kvStoreClient.get(key)));
            return; // No changes to sync
        }
        try {
            if (delta > 0) {
                // Positive delta - increment
                long newRedisValue = kvStoreClient.incrementBy(key, delta);
                localCounter.set(newRedisValue);
                log.debug("Synced +{} to Redis for key '{}'", delta, key);
            } else {
                // Negative delta - decrement
                long newRedisValue = kvStoreClient.decrementBy(key, Math.abs(delta));
                localCounter.set(newRedisValue);
                log.debug("Synced {} to Redis for key '{}'", delta, key);
            }
        } catch (KeyValueStoreException e) {
            log.error("Error syncing delta {} to Redis for key '{}'. Will retry on next sync. Error: {}",
                    delta, key, e.getMessage());
            // Add the delta back to pending changes for retry
            pendingDelta.addAndGet(delta);
        }
    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object data) {
        ensureInitialized();
        try {
            // Fast local increment
            localCounter.incrementAndGet();

            // Track pending change for Redis sync
            if (kvStoreClient != null && key != null) {
                pendingDelta.incrementAndGet();
            }
            //todo -> return localCounter instead
            log.info("processAdd for both local and Redis counters for key '{}'", key);
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
        ensureInitialized();
        try {
            // Fast local decrement
            localCounter.decrementAndGet();
            // Track pending change for Redis sync
            if (kvStoreClient != null && key != null) {
                pendingDelta.decrementAndGet();
            }
            log.info("processRemove for both local and Redis counters for key '{}'", key);
            return localCounter.get();

        } catch (Exception e) {
            log.error("Error in processRemove for key '{}'. Error: {}", key, e.getMessage());
            return localCounter.get();
        }
    }

    @Override
    public Object processRemove(Object[] data) {
        // Similar to processAdd(Object[] data)
        return processRemove((Object) data);
    }

    @Override
    public Object reset() {
        // Ensure we're initialized before resetting
        ensureInitialized();

        try {
            localCounter.set(0L);

            if (kvStoreClient != null && key != null) {
                // Reset Redis immediately for reset operations
                kvStoreClient.set(key, "0");
                pendingDelta.set(0L); // Clear pending changes
                log.info("Reset both local and Redis counters for key '{}'", key);
            } else {
                log.info("Reset local counter for key '{}' (Redis not available or key not set)", key);
            }

            return 0L;

        } catch (KeyValueStoreException e) {
            log.error("Error resetting Redis counter for key '{}'. Local counter reset successfully. Error: {}",
                    key, e.getMessage());
            return 0L;
        }
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        try {
            // Only remove if key is not null
            if (key != null) {
                activeAggregators.remove(key);
            }
            // Perform final sync with Redis
            if (kvStoreClient != null && key != null) {
                syncWithRedis();
                log.info("Final sync completed for key '{}' before stop", key);
            }
        } catch (Exception e) {
            log.error("Error during stop for key '{}'. Error: {}", key, e.getMessage());
        }
    }

    @Override
    public Object[] currentState() {
        ensureInitialized();
        long currentValue = localCounter.get();
        if (kvStoreClient != null && key != null) {
            try {
                // Sync any pending changes before returning state
                syncWithRedis();
                log.debug("Synced pending changes before returning state for key '{}'", key);
            } catch (Exception e) {
                log.warn("Could not sync with Redis before returning state for key '{}'. Using local value: {}. Error: {}",
                        key, currentValue, e.getMessage());
            }
        }

        return new Object[]{new AbstractMap.SimpleEntry<>("Value", currentValue)};
    }

    @Override
    public void restoreState(Object[] state) {
        ensureInitialized();
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        long restoredValue = (Long) stateEntry.getValue();

        localCounter.set(restoredValue);
        pendingDelta.set(0L); // Clear pending changes

        if (kvStoreClient != null && key != null) {
            try {
                kvStoreClient.set(key, String.valueOf(restoredValue));
                log.info("Successfully restored state for key '{}' to {} in both local and Redis counters.", key, restoredValue);
            } catch (KeyValueStoreException e) {
                log.error("Error restoring state to Redis for key '{}'. State restored to local counter only. Error: {}",
                        key, e.getMessage());
            }
        } else {
            log.warn("restoreState: Redis not available or key not set for key '{}'. State restored to local counter only.", key);
        }
    }
}



//    private static final CopyOnWriteArrayList<Long> operationDurations = new CopyOnWriteArrayList<>();
//    private static final ScheduledExecutorService avgLogger = Executors.newSingleThreadScheduledExecutor();
//
//    static {
//        avgLogger.scheduleAtFixedRate(() -> {
//            if (!operationDurations.isEmpty()) {
//                long sum = 0;
//                for (Long duration : operationDurations) {
//                    sum += duration;
//                }
//                double avg = sum / (double) operationDurations.size();
//                log.info("Average Redis increment/decrement operation duration in the last minute: {} ms ({} samples)", avg, operationDurations.size());
//                operationDurations.clear();
//            }
//        }, 1, 1, TimeUnit.MINUTES);
//    }