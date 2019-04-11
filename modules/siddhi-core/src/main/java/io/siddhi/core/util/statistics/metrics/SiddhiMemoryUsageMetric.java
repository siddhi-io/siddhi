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

package io.siddhi.core.util.statistics.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.siddhi.core.util.statistics.MemoryUsageTracker;
import io.siddhi.core.util.statistics.memory.ObjectSizeCalculator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Class to calculate Siddhi memory usage.
 */
public class SiddhiMemoryUsageMetric implements MemoryUsageTracker {
    private ConcurrentMap<Object, ObjectMetric> registeredObjects = new ConcurrentHashMap<Object, ObjectMetric>();
    private MetricRegistry metricRegistry;

    public SiddhiMemoryUsageMetric(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    /**
     * Register the object that needs to be measured the memory usage
     *
     * @param object Object
     * @param name   An unique value to identify the object.
     */
    @Override
    public void registerObject(Object object, String name) {
        if (registeredObjects.get(object) == null) {
            ObjectMetric objectMetric = new ObjectMetric(object, name);
            metricRegistry.register(name, objectMetric.getGauge());
            registeredObjects.put(object, new ObjectMetric(object, name));
        }
    }

    @Override
    public void enableMemoryUsageMetrics() {
        for (ConcurrentMap.Entry<Object, ObjectMetric> entry :
                registeredObjects.entrySet()) {
            if (!metricRegistry.getNames().contains(entry.getValue().getName())) {
                metricRegistry.register(entry.getValue().getName(), entry.getValue().getGauge());
            }
        }
    }

    @Override
    public void disableMemoryUsageMetrics() {
        for (ConcurrentMap.Entry<Object, ObjectMetric> entry : registeredObjects.entrySet()) {
            metricRegistry.remove(entry.getValue().getName());
        }
    }

    /**
     * @return Name of the memory usage tracker.
     */
    @Override
    public String getName(Object object) {
        if (registeredObjects.get(object) != null) {
            return registeredObjects.get(object).getName();
        } else {
            return null;
        }
    }

    class ObjectMetric {

        private final Object object;
        private String name;
        private Gauge gauge;

        public ObjectMetric(final Object object, String name) {
            this.object = object;
            this.name = name;
            this.gauge = new Gauge<Long>() {
                @Override
                public Long getValue() {
                    try {
                        return ObjectSizeCalculator.getObjectSize(object);
                    } catch (UnsupportedOperationException e) {
                        return 0L;
                    }
                }
            };
        }

        public String getName() {
            return name;
        }

        public Gauge getGauge() {
            return gauge;
        }
    }
}
