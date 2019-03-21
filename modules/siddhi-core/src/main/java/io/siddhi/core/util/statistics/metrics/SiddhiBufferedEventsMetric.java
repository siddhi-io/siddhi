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
import io.siddhi.core.util.statistics.BufferedEventsTracker;
import io.siddhi.core.util.statistics.EventBufferHolder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Class to calculate BufferedEvents in Siddhi.
 */
public class SiddhiBufferedEventsMetric implements BufferedEventsTracker {
    private ConcurrentMap<Object, SiddhiBufferedEventsMetric.ObjectMetric> registeredObjects =
            new ConcurrentHashMap<>();
    private MetricRegistry metricRegistry;

    public SiddhiBufferedEventsMetric(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    /**
     * Register the EventBufferHolder that needs to be measured the used capacity usage
     *
     * @param eventBufferHolder EventBufferHolder
     * @param name              An unique value to identify the object.
     */
    @Override
    public void registerEventBufferHolder(EventBufferHolder eventBufferHolder, String name) {
        if (registeredObjects.get(eventBufferHolder) == null) {
            ObjectMetric objectMetric = new ObjectMetric(eventBufferHolder, name);
            metricRegistry.register(name, objectMetric.getGauge());
            registeredObjects.put(eventBufferHolder, new ObjectMetric(eventBufferHolder, name));
        }
    }

    @Override
    public void enableEventBufferHolderMetrics() {
        for (ConcurrentMap.Entry<Object, ObjectMetric> entry :
                registeredObjects.entrySet()) {
            if (!metricRegistry.getNames().contains(entry.getValue().getName())) {
                metricRegistry.register(entry.getValue().getName(), entry.getValue().getGauge());
            }
        }
    }

    @Override
    public void disableEventBufferHolderMetrics() {
        for (ConcurrentMap.Entry<Object, ObjectMetric> entry :
                registeredObjects.entrySet()) {
            metricRegistry.remove(entry.getValue().getName());
        }
    }

    /**
     * @param eventBufferHolder Event buffer holder
     * @return Name of the BufferedEvents tracker.
     */
    @Override
    public String getName(EventBufferHolder eventBufferHolder) {
        if (registeredObjects.get(eventBufferHolder) != null) {
            return registeredObjects.get(eventBufferHolder).getName();
        } else {
            return null;
        }
    }

    class ObjectMetric {

        private final EventBufferHolder eventBufferHolder;
        private String name;
        private Gauge gauge;

        public ObjectMetric(final EventBufferHolder eventBufferHolder, String name) {
            this.eventBufferHolder = eventBufferHolder;
            this.name = name;
            this.gauge = new Gauge<Long>() {
                @Override
                public Long getValue() {
                    try {
                        if (eventBufferHolder != null) {
                            return eventBufferHolder.getBufferedEvents();
                        } else {
                            return 0L;
                        }
                    } catch (Throwable e) {
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
