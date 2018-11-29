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

package org.wso2.siddhi.core.util.statistics;

import org.wso2.siddhi.query.api.annotation.Element;

import java.util.List;

/**
 * Factory interface to create Trackers and Managers
 */
public interface StatisticsTrackerFactory {

    LatencyTracker createLatencyTracker(String name, StatisticsManager statisticsManager,
                                        MetricsLogLevel metricsLogLevel);

    ThroughputTracker createThroughputTracker(String name, StatisticsManager statisticsManager,
                                              MetricsLogLevel metricsLogLevel);

    BufferedEventsTracker createBufferSizeTracker(StatisticsManager statisticsManager, MetricsLogLevel metricsLogLevel);

    MemoryUsageTracker createMemoryUsageTracker(StatisticsManager statisticsManager, MetricsLogLevel metricsLogLevel);

    StatisticsManager createStatisticsManager(String prefix, String siddhiAppName, List<Element> elements,
                                              boolean isStatisticsEnabled);

    Comparable getLogLevel(MetricsLogLevel metricsLogLevel);

    /**
     * Levels of metrics
     */
    public enum MetricsLogLevel {
        OFF,
        INFO,
        DEBUG,
        TRACE,
        ALL
    }

}
