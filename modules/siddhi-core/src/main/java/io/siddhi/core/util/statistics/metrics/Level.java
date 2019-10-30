/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * <p> This class defines a set of standard levels to be used in SP Metrics. This class is similar to
 * org.apache.logging.log4j.Level in Apache Log4j 2 project. </p> <p> Levels are organized from most specific to least:
 * </p> <ul> <li>{@link #OFF} (most specific, no metrics)</li> <li>{@link #BASIC}</li> <li>{@link #DETAIL}</li></ul>
 */
public class Level implements Comparable<Level> {

    /**
     * No events will be used for metrics.
     */
    public static final Level OFF;
    /**
     * A metric for informational purposes.
     */
    public static final Level BASIC;
    /**
     * A general debugging metric.
     */
    public static final Level DETAIL;
    private static final ConcurrentMap<String, Level> levels = new ConcurrentHashMap<String, Level>();

    static {
        OFF = new Level("OFF", 0);
        BASIC = new Level("BASIC", 400);
        DETAIL = new Level("DETAIL", 500);
    }

    private final String name;
    private final int intLevel;

    private Level(final String name, final int intLevel) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Illegal null Level constant");
        }
        if (intLevel < 0) {
            throw new IllegalArgumentException("Illegal Level int less than zero.");
        }
        this.name = name;
        this.intLevel = intLevel;
        if (levels.putIfAbsent(name, this) != null) {
            throw new IllegalStateException("Level " + name + " has already been defined.");
        }
    }

    /**
     * Return the Level associated with the name.
     *
     * @param name The name of the Level to return.
     * @return The Level.
     * @throws NullPointerException     if the Level name is {@code null}.
     * @throws IllegalArgumentException if the Level name is not registered.
     */
    public static Level valueOf(final String name) {
        if (name == null) {
            throw new NullPointerException("No level name given.");
        }
        final String levelName = name.toUpperCase();
        if (levels.containsKey(levelName)) {
            return levels.get(levelName);
        }
        throw new IllegalArgumentException("Unknown level constant [" + levelName + "].");
    }

    @Override
    public int compareTo(final Level other) {
        return intLevel < other.intLevel ? -1 : (intLevel > other.intLevel ? 1 : 0);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof Level && other == this;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    /**
     * Gets the symbolic name of this Level. Equivalent to calling {@link #toString()}.
     *
     * @return the name of this Level.
     */
    public String name() {
        return this.name;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
