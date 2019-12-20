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

package io.siddhi.query.api.definition;

import io.siddhi.query.api.SiddhiElement;
import io.siddhi.query.api.expression.constant.TimeConstant;

/**
 * Siddhi Trigger Definition
 */
public class TriggerDefinition implements SiddhiElement {

    private static final long serialVersionUID = 1L;
    private String id;
    private Long atEvery;
    private String at;
    private int[] queryContextStartIndex;
    private int[] queryContextEndIndex;

    public TriggerDefinition() {
    }

    protected TriggerDefinition(String id) {
        this.id = id;
    }

    public static TriggerDefinition id(String id) {
        return new TriggerDefinition(id);
    }

    public String getId() {
        return id;
    }

    public Long getAtEvery() {
        return atEvery;
    }

    public String getAt() {
        return at;
    }

    public TriggerDefinition atEvery(long timeInMilliSeconds) {
        this.atEvery = timeInMilliSeconds;
        return this;
    }

    public TriggerDefinition at(String interval) {
        this.at = interval;
        return this;
    }

    public TriggerDefinition atEvery(TimeConstant time) {
        this.atEvery = time.value();
        return this;
    }

    @Override
    public int[] getQueryContextStartIndex() {
        return queryContextStartIndex;
    }

    @Override
    public void setQueryContextStartIndex(int[] lineAndColumn) {
        queryContextStartIndex = lineAndColumn;
    }

    @Override
    public int[] getQueryContextEndIndex() {
        return queryContextEndIndex;
    }

    @Override
    public void setQueryContextEndIndex(int[] lineAndColumn) {
        queryContextEndIndex = lineAndColumn;
    }
}
