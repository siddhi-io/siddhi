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
package io.siddhi.query.api.execution.query.input.state;

import io.siddhi.query.api.execution.query.input.stream.BasicSingleInputStream;

/**
 * State element containing the event stream
 */
public class StreamStateElement implements StateElement {

    private static final long serialVersionUID = 1L;
    private final BasicSingleInputStream basicSingleInputStream;
    private int[] queryContextStartIndex;
    private int[] queryContextEndIndex;

    public StreamStateElement(BasicSingleInputStream basicSingleInputStream) {

        this.basicSingleInputStream = basicSingleInputStream;
    }

    public BasicSingleInputStream getBasicSingleInputStream() {
        return basicSingleInputStream;
    }

    @Override
    public String toString() {
        return "StreamStateElement{" +
                "basicSingleInputStream=" + basicSingleInputStream +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StreamStateElement)) {
            return false;
        }

        StreamStateElement that = (StreamStateElement) o;

        if (basicSingleInputStream != null ? !basicSingleInputStream.equals(that.basicSingleInputStream) : that
                .basicSingleInputStream != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return basicSingleInputStream != null ? basicSingleInputStream.hashCode() : 0;
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
