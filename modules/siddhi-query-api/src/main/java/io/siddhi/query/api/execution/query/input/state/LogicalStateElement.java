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

/**
 * Logical state element used in pattern to handle logical operations
 */
public class LogicalStateElement implements StateElement {

    private static final long serialVersionUID = 1L;
    protected StreamStateElement streamStateElement1;
    protected Type type;
    protected StreamStateElement streamStateElement2;
    private int[] queryContextStartIndex;
    private int[] queryContextEndIndex;

    public LogicalStateElement(StreamStateElement streamStateElement1, Type type,
                               StreamStateElement streamStateElement2) {
        this.streamStateElement1 = streamStateElement1;
        this.type = type;
        this.streamStateElement2 = streamStateElement2;
    }

    public StreamStateElement getStreamStateElement1() {
        return streamStateElement1;
    }

    public StreamStateElement getStreamStateElement2() {
        return streamStateElement2;
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return "LogicalStateElement{" +
                "streamStateElement1=" + streamStateElement1 +
                ", type=" + type +
                ", streamStateElement2=" + streamStateElement2 +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogicalStateElement)) {
            return false;
        }

        LogicalStateElement that = (LogicalStateElement) o;

        if (streamStateElement1 != null ? !streamStateElement1.equals(that.streamStateElement1) : that
                .streamStateElement1 != null) {
            return false;
        }
        if (streamStateElement2 != null ? !streamStateElement2.equals(that.streamStateElement2) : that
                .streamStateElement2 != null) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = streamStateElement1 != null ? streamStateElement1.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (streamStateElement2 != null ? streamStateElement2.hashCode() : 0);
        return result;
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

    /**
     * Different type of logical condition
     */
    public enum Type {
        AND,
        OR
    }
}
