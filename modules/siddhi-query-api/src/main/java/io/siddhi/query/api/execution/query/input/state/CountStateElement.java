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
 * Count state element used in patterns
 */
public class CountStateElement implements StateElement {

    public static final int ANY = -1;
    private static final long serialVersionUID = 1L;
    private StreamStateElement streamStateElement;
    private int minCount = ANY;
    private int maxCount = ANY;
    private int[] queryContextStartIndex;
    private int[] queryContextEndIndex;

    public CountStateElement(StreamStateElement streamStateElement, int minCount, int maxCount) {
        this.streamStateElement = streamStateElement;
        this.minCount = minCount;
        this.maxCount = maxCount;
    }

    public int getMinCount() {
        return minCount;
    }

    public int getMaxCount() {
        return maxCount;
    }

    public StreamStateElement getStreamStateElement() {
        return streamStateElement;
    }

    @Override
    public String toString() {
        return "CountStateElement{" +
                "streamStateElement=" + streamStateElement +
                ", minCount=" + minCount +
                ", maxCount=" + maxCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CountStateElement)) {
            return false;
        }

        CountStateElement that = (CountStateElement) o;

        if (maxCount != that.maxCount) {
            return false;
        }
        if (minCount != that.minCount) {
            return false;
        }
        if (streamStateElement != null ? !streamStateElement.equals(that.streamStateElement) : that
                .streamStateElement != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = streamStateElement != null ? streamStateElement.hashCode() : 0;
        result = 31 * result + minCount;
        result = 31 * result + maxCount;
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
}
