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
package io.siddhi.query.api.execution.query.input.stream;

import io.siddhi.query.api.SiddhiElement;
import io.siddhi.query.api.aggregation.Within;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.state.StateElement;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.constant.TimeConstant;

import java.util.List;

/**
 * Input stream in queries
 */
public abstract class InputStream implements SiddhiElement {

    private static final long serialVersionUID = 1L;
    private int[] queryContextStartIndex;
    private int[] queryContextEndIndex;

    public static InputStream joinStream(SingleInputStream leftStream, JoinInputStream.Type type,
                                         SingleInputStream rightStream) {
        return new JoinInputStream(leftStream, type, rightStream, null,
                JoinInputStream.EventTrigger.ALL, null, null);
    }

    public static InputStream joinStream(SingleInputStream leftStream, JoinInputStream.Type type,
                                         SingleInputStream rightStream, Expression onCompare) {
        return new JoinInputStream(leftStream, type, rightStream, onCompare,
                JoinInputStream.EventTrigger.ALL, null, null);
    }

    public static InputStream joinStream(SingleInputStream leftStream, JoinInputStream.Type type,
                                         SingleInputStream rightStream, Expression onCompare,
                                         JoinInputStream.EventTrigger trigger) {
        return new JoinInputStream(leftStream, type, rightStream, onCompare,
                trigger, null, null);
    }

    public static InputStream joinStream(SingleInputStream leftStream, JoinInputStream.Type type,
                                         SingleInputStream rightStream, JoinInputStream.EventTrigger trigger) {
        return new JoinInputStream(leftStream, type, rightStream, null,
                trigger, null, null);
    }

    public static InputStream joinStream(SingleInputStream leftStream, JoinInputStream.Type type,
                                         SingleInputStream rightStream, Expression onCompare,
                                         Within within, Expression per) {
        return new JoinInputStream(leftStream, type, rightStream, onCompare,
                JoinInputStream.EventTrigger.ALL, within, per);
    }

    public static InputStream joinStream(SingleInputStream leftStream, JoinInputStream.Type type,
                                         SingleInputStream rightStream,
                                         Within within, Expression per) {
        return new JoinInputStream(leftStream, type, rightStream, null,
                JoinInputStream.EventTrigger.ALL, within, per);
    }

    public static InputStream joinStream(SingleInputStream leftStream, JoinInputStream.Type type,
                                         SingleInputStream rightStream, Expression onCompare,
                                         JoinInputStream.EventTrigger trigger,
                                         Within within, Expression per) {
        return new JoinInputStream(leftStream, type, rightStream, onCompare,
                trigger, within, per);
    }

    public static InputStream joinStream(SingleInputStream leftStream, JoinInputStream.Type type,
                                         SingleInputStream rightStream, JoinInputStream.EventTrigger trigger,
                                         Within within, Expression per) {
        return new JoinInputStream(leftStream, type, rightStream, null,
                trigger, within, per);
    }

    public static StateInputStream patternStream(StateElement patternElement) {
        return new StateInputStream(StateInputStream.Type.PATTERN, patternElement, null);
    }

    public static StateInputStream patternStream(StateElement patternElement, TimeConstant timeConstant) {
        return new StateInputStream(StateInputStream.Type.PATTERN, patternElement, timeConstant);
    }

    public static StateInputStream sequenceStream(StateElement sequenceElement) {
        return new StateInputStream(StateInputStream.Type.SEQUENCE, sequenceElement, null);
    }

    public static StateInputStream sequenceStream(StateElement sequenceElement, TimeConstant timeConstant) {
        return new StateInputStream(StateInputStream.Type.SEQUENCE, sequenceElement, timeConstant);
    }

    public static BasicSingleInputStream innerStream(String streamId) {
        return new BasicSingleInputStream(null, streamId, true, false);
    }

    public static BasicSingleInputStream innerStream(String streamReferenceId, String streamId) {
        return new BasicSingleInputStream(streamReferenceId, streamId, true, false);
    }

    public static BasicSingleInputStream faultStream(String streamId) {
        return new BasicSingleInputStream(null, streamId, false, true);
    }

    public static BasicSingleInputStream faultStream(String streamReferenceId, String streamId) {
        return new BasicSingleInputStream(streamReferenceId, streamId, false, true);
    }

    public static BasicSingleInputStream stream(String streamId) {
        return new BasicSingleInputStream(null, streamId);
    }

    public static BasicSingleInputStream stream(String streamReferenceId, String streamId) {
        return new BasicSingleInputStream(streamReferenceId, streamId);
    }

    public static SingleInputStream stream(Query query) {
        return new AnonymousInputStream(query);
    }

    public abstract List<String> getAllStreamIds();

    public abstract List<String> getUniqueStreamIds();

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
