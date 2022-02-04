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
package io.siddhi.query.api.expression.condition;

import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.util.SiddhiConstants;

/**
 * Condition {@link Expression} checking whether the event is null
 */
public class IsNull extends Expression {

    private static final long serialVersionUID = 1L;

    private String streamId;
    private Integer streamIndex;
    private boolean isInnerStream;
    private boolean isFaultStream;
    private Expression expression;

    public IsNull(Expression expression) {

        this.expression = expression;
    }

    public IsNull(String streamId, Integer streamIndex, boolean isInnerStream) {

        this(streamId, streamIndex, isInnerStream, false);
    }

    public IsNull(String streamId, Integer streamIndex, boolean isInnerStream, boolean isFaultStream) {

        this.streamIndex = streamIndex;
        this.isInnerStream = isInnerStream;
        this.isFaultStream = isFaultStream;
        if (isInnerStream) {
            if (isFaultStream) {
                this.streamId = SiddhiConstants.FAULT_STREAM_FLAG.concat(SiddhiConstants.INNER_STREAM_FLAG)
                        .concat(streamId);
            } else {
                this.streamId = SiddhiConstants.INNER_STREAM_FLAG.concat(streamId);
            }
        } else {
            if (isFaultStream) {
                this.streamId = SiddhiConstants.FAULT_STREAM_FLAG.concat(streamId);
            } else {
                this.streamId = streamId;
            }
        }
    }

    public Expression getExpression() {

        return expression;
    }

    public String getStreamId() {

        return streamId;
    }

    public Integer getStreamIndex() {

        return streamIndex;
    }

    public boolean isInnerStream() {

        return isInnerStream;
    }

    public boolean isFaultStream() {

        return isFaultStream;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof IsNull)) {
            return false;
        }

        IsNull isNull = (IsNull) o;

        if (isInnerStream != isNull.isInnerStream) {
            return false;
        }
        if (isFaultStream != isNull.isFaultStream) {
            return false;
        }
        if (streamId != null ? !streamId.equals(isNull.streamId) : isNull.streamId != null) {
            return false;
        }
        if (streamIndex != null ? !streamIndex.equals(isNull.streamIndex) : isNull.streamIndex != null) {
            return false;
        }
        return expression != null ? expression.equals(isNull.expression) : isNull.expression == null;
    }

    @Override
    public int hashCode() {

        int result = streamId != null ? streamId.hashCode() : 0;
        result = 31 * result + (streamIndex != null ? streamIndex.hashCode() : 0);
        result = 31 * result + (isInnerStream ? 1 : 0);
        result = 31 * result + (isFaultStream ? 1 : 0);
        result = 31 * result + (expression != null ? expression.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {

        return "IsNull{" +
                "streamId='" + streamId + '\'' +
                ", streamIndex=" + streamIndex +
                ", isInnerStream=" + isInnerStream +
                ", isFaultStream=" + isFaultStream +
                ", expression=" + expression +
                '}';
    }
}
