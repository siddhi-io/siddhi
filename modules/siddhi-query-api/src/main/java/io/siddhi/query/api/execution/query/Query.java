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
package io.siddhi.query.api.execution.query;

import io.siddhi.query.api.SiddhiElement;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.execution.ExecutionElement;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.output.ratelimit.OutputRate;
import io.siddhi.query.api.execution.query.output.ratelimit.SnapshotOutputRate;
import io.siddhi.query.api.execution.query.output.stream.DeleteStream;
import io.siddhi.query.api.execution.query.output.stream.InsertIntoStream;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.output.stream.ReturnStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateOrInsertStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import io.siddhi.query.api.execution.query.output.stream.UpdateStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Siddhi Query
 */
public class Query implements ExecutionElement, SiddhiElement {

    private static final long serialVersionUID = 1L;
    private InputStream inputStream;
    private Selector selector = new Selector();
    private OutputStream outputStream = new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS);
    private OutputRate outputRate;
    private List<Annotation> annotations = new ArrayList<Annotation>();
    private int[] queryContextStartIndex;
    private int[] queryContextEndIndex;

    public static Query query() {

        return new Query();
    }

    public Query from(InputStream inputStream) {

        this.inputStream = inputStream;
        return this;
    }

    public InputStream getInputStream() {

        return inputStream;
    }

    public Query select(Selector selector) {

        this.selector = selector;
        return this;
    }

    public Selector getSelector() {

        return selector;
    }

    public Query outStream(OutputStream outputStream) {

        this.outputStream = outputStream;
        updateOutputEventType(outputRate, outputStream);
        return this;
    }

    public Query insertInto(String outputStreamId, OutputStream.OutputEventType outputEventType) {

        this.outputStream = new InsertIntoStream(outputStreamId, outputEventType);
        updateOutputEventType(outputRate, outputStream);
        return this;
    }

    public Query insertInto(String outputStreamId) {

        this.outputStream = new InsertIntoStream(outputStreamId);
        updateOutputEventType(outputRate, outputStream);
        return this;
    }

    public Query insertIntoInner(String outputStreamId, OutputStream.OutputEventType outputEventType) {

        this.outputStream = new InsertIntoStream(outputStreamId, true, outputEventType);
        updateOutputEventType(outputRate, outputStream);
        return this;
    }

    public Query insertIntoInner(String outputStreamId) {

        this.outputStream = new InsertIntoStream(outputStreamId, true);
        updateOutputEventType(outputRate, outputStream);
        return this;
    }

    public Query insertIntoFault(String outputStreamId, OutputStream.OutputEventType outputEventType) {

        this.outputStream = new InsertIntoStream(outputStreamId, false, true, outputEventType);
        updateOutputEventType(outputRate, outputStream);
        return this;
    }

    public Query insertIntoFault(String outputStreamId) {

        this.outputStream = new InsertIntoStream(outputStreamId, false, true);
        updateOutputEventType(outputRate, outputStream);
        return this;
    }

    public Query returns() {

        this.outputStream = new ReturnStream();
        updateOutputEventType(outputRate, outputStream);
        return this;
    }

    public Query returns(OutputStream.OutputEventType outputEventType) {

        this.outputStream = new ReturnStream(outputEventType);
        updateOutputEventType(outputRate, outputStream);
        return this;
    }

    public void deleteBy(String outputTableId, Expression onDeletingExpression) {

        this.outputStream = new DeleteStream(outputTableId, onDeletingExpression);
        updateOutputEventType(outputRate, outputStream);
    }

    public void deleteBy(String outputTableId, OutputStream.OutputEventType outputEventType, Expression
            onDeletingExpression) {

        this.outputStream = new DeleteStream(outputTableId, outputEventType, onDeletingExpression);
        updateOutputEventType(outputRate, outputStream);
    }

    public void updateBy(String outputTableId, Expression onUpdateExpression) {

        this.outputStream = new UpdateStream(outputTableId, onUpdateExpression);
        updateOutputEventType(outputRate, outputStream);
    }

    public void updateBy(String outputTableId, UpdateSet updateSetAttributes, Expression onUpdateExpression) {

        this.outputStream = new UpdateStream(outputTableId, updateSetAttributes, onUpdateExpression);
        updateOutputEventType(outputRate, outputStream);
    }

    public void updateBy(String outputTableId, OutputStream.OutputEventType outputEventType,
                         Expression onUpdateExpression) {

        this.outputStream = new UpdateStream(outputTableId, outputEventType, null, onUpdateExpression);
        updateOutputEventType(outputRate, outputStream);
    }

    public void updateBy(String outputTableId, OutputStream.OutputEventType outputEventType,
                         UpdateSet updateSetAttributes, Expression onUpdateExpression) {

        this.outputStream = new UpdateStream(outputTableId, outputEventType, updateSetAttributes, onUpdateExpression);
        updateOutputEventType(outputRate, outputStream);
    }

    public void updateOrInsertBy(String outputTableId, UpdateSet updateSetAttributes, Expression onUpdateExpression) {

        this.outputStream = new UpdateOrInsertStream(outputTableId, updateSetAttributes, onUpdateExpression);
        updateOutputEventType(outputRate, outputStream);
    }

    public void updateOrInsertBy(String outputTableId, OutputStream.OutputEventType outputEventType,
                                 UpdateSet updateSetAttributes, Expression onUpdateExpression) {

        this.outputStream = new UpdateOrInsertStream(outputTableId, outputEventType, updateSetAttributes,
                onUpdateExpression);
        updateOutputEventType(outputRate, outputStream);

    }

    public OutputStream getOutputStream() {

        return outputStream;
    }

    public void output(OutputRate outputRate) {

        this.outputRate = outputRate;
        updateOutputEventType(outputRate, outputStream);
    }

    private void updateOutputEventType(OutputRate outputRate, OutputStream outputStream) {

        if (outputStream != null && outputStream.getOutputEventType() == null) {
            if (outputRate instanceof SnapshotOutputRate) {
                outputStream.setOutputEventType(OutputStream.OutputEventType.ALL_EVENTS);
            } else {
                outputStream.setOutputEventType(OutputStream.OutputEventType.CURRENT_EVENTS);
            }
        }
    }

    public OutputRate getOutputRate() {

        return outputRate;
    }

    public Query annotation(Annotation annotation) {

        annotations.add(annotation);
        return this;
    }

    public List<Annotation> getAnnotations() {

        return annotations;
    }

    @Override
    public String toString() {

        return "Query{" +
                "stream=" + inputStream +
                ", selector=" + selector +
                ", outputStream=" + outputStream +
                ", outputRate=" + outputRate +
                ", annotations=" + annotations +
                '}';
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof Query)) {
            return false;
        }

        Query query = (Query) o;

        if (annotations != null ? !annotations.equals(query.annotations) : query.annotations != null) {
            return false;
        }
        if (inputStream != null ? !inputStream.equals(query.inputStream) : query.inputStream != null) {
            return false;
        }
        if (outputRate != null ? !outputRate.equals(query.outputRate) : query.outputRate != null) {
            return false;
        }
        if (outputStream != null ? !outputStream.equals(query.outputStream) : query.outputStream != null) {
            return false;
        }
        if (selector != null ? !selector.equals(query.selector) : query.selector != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {

        int result = inputStream != null ? inputStream.hashCode() : 0;
        result = 31 * result + (selector != null ? selector.hashCode() : 0);
        result = 31 * result + (outputStream != null ? outputStream.hashCode() : 0);
        result = 31 * result + (outputRate != null ? outputRate.hashCode() : 0);
        result = 31 * result + (annotations != null ? annotations.hashCode() : 0);
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
