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
package io.siddhi.query.api.execution.query;

import io.siddhi.query.api.SiddhiElement;
import io.siddhi.query.api.execution.query.input.store.InputStore;
import io.siddhi.query.api.execution.query.output.stream.DeleteStream;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.output.stream.ReturnStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateOrInsertStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import io.siddhi.query.api.execution.query.output.stream.UpdateStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;

import static io.siddhi.query.api.execution.query.output.stream.OutputStream.OutputEventType.CURRENT_EVENTS;

/**
 * This class keep information of on-demand query.
 */
public class OnDemandQuery implements SiddhiElement {

    private static final long serialVersionUID = 1L;
    private InputStore inputStore;
    private Selector selector = new Selector();
    private OutputStream outputStream = new ReturnStream(CURRENT_EVENTS);
    private int[] queryContextStartIndex;
    private int[] queryContextEndIndex;
    private OnDemandQueryType type;

    /**
     * Builder method to get a new on-demand query instance
     *
     * @return a new OnDemandQuery instance
     */
    public static OnDemandQuery query() {

        return new OnDemandQuery();
    }

    /**
     * Builder method to set input store to the on-demand query
     *
     * @param inputStore inputStore for the on-demand query
     * @return updated on-demand query
     */
    public OnDemandQuery from(InputStore inputStore) {

        this.inputStore = inputStore;
        return this;
    }

    /**
     * Getter for the input store
     *
     * @return inputStore
     */
    public InputStore getInputStore() {

        return inputStore;
    }

    /**
     * Builder method to set a selector to the on-demand query
     *
     * @param selector selector for the on-demand query
     * @return updated on-demand query
     */
    public OnDemandQuery select(Selector selector) {

        this.selector = selector;
        return this;
    }

    /**
     * Builder method to set an outPutStream to the on-demand query
     *
     * @param outputStream outPutStream for the on-demand query
     * @return updated on-demand query
     */
    public OnDemandQuery outStream(OutputStream outputStream) {

        this.outputStream = outputStream;
        if (outputStream != null && outputStream.getOutputEventType() == null) {
            outputStream.setOutputEventType(OutputStream.OutputEventType.CURRENT_EVENTS);
        }
        return this;
    }

    /**
     * Method to set a deleteStream as the outputStream of the on-demand query
     *
     * @param outputTableId        id of the table which is going to be queried
     * @param onDeletingExpression expression for the delete operation defined in the on-demand query
     */
    public void deleteBy(String outputTableId, Expression onDeletingExpression) {

        this.outputStream = new DeleteStream(outputTableId, CURRENT_EVENTS, onDeletingExpression);
    }

    /**
     * Method to set an updateStream as the outputStream of the on-demand query
     *
     * @param outputTableId      id of the table which is going to be queried
     * @param onUpdateExpression expression for the update operation defined in the on-demand query
     */
    public void updateBy(String outputTableId, Expression onUpdateExpression) {

        this.outputStream = new UpdateStream(outputTableId, CURRENT_EVENTS, onUpdateExpression);
    }

    /**
     * Method to set an updateStream as the outputStream of the on-demand query
     *
     * @param outputTableId       id of the table which is going to be queried
     * @param updateSetAttributes updateSet for the attributes which are going to be updated.
     * @param onUpdateExpression  expression for the update operation defined in the on-demand query
     */
    public void updateBy(String outputTableId, UpdateSet updateSetAttributes, Expression onUpdateExpression) {

        this.outputStream = new UpdateStream(outputTableId, CURRENT_EVENTS, updateSetAttributes, onUpdateExpression);
    }

    /**
     * Method to set an updateOrInsertStream as the outputStream of the on-demand query
     *
     * @param outputTableId       id of the table which is going to be queried
     * @param updateSetAttributes updateSet for the attributes which are going to be updated.
     * @param onUpdateExpression  expression for the update or insert operation defined in the on-demand query
     */
    public void updateOrInsertBy(String outputTableId, UpdateSet updateSetAttributes, Expression onUpdateExpression) {

        this.outputStream = new UpdateOrInsertStream(outputTableId, CURRENT_EVENTS,
                updateSetAttributes, onUpdateExpression);
    }

    /**
     * Getter method to get the selector of the on-demand query
     *
     * @return selector of the on-demand query
     */
    public Selector getSelector() {

        return selector;
    }

    /**
     * Getter method to get the outputStream of the on-demand query
     *
     * @return outputStream of the on-demand query
     */
    public OutputStream getOutputStream() {

        return outputStream;
    }

    @Override
    public String toString() {

        return "OnDemandQuery{" +
                "inputStore=" + inputStore +
                ", selector=" + selector +
                ", outputStream=" + outputStream +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        OnDemandQuery that;
        if (o instanceof StoreQuery) {
            that = ((StoreQuery) o).getOnDemandQuery();
        } else {
            if (getClass() != o.getClass()) {
                return false;
            }
            that = (OnDemandQuery) o;
        }
        if (inputStore != null ? !inputStore.equals(that.inputStore) : that.inputStore != null) {
            return false;
        }
        if (outputStream != null ? !outputStream.equals(that.outputStream) : that.outputStream != null) {
            return false;
        }
        return selector != null ? selector.equals(that.selector) : that.selector == null;
    }

    @Override
    public int hashCode() {

        int result = inputStore != null ? inputStore.hashCode() : 0;
        result = 31 * result + (selector != null ? selector.hashCode() : 0);
        result = 31 * result + (outputStream != null ? outputStream.hashCode() : 0);
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
     * This method returns the type of given on-demand query.
     *
     * @return type of given on-demand query
     */
    public OnDemandQueryType getType() {

        return type;
    }

    /**
     * This method sets the type of given on-demand query.
     */
    public void setType(OnDemandQueryType type) {

        this.type = type;
    }

    /**
     * This enum is used to identify the type of a on-demand query.
     * Type can be one of the following.
     * - INSERT
     * - DELETE
     * - UPDATE
     * - SELECT
     * - FIND
     * - UPDATE OR INSERT
     */
    public enum OnDemandQueryType {
        INSERT,
        DELETE,
        UPDATE,
        SELECT,
        UPDATE_OR_INSERT,
        FIND
    }
}
