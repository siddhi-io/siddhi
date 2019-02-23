/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * This class keep information of a store query.
 */
public class StoreQuery implements SiddhiElement {

    private static final long serialVersionUID = 1L;
    private InputStore inputStore;
    private Selector selector = new Selector();
    private OutputStream outputStream = new ReturnStream(CURRENT_EVENTS);
    private int[] queryContextStartIndex;
    private int[] queryContextEndIndex;
    private StoreQueryType type;

    /**
     * Builder method to get a new store query instance
     *
     * @return a new storeQuery instance
     */
    public static StoreQuery query() {
        return new StoreQuery();
    }

    /**
     * Builder method to set input store to the store query
     *
     * @param inputStore inputStore for the store query
     * @return updated store query
     */
    public StoreQuery from(InputStore inputStore) {
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
     * Builder method to set a selector to the store query
     *
     * @param selector selector for the store query
     * @return updated store query
     */
    public StoreQuery select(Selector selector) {
        this.selector = selector;
        return this;
    }

    /**
     * Builder method to set an outPutStream to the store query
     *
     * @param outputStream outPutStream for the store query
     * @return updated store query
     */
    public StoreQuery outStream(OutputStream outputStream) {
        this.outputStream = outputStream;
        if (outputStream != null && outputStream.getOutputEventType() == null) {
            outputStream.setOutputEventType(OutputStream.OutputEventType.CURRENT_EVENTS);
        }
        return this;
    }

    /**
     * Method to set a deleteStream as the outputStream of the store query
     *
     * @param outputTableId        id of the table which is going to be queried
     * @param onDeletingExpression expression for the delete operation defined in the store query
     */
    public void deleteBy(String outputTableId, Expression onDeletingExpression) {
        this.outputStream = new DeleteStream(outputTableId, CURRENT_EVENTS, onDeletingExpression);
    }

    /**
     * Method to set an updateStream as the outputStream of the store query
     *
     * @param outputTableId      id of the table which is going to be queried
     * @param onUpdateExpression expression for the update operation defined in the store query
     */
    public void updateBy(String outputTableId, Expression onUpdateExpression) {
        this.outputStream = new UpdateStream(outputTableId, CURRENT_EVENTS, onUpdateExpression);
    }

    /**
     * Method to set an updateStream as the outputStream of the store query
     *
     * @param outputTableId       id of the table which is going to be queried
     * @param updateSetAttributes updateSet for the attributes which are going to be updated.
     * @param onUpdateExpression  expression for the update operation defined in the store query
     */
    public void updateBy(String outputTableId, UpdateSet updateSetAttributes, Expression onUpdateExpression) {
        this.outputStream = new UpdateStream(outputTableId, CURRENT_EVENTS, updateSetAttributes, onUpdateExpression);
    }

    /**
     * Method to set an updateOrInsertStream as the outputStream of the store query
     *
     * @param outputTableId       id of the table which is going to be queried
     * @param updateSetAttributes updateSet for the attributes which are going to be updated.
     * @param onUpdateExpression  expression for the update or insert operation defined in the store query
     */
    public void updateOrInsertBy(String outputTableId, UpdateSet updateSetAttributes, Expression onUpdateExpression) {
        this.outputStream = new UpdateOrInsertStream(outputTableId, CURRENT_EVENTS,
                updateSetAttributes, onUpdateExpression);
    }

    /**
     * Getter method to get the selector of the store query
     *
     * @return selector of the store query
     */
    public Selector getSelector() {
        return selector;
    }

    /**
     * Getter method to get the outputStream of the store query
     *
     * @return outputStream of the store query
     */
    public OutputStream getOutputStream() {
        return outputStream;
    }

    @Override
    public String toString() {
        return "StoreQuery{" +
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StoreQuery that = (StoreQuery) o;

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
     * This method returns the type of given store query.
     *
     * @return type of given store query
     */
    public StoreQueryType getType() {
        return type;
    }

    /**
     * This method sets the type of given store query.
     */
    public void setType(StoreQueryType type) {
        this.type = type;
    }

    /**
     * This enum is used to identify the type of a store query.
     * Type can be one of the following.
     * - INSERT
     * - DELETE
     * - UPDATE
     * - SELECT
     * - FIND
     * - UPDATE OR INSERT
     */
    public enum StoreQueryType {
        INSERT,
        DELETE,
        UPDATE,
        SELECT,
        UPDATE_OR_INSERT,
        FIND
    }
}
