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
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;

/**
 * This class keep information of on-demand query.
 * This is deprecated use OnDemandQuery instead
 */
@Deprecated
public class StoreQuery implements SiddhiElement {

    private OnDemandQuery onDemandQuery = new OnDemandQuery();

    public StoreQuery(OnDemandQuery onDemandQuery) {

        this.onDemandQuery = onDemandQuery;
    }

    public StoreQuery() {

    }

    /**
     * Builder method to get a new on-demand query instance
     *
     * @return a new storeQuery instance
     */
    public static StoreQuery query() {

        return new StoreQuery();
    }

    public OnDemandQuery getOnDemandQuery() {

        return onDemandQuery;
    }

    /**
     * Builder method to set input store to the store query
     *
     * @param inputStore inputStore for the store query
     * @return updated store query
     */
    public StoreQuery from(InputStore inputStore) {

        onDemandQuery.from(inputStore);
        return this;
    }

    /**
     * Getter for the input store
     *
     * @return inputStore
     */
    public InputStore getInputStore() {

        return onDemandQuery.getInputStore();
    }

    /**
     * Builder method to set a selector to the store query
     *
     * @param selector selector for the store query
     * @return updated store query
     */
    public StoreQuery select(Selector selector) {

        onDemandQuery.select(selector);
        return this;
    }

    /**
     * Builder method to set an outPutStream to the store query
     *
     * @param outputStream outPutStream for the store query
     * @return updated store query
     */
    public StoreQuery outStream(OutputStream outputStream) {

        onDemandQuery.outStream(outputStream);
        return this;
    }

    /**
     * Method to set a deleteStream as the outputStream of the store query
     *
     * @param outputTableId        id of the table which is going to be queried
     * @param onDeletingExpression expression for the delete operation defined in the store query
     */
    public void deleteBy(String outputTableId, Expression onDeletingExpression) {

        onDemandQuery.deleteBy(outputTableId, onDeletingExpression);
    }

    /**
     * Method to set an updateStream as the outputStream of the store query
     *
     * @param outputTableId      id of the table which is going to be queried
     * @param onUpdateExpression expression for the update operation defined in the store query
     */
    public void updateBy(String outputTableId, Expression onUpdateExpression) {

        onDemandQuery.updateBy(outputTableId, onUpdateExpression);
    }

    /**
     * Method to set an updateStream as the outputStream of the store query
     *
     * @param outputTableId       id of the table which is going to be queried
     * @param updateSetAttributes updateSet for the attributes which are going to be updated.
     * @param onUpdateExpression  expression for the update operation defined in the store query
     */
    public void updateBy(String outputTableId, UpdateSet updateSetAttributes, Expression onUpdateExpression) {

        onDemandQuery.updateBy(outputTableId, updateSetAttributes, onUpdateExpression);
    }

    /**
     * Method to set an updateOrInsertStream as the outputStream of the store query
     *
     * @param outputTableId       id of the table which is going to be queried
     * @param updateSetAttributes updateSet for the attributes which are going to be updated.
     * @param onUpdateExpression  expression for the update or insert operation defined in the store query
     */
    public void updateOrInsertBy(String outputTableId, UpdateSet updateSetAttributes, Expression onUpdateExpression) {

        onDemandQuery.updateBy(outputTableId, updateSetAttributes, onUpdateExpression);
    }

    /**
     * Getter method to get the selector of the store query
     *
     * @return selector of the store query
     */
    public Selector getSelector() {

        return onDemandQuery.getSelector();
    }

    /**
     * Getter method to get the outputStream of the store query
     *
     * @return outputStream of the store query
     */
    public OutputStream getOutputStream() {

        return onDemandQuery.getOutputStream();
    }

    @Override
    public String toString() {

        return onDemandQuery.toString();
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        } else if (o instanceof OnDemandQuery) {
            return onDemandQuery.equals(o);
        } else {
            return o instanceof StoreQuery && onDemandQuery.equals(((StoreQuery) o).getOnDemandQuery());
        }
    }

    @Override
    public int hashCode() {

        return onDemandQuery.hashCode();
    }

    @Override
    public int[] getQueryContextStartIndex() {

        return onDemandQuery.getQueryContextStartIndex();
    }

    @Override
    public void setQueryContextStartIndex(int[] lineAndColumn) {

        onDemandQuery.setQueryContextStartIndex(lineAndColumn);
    }

    @Override
    public int[] getQueryContextEndIndex() {

        return onDemandQuery.getQueryContextEndIndex();
    }

    @Override
    public void setQueryContextEndIndex(int[] lineAndColumn) {

        onDemandQuery.setQueryContextEndIndex(lineAndColumn);
    }

    /**
     * This method returns the type of given store query.
     *
     * @return type of given store query
     */
    public StoreQueryType getType() {

        switch (onDemandQuery.getType()) {
            case INSERT:
                return StoreQueryType.INSERT;
            case DELETE:
                return StoreQueryType.DELETE;
            case UPDATE:
                return StoreQueryType.UPDATE;
            case SELECT:
                return StoreQueryType.SELECT;
            case UPDATE_OR_INSERT:
                return StoreQueryType.UPDATE_OR_INSERT;
            case FIND:
                return StoreQueryType.FIND;
        }
        return null;
    }

    /**
     * This method sets the type of given store query.
     */
    public void setType(StoreQueryType type) {

        switch (type) {

            case INSERT:
                onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.INSERT);
                break;
            case DELETE:
                onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.DELETE);
                break;
            case UPDATE:
                onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.UPDATE);
                break;
            case SELECT:
                onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.SELECT);
                break;
            case UPDATE_OR_INSERT:
                onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.UPDATE_OR_INSERT);
                break;
            case FIND:
                onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.FIND);
                break;
        }
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
