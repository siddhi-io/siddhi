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
package io.siddhi.query.api.execution.query.output.stream;

import io.siddhi.query.api.expression.Expression;

/**
 * Query output stream try to update table else insert into it
 */
public class UpdateOrInsertStream extends OutputStream {

    private static final long serialVersionUID = 1L;
    protected Expression onUpdateExpression;
    private UpdateSet updateSetter;

    public UpdateOrInsertStream(String tableId, OutputEventType outputEventType, UpdateSet updateSet,
                                Expression onUpdateExpression) {
        this.updateSetter = updateSet;
        this.id = tableId;
        this.outputEventType = outputEventType;
        this.onUpdateExpression = onUpdateExpression;
    }

    public UpdateOrInsertStream(String tableId, UpdateSet updateSet, Expression onUpdateExpression) {
        this.updateSetter = updateSet;
        this.id = tableId;
        this.outputEventType = null;
        this.onUpdateExpression = onUpdateExpression;
    }

    public UpdateOrInsertStream(String tableId, OutputEventType outputEventType,
                                Expression onUpdateExpression) {
        this.id = tableId;
        this.outputEventType = outputEventType;
        this.onUpdateExpression = onUpdateExpression;
    }

    public UpdateOrInsertStream(String tableId, Expression onUpdateExpression) {
        this.id = tableId;
        this.outputEventType = null;
        this.onUpdateExpression = onUpdateExpression;
    }

    public static UpdateSet updateSet() {
        return new UpdateSet();
    }

    public Expression getOnUpdateExpression() {
        return onUpdateExpression;
    }

    public void setOnUpdateExpression(Expression onUpdateExpression) {
        this.onUpdateExpression = onUpdateExpression;
    }

    public UpdateSet getUpdateSet() {
        return updateSetter;
    }

    public void setUpdateSet(UpdateSet updateSet) {
        this.updateSetter = updateSet;
    }

    @Override
    public String toString() {
        return "UpdateOrInsertStream{" +
                "onUpdateExpression=" + onUpdateExpression +
                ", updateSet=" + updateSetter +
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
        if (!super.equals(o)) {
            return false;
        }

        UpdateOrInsertStream that = (UpdateOrInsertStream) o;

        if (onUpdateExpression != null ? !onUpdateExpression.equals(that.onUpdateExpression) :
                that.onUpdateExpression != null) {
            return false;
        }
        return updateSetter != null ? updateSetter.equals(that.updateSetter) : that.updateSetter == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (onUpdateExpression != null ? onUpdateExpression.hashCode() : 0);
        result = 31 * result + (updateSetter != null ? updateSetter.hashCode() : 0);
        return result;
    }
}
