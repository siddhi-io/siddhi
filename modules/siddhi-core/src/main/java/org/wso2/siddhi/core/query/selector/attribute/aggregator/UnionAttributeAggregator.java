/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.core.query.selector.attribute.aggregator;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@link AttributeAggregator} to return a union of an aggregation of sets.
 */
@Extension(
        name = "union",
        namespace = "",
        description = "Aggregates sets into a union.",
        parameters =
        @Parameter(name = "set",
                description = "The java.util.HashSet object that needs to be added into the union set.",
                type = {DataType.OBJECT})
        ,
        returnAttributes = @ReturnAttribute(
                description = "Returns a java.util.HashSet object which is the union of aggregated sets",
                type = {DataType.OBJECT}),
        examples = @Example(
                syntax = "from stockStream \n" +
                        "select initSet(symbol) as initialSet \n" +
                        "insert into initStream \n\n" +
                        "" +
                        "from initStream#window.timeBatch(10 sec) \n" +
                        "select union(initialSet) as distinctSymbols \n" +
                        "insert into distinctStockStream;",
                description = "distinctStockStream will return the set object which contains the distinct set of " +
                        "stock symbols received during a sliding window of 10 seconds."
        )
)
public class UnionAttributeAggregator extends AttributeAggregator {

    private Set set = new HashSet();

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param configReader                 this hold the {@link CountAttributeAggregator} configuration reader.
     * @param siddhiAppContext             Siddhi app runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("union aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.OBJECT) {
            throw new OperationNotSupportedException("Parameter passed to union aggregator should be of type" +
                    " object but found: " + attributeExpressionExecutors[0].getReturnType());
        }
    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.OBJECT;
    }

    @Override
    public Object processAdd(Object data) {
        Set inputSet = (HashSet) data;
        for (Object o : inputSet) {
            set.add(o);
        }
        // Creating a new set object as the returned set reference is kept until the aggregated values are
        // inserted into the store
        Set returnSet = new HashSet();
        returnSet.addAll(set);
        return returnSet;
    }

    @Override
    public Object processAdd(Object[] data) {
        for (Object obj : data) {
            Set newSet = (HashSet) obj;
            for (Object o : newSet) {
                set.add(o);
            }
        }
        // Creating a new set object as the returned set reference is kept until the aggregated values are
        // inserted into the store
        Set returnSet = new HashSet();
        returnSet.addAll(set);
        return returnSet;
    }

    @Override
    public Object processRemove(Object data) {
        Set newSet = (HashSet) data;
        for (Object o : newSet) {
            set.remove(o);
        }
        Set returnSet = new HashSet();
        returnSet.addAll(set);
        return returnSet;
    }

    @Override
    public Object processRemove(Object[] data) {
        for (Object obj : data) {
            Set newSet = (HashSet) obj;
            for (Object o : newSet) {
                set.remove(o);
            }
        }
        Set returnSet = new HashSet();
        returnSet.addAll(set);
        return returnSet;
    }

    @Override
    public Object reset() {
        set.clear();
        Set returnSet = new HashSet();
        returnSet.addAll(set);
        return returnSet;
    }

    @Override
    public boolean canDestroy() {
        return set.size() == 0;
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        state.put("Set", set);
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        set = (Set) state.get("Set");
    }
}
