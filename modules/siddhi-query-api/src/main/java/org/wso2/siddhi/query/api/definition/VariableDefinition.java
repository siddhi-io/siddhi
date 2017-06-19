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

package org.wso2.siddhi.query.api.definition;


import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.constant.BoolConstant;
import org.wso2.siddhi.query.api.expression.constant.Constant;
import org.wso2.siddhi.query.api.expression.constant.DoubleConstant;
import org.wso2.siddhi.query.api.expression.constant.FloatConstant;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.expression.constant.LongConstant;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.util.SiddhiConstants;

import java.util.Locale;

/**
 * Siddhi execution plan level variable definition.
 */
public class VariableDefinition extends AbstractDefinition {

    private static final long serialVersionUID = 1L;

    private String id;
    private Attribute.Type type;
    private Object value;

    protected VariableDefinition(String name) {
        this.id = SiddhiConstants.GLOBAL_VARIABLE_PREFIX + name;
    }

    public static VariableDefinition name(String name) {
        return new VariableDefinition(name);
    }

    public static void typeMismatchError(String id, Attribute.Type type, Constant constant) {
        id = id.replace(SiddhiConstants.GLOBAL_VARIABLE_PREFIX, "");
        String found = constant.getClass().getSimpleName().toUpperCase(Locale.ENGLISH).replace("CONSTANT", "");
        throw new SiddhiAppValidationException("Variable " + id + " requires " + type + " value but found " +
                found);
    }

    public String getId() {
        return id;
    }

    public Object getValue() {
        return value;
    }

    public Attribute.Type getType() {
        return type;
    }

    public VariableDefinition type(Attribute.Type type) {
        this.type = type;
        return this;
    }

    public VariableDefinition value(Constant value) {
        if (type == Attribute.Type.BOOL) {
            if (value instanceof BoolConstant) {
                this.value = ((BoolConstant) value).getValue();
            } else {
                typeMismatchError(id, type, value);
            }
        } else if (type == Attribute.Type.INT) {
            if (value instanceof IntConstant) {
                this.value = ((IntConstant) value).getValue();
            } else {
                typeMismatchError(id, type, value);
            }
        } else if (type == Attribute.Type.FLOAT) {
            if (value instanceof FloatConstant) {
                this.value = ((FloatConstant) value).getValue();
            } else {
                typeMismatchError(id, type, value);
            }
        } else if (type == Attribute.Type.LONG) {
            if (value instanceof LongConstant) {
                this.value = ((LongConstant) value).getValue();
            } else {
                typeMismatchError(id, type, value);
            }
        } else if (type == Attribute.Type.DOUBLE) {
            if (value instanceof DoubleConstant) {
                this.value = ((DoubleConstant) value).getValue();
            } else {
                typeMismatchError(id, type, value);
            }
        } else if (type == Attribute.Type.STRING) {
            if (value instanceof StringConstant) {
                this.value = ((StringConstant) value).getValue();
            } else {
                typeMismatchError(id, type, value);
            }
        } else if (type == Attribute.Type.OBJECT) {
            throw new SiddhiAppValidationException("Variable " + id + " cannot be initialized with an object");
        } else {
            throw new SiddhiAppValidationException("Variable " + id + " has an unknown type");
        }
        return this;
    }

    @Override
    public String toString() {
        return "Variable{" +
                "id='" + id + '\'' +
                ", type=" + type +
                ", value=" + value +
                '}';
    }
}
