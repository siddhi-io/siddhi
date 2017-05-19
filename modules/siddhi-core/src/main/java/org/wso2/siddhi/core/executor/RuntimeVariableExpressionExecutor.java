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

package org.wso2.siddhi.core.executor;

/**
 * An abstract representation of constants and variables. This class provides an abstract
 * {@link RuntimeVariableExpressionExecutor#getValue} method and a convenient method
 * {@link RuntimeVariableExpressionExecutor#getValue(Class)} to cast or convert the value into
 * a desired type.
 */
public abstract class RuntimeVariableExpressionExecutor implements ExpressionExecutor {

    /**
     * Get the value stored in the expression.
     *
     * @return the value stored in the expression
     */
    public abstract Object getValue();

    /**
     * Cast and get the value to the desired type. The actual value must be either the subclass of the given
     * class or the given class must be {@link String}.
     * This method casts and returns the value of the desired type is same as the actual type
     * If the value is a {@link Number}, the desired type can be any subclasses of {@link Number}
     * If the desired type is {@link String}, this method returns the toString value of the object.
     * If nothing matched, throws {@link UnsupportedOperationException}.
     *
     * @param clazz the Class of the output type
     * @param <T>   the generic type
     * @return the value casted or converted into the desired type
     * @throws UnsupportedOperationException if the actual value cannot be converted into the desired type
     */
    public final <T> T getValue(Class<T> clazz) {
        Object value = getValue();
        if (value.getClass().equals(clazz)) {
            return (T) value;
        } else if (value instanceof Number) {
            Number number = (Number) value;
            if (Integer.class.equals(clazz)) {
                return (T) Integer.valueOf(number.intValue());
            } else if (Long.class.equals(clazz)) {
                return (T) Long.valueOf(number.longValue());
            } else if (Float.class.equals(clazz)) {
                return (T) Float.valueOf(number.floatValue());
            } else if (Double.class.equals(clazz)) {
                return (T) Double.valueOf(number.doubleValue());
            } else {
                throw new UnsupportedOperationException("Cannot convert " + number.getClass().getSimpleName() +
                        " to " + clazz.getSimpleName());
            }
        } else if (String.class.equals(clazz)) {
            return (T) value.toString();
        } else {
            throw new UnsupportedOperationException("Cannot convert " + getReturnType() +
                    " to " + clazz.getSimpleName());
        }
    }
}
