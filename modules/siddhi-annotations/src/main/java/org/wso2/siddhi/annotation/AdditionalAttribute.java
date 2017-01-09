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
package org.wso2.siddhi.annotation;

import org.wso2.siddhi.annotation.util.DataType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for storing additional attributes returned by a stream processor.
 * This should not be directly applied to any classes.
 * This should only be passed as a parameter to org.wso2.siddhi.core.util.docs.annotation.ReturnEvent
 *
 * <pre><code>
 * eg:-
 *      {@literal @}ReturnEvent({
 *          {@literal @}AdditionalAttribute(name = "attribute1", type = {DataType.INT, DataType.LONG}, description="Description about the addition return attributes"),
 *          {@literal @}AdditionalAttribute(name = "attribute2", type = {DataType.DOUBLE, DataType.FLOAT})
 *      })
 *      public CustomStreamProcessor extends StreamProcessor {
 *          ...
 *      }
 * </code></pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AdditionalAttribute {
    String name();

    DataType[] type();

    String description() default "";        // optional
}
