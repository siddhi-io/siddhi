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
package io.siddhi.core.query.selector;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;

/**
 * ComplexEventChunk to also have the processing type
 */
public class SelectorTypeComplexEventChunk extends ComplexEventChunk<ComplexEvent> {

    private boolean isProcessPassThrough;

    public SelectorTypeComplexEventChunk(ComplexEventChunk<ComplexEvent> complexEventChunk,
                                         boolean isProcessPassThrough) {
        super(complexEventChunk.getFirst(), complexEventChunk.getLast());
        this.isProcessPassThrough = isProcessPassThrough;
    }

    public boolean isProcessPassThrough() {
        return isProcessPassThrough;
    }

}
