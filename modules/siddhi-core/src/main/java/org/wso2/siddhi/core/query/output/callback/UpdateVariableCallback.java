/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.core.query.output.callback;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.executor.GlobalVariableExpressionExecutor;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

/**
 * This callback is an adapter between {@link org.wso2.siddhi.core.stream.StreamJunction} and
 * {@link GlobalVariableExpressionExecutor}.
 * It receives {@link ComplexEventChunk}s and update {@link GlobalVariableExpressionExecutor}.
 */
public class UpdateVariableCallback extends OutputCallback {
    /**
     * Window to which the events have to be inserted.
     */
    private final GlobalVariableExpressionExecutor globalVariableExpressionExecutor;

    /**
     * StreamDefinition of the window.
     */
    private final StreamDefinition outputStreamDefinition;

    public UpdateVariableCallback(GlobalVariableExpressionExecutor globalVariableExpressionExecutor, StreamDefinition
            outputStreamDefinition) {
        this.globalVariableExpressionExecutor = globalVariableExpressionExecutor;
        this.outputStreamDefinition = outputStreamDefinition;
    }

    /**
     * Add the event into the {@link GlobalVariableExpressionExecutor}
     *
     * @param complexEventChunk the event to add
     */
    @Override
    public void send(ComplexEventChunk complexEventChunk) {
        // If events are inserted directly from another window, expired events can arrive
        complexEventChunk.reset();
        while (complexEventChunk.hasNext()) {
            ComplexEvent complexEvent = complexEventChunk.next();
            if (complexEvent.getType() == ComplexEvent.Type.EXPIRED) {
                complexEvent.setType(ComplexEvent.Type.CURRENT);
            }
        }
        globalVariableExpressionExecutor.update(complexEventChunk);
    }

    /**
     * Return the StreamDefinition of the corresponding window.
     *
     * @return stream definition of the target window
     */
    public StreamDefinition getOutputStreamDefinition() {
        return outputStreamDefinition;
    }

    /**
     * Return the {@link GlobalVariableExpressionExecutor} associated with this callback.
     *
     * @return the Window
     */
    public GlobalVariableExpressionExecutor getGlobalVariableExpressionExecutor() {
        return globalVariableExpressionExecutor;
    }
}
