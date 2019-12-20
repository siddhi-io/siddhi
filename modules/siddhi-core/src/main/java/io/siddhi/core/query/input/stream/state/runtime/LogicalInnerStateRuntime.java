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
package io.siddhi.core.query.input.stream.state.runtime;

import io.siddhi.core.query.processor.Processor;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;

/**
 * Created on 12/19/14.
 */
public class LogicalInnerStateRuntime extends StreamInnerStateRuntime {

    private final InnerStateRuntime innerStateRuntime1;
    private final InnerStateRuntime innerStateRuntime2;

    public LogicalInnerStateRuntime(InnerStateRuntime innerStateRuntime1, InnerStateRuntime innerStateRuntime2,
                                    StateInputStream.Type stateType) {
        super(stateType);
        this.innerStateRuntime1 = innerStateRuntime1;
        this.innerStateRuntime2 = innerStateRuntime2;
    }

    @Override
    public void setQuerySelector(Processor commonProcessor) {
        innerStateRuntime2.setQuerySelector(commonProcessor);
        innerStateRuntime1.setQuerySelector(commonProcessor);
    }

    @Override
    public void setup() {
        innerStateRuntime2.setup();
        innerStateRuntime1.setup();
    }

    @Override
    public void init() {
        innerStateRuntime2.init();
        innerStateRuntime1.init();
    }

    @Override
    public void reset() {
        innerStateRuntime2.reset();
    }

    @Override
    public void update() {
        innerStateRuntime2.update();
    }

}
