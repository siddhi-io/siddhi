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
package io.siddhi.core.query.input.stream;

import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.selector.QuerySelector;

import java.util.List;

/**
 * Interface for all StreamRuntime s.(Ex: JoinStreamRuntime, SingleStreamRuntime)
 */
public interface StreamRuntime {

    List<SingleStreamRuntime> getSingleStreamRuntimes();

    void setCommonProcessor(Processor commonProcessor);

    MetaComplexEvent getMetaComplexEvent();

    ProcessingMode getProcessingMode();

    QuerySelector getQuerySelector();
}
