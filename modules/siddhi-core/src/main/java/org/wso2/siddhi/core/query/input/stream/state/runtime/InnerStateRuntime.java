/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.core.query.input.stream.state.runtime;

import org.wso2.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import org.wso2.siddhi.core.query.input.stream.state.PostStateProcessor;
import org.wso2.siddhi.core.query.input.stream.state.PreStateProcessor;
import org.wso2.siddhi.core.query.processor.Processor;

import java.util.List;

/**
 * Created on 12/19/14.
 */
public interface InnerStateRuntime {

    PreStateProcessor getFirstProcessor();

    PostStateProcessor getLastProcessor();

    List<SingleStreamRuntime> getSingleStreamRuntimeList();

    void setFirstProcessor(PreStateProcessor firstProcessor);

    void setLastProcessor(PostStateProcessor lastProcessor);

    void addStreamRuntime(SingleStreamRuntime singleStreamRuntime);

    void setQuerySelector(Processor commonProcessor);

    void setStartState();

    void init();

    void reset();

    void update();

    InnerStateRuntime clone(String key);
}
