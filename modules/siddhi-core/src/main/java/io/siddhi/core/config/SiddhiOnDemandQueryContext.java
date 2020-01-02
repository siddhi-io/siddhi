/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.config;

/**
 * Holder object for OnDemand query context information.
 */
public class SiddhiOnDemandQueryContext extends SiddhiQueryContext {

    private String onDemandQueryString;

    public SiddhiOnDemandQueryContext(SiddhiAppContext siddhiAppContext, String queryName, String queryString) {
        super(siddhiAppContext, queryName, null);
        this.onDemandQueryString = queryString;
    }

    public String getOnDemandQueryString() {
        return onDemandQueryString;
    }
}
