/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.aggregation.persistedaggregation.config;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This class represents the JAXB bean for the query configuration which provide a link between RDBMS Event Table
 * functions and DB vendor-specific SQL syntax.
 */
@XmlRootElement(name = "rdbms-table-configuration")
public class DBAggregationQueryConfiguration {

    private DBAggregationQueryConfigurationEntry[] databases;

    @XmlElement(name = "database")
    public DBAggregationQueryConfigurationEntry[] getDatabases() {
        return databases;
    }

    public void setDatabases(DBAggregationQueryConfigurationEntry[] databases) {
        this.databases = databases;
    }

}
