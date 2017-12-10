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

package org.wso2.siddhi.core.util.persistence.util;

/**
 * A collection of constants used for state persistance.
 */
public class PersistenceConstants {

    public static final String RDBMS_QUERY_CONFIG_FILE = "rdbms-table-config.xml";
    public static final String PLACEHOLDER_TABLE_NAME = "{{TABLE_NAME}}";
    public static final String STATE_PERSISTENCE_NS = "state.persistence";
    public static final String STATE_PERSISTENCE_ENABLED = "enabled";
    public static final String STATE_PERSISTENCE_INTERVAL_IN_MIN = "intervalInMin";
    public static final String STATE_PERSISTENCE_REVISIONS_TO_KEEP = "revisionsToKeep";
    public static final String STATE_PERSISTENCE_CLASS = "class";
    public static final String STATE_PERSISTENCE_CONFIGS = "config";
    public static final String DEFAULT_FILE_PERSISTENCE_FOLDER = "siddhi-app-persistence";
    public static final String DEFAULT_DB_PERSISTENCE_DATASOURCE = "WSO2_ANALYTICS_DB";
    public static final String DEFAULT_DB_PERSISTENCE_TABLE_NAME = "PERSISTENCE_TABLE";

}
