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

package io.siddhi.core.util;

/**
 * Class to hold constants.
 */
public final class SiddhiConstants {

    public static final String NAMESPACE_STORE = "store";
    public static final String NAMESPACE_SOURCE = "source";
    public static final String NAMESPACE_SOURCE_MAPPER = "sourceMapper";
    public static final String NAMESPACE_SCRIPT = "script";

    public static final String NAMESPACE_SINK = "sink";
    public static final String NAMESPACE_SINK_MAPPER = "sinkMapper";

    public static final String NAMESPACE_DISTINCT_COUNT_AGGREGATION_FUNCTION = "distinctCount";

    public static final String NAMESPACE_DISTRIBUTION_STRATEGY = "distributionStrategy";
    public static final String DISTRIBUTION_STRATEGY_PARTITIONED = "partitioned";

    public static final int BEFORE_WINDOW_DATA_INDEX = 0;
    public static final int ON_AFTER_WINDOW_DATA_INDEX = 1;
    public static final int OUTPUT_DATA_INDEX = 2;
    public static final int STATE_OUTPUT_DATA_INDEX = 3;

    public static final int STREAM_EVENT_CHAIN_INDEX = 0;
    public static final int STREAM_EVENT_INDEX_IN_CHAIN = 1;
    public static final int STREAM_ATTRIBUTE_TYPE_INDEX = 2;
    public static final int STREAM_ATTRIBUTE_INDEX_IN_TYPE = 3;

    public static final String ANNOTATION_NAME = "Name";
    public static final String ANNOTATION_PLAYBACK = "Playback";
    public static final String ANNOTATION_ENFORCE_ORDER = "EnforceOrder";
    public static final String ANNOTATION_ASYNC = "Async";

    public static final String ANNOTATION_ON_ERROR = "OnError";
    public static final String FAULT_STREAM_PREFIX = "!";
    public static final String ANNOTATION_ELEMENT_ACTION = "action";
    public static final String ANNOTATION_ELEMENT_ON_ERROR = "on.error";

    public static final String ANNOTATION_STATISTICS = "Statistics";
    public static final String ANNOTATION_INDEX_BY = "IndexBy";
    public static final String ANNOTATION_INDEX = "Index";
    public static final String ANNOTATION_PRIMARY_KEY = "PrimaryKey";
    public static final String ANNOTATION_STORE = "Store";
    public static final String ANNOTATION_SOURCE = "Source";
    public static final String ANNOTATION_SINK = "Sink";
    public static final String ANNOTATION_MAP = "Map";
    public static final String ANNOTATION_SCRIPT = "Script";
    public static final String ANNOTATION_DISTRIBUTION = "Distribution";
    public static final String ANNOTATION_DESTINATION = "Destination";
    public static final String ANNOTATION_ATTRIBUTES = "Attributes";
    public static final String ANNOTATION_PAYLOAD = "Payload";
    public static final String ANNOTATION_ELEMENT_BUFFER_SIZE = "buffer.size";
    public static final String ANNOTATION_ELEMENT_WORKERS = "workers";
    public static final String ANNOTATION_ELEMENT_MAX_BATCH_SIZE = "batch.size.max";
    public static final String ANNOTATION_ELEMENT_IDLE_TIME = "idle.time";
    public static final String ANNOTATION_ELEMENT_INCREMENT = "increment";
    public static final String ANNOTATION_ELEMENT_TYPE = "type";
    public static final String ANNOTATION_BUFFER_SIZE = "BufferSize";
    public static final String ANNOTATION_IGNORE_EVENTS_OLDER_THAN_BUFFER = "IgnoreEventsOlderThanBuffer";
    public static final String ANNOTATION_ELEMENT_REF = "ref";
    public static final String ANNOTATION_ELEMENT_ENABLE = "enable";
    public static final String ANNOTATION_ELEMENT_IS_READ_ONLY = "is.read.only";
    public static final String ANNOTATION_ELEMENT_IDLE_PERIOD = "idle.period";
    public static final String ANNOTATION_ELEMENT_INTERVAL = "interval";
    public static final String ANNOTATION_ELEMENT_INCLUDE = "include";
    public static final String ANNOTATION_PARTITION_BY_ID = "PartitionById";
    public static final String ANNOTATION_PERSISTED_AGGREGATION = "persistedAggregation";

    public static final String TRUE = "true";
    public static final String TRIGGER_START = "start";
    public static final int DEFAULT_EVENT_BUFFER_SIZE = 1024;
    public static final int HAVING_STATE = -2;
    public static final int UNKNOWN_STATE = -1;
    public static final int CURRENT = -1;
    public static final int LAST = -2;
    public static final int ANY = -1;

    public static final String DISTRIBUTION_STRATEGY_KEY = "strategy";
    public static final String PARTITION_KEY_FIELD_KEY = "partitionKey";

    public static final String METRIC_INFIX_SIDDHI_APPS = "SiddhiApps";
    public static final String METRIC_INFIX_STREAMS = "Streams";
    public static final String METRIC_INFIX_ON_DEMAND_QUERIES = "OnDemandQueries";
    public static final String METRIC_INFIX_TABLES = "Tables";
    public static final String METRIC_INFIX_TRIGGERS = "Trigger";
    public static final String METRIC_INFIX_SIDDHI = "Siddhi";
    public static final String METRIC_INFIX_QUERIES = "Queries";
    public static final String METRIC_INFIX_AGGREGATIONS = "Aggregations";
    public static final String METRIC_INFIX_WINDOWS = "Windows";
    public static final String METRIC_INFIX_SOURCES = "Sources";
    public static final String METRIC_INFIX_SOURCE_MAPPERS = "SourceMappers";
    public static final String METRIC_INFIX_SINKS = "Sinks";
    public static final String METRIC_INFIX_SINK_MAPPERS = "SinkMappers";
    public static final String METRIC_TYPE_FIND = "find";
    public static final String METRIC_TYPE_INSERT = "insert";
    public static final String METRIC_TYPE_UPDATE = "update";
    public static final String METRIC_TYPE_DELETE = "delete";
    public static final String METRIC_TYPE_UPDATE_OR_INSERT = "updateOrInsert";
    public static final String METRIC_TYPE_CONTAINS = "contains";
    public static final String METRIC_DELIMITER = ".";
    public static final String METRIC_AGGREGATE_ANNOTATION = "[+]";
    public static final String EXTENSION_SEPARATOR = ":";

    public static final String KEY_DELIMITER = ":-:";
    public static final String KEY_DELIMITER_FILE = "-_-";
    public static final String TRANSPORT_CHANNEL_CREATION_IDENTIFIER = "transportChannelCreationEnabled";

    public static final String NAMESPACE_PURGE = "purge";
    public static final String NAMESPACE_RETENTION_PERIOD = "retentionPeriod";

    public static final String PARTITION_ID_DEFAULT = "null";

    public static final String ANNOTATION_CACHE = "Cache";
    public static final String CACHE_EXPIRE_CURRENT_TIME = "current.time";
    public static final String CACHE_TABLE_SIZE = "Size";
    public static final String CACHE_QUERY_NAME = "table_cache_";
    public static final String CACHE_TABLE_TIMESTAMP_ADDED = "_TIMESTAMP_ADDED";
    public static final String CACHE_TABLE_TIMESTAMP_LRU = "_TIMESTAMP_LRU";
    public static final String CACHE_TABLE_COUNT_LFU = "_COUNT_LFU";
    public static final String ANNOTATION_CACHE_RETENTION_PERIOD = "retention.period";
    public static final String ANNOTATION_CACHE_POLICY = "cache.policy";
    public static final String ANNOTATION_CACHE_PURGE_INTERVAL = "purge.interval";

    public static final String AGG_START_TIMESTAMP_COL = "AGG_TIMESTAMP";
    public static final String AGG_EXTERNAL_TIMESTAMP_COL = "AGG_EVENT_TIMESTAMP";
    public static final String AGG_LAST_TIMESTAMP_COL = "AGG_LAST_EVENT_TIMESTAMP";
    public static final String AGG_SHARD_ID_COL = "SHARD_ID";
    public static final String AGG_TIME_ZONE = "aggTimeZone";
    public static final String AGG_TIME_ZONE_DEFAULT = "GMT";

    public static final String REPETITIVE_PARAMETER_NOTATION = "...";

    public static final String ENABLE_EVENT_COUNT_LOGGER = "enableLoggingEventCount";
    public static final String LOGGING_DURATION = "loggingDuration";

    public static final String NAMESPACE_RDBMS = "rdbms";
    public static final String FUNCTION_NAME_CUD = "cud";
    public static final String DB_AGGREGATION_CONFIG_FILE = "db-aggregation-config.xml";
    public static final String PLACEHOLDER_TABLE_NAME = "{{TABLE_NAME}}";
    public static final String PLACEHOLDER_COLUMNS = "{{COLUMNS}}";
    public static final String PLACEHOLDER_COLUMN = "{{COLUMN}}";
    public static final String PLACEHOLDER_DURATION = "{{DURATION}}";
    public static final String PLACEHOLDER_QUERY = "{{QUERY}}";
    public static final String PLACEHOLDER_SELECTORS = "{{SELECTORS}}";
    public static final String PLACEHOLDER_CONDITION = "{{CONDITION}}";
    public static final String PLACEHOLDER_INNER_QUERY_1 = "{{INNER_QUERY_1}}";
    public static final String PLACEHOLDER_INNER_QUERY_2 = "{{INNER_QUERY_2}}";
    public static final String PLACEHOLDER_FROM_CONDITION = "{{FROM_CONDITION}}";
    public static final String PLACEHOLDER_ON_CONDITION = "{{ON_CONDITION}}";

    public static final String INSERT_TO_TABLE_NAME = "TO_TABLE_NAME";
    public static final String FROM_TABLE_NAME = "FROM_TABLE_NAME";
    public static final String UPDATED_TIMESTAMP = "UPDATED_TIMESTAMP";
    public static final String FROM_TIMESTAMP = "FROM_TIMESTAMP";
    public static final String TO_TIMESTAMP = "TO_TIMESTAMP";
    public static final String SUB_SELECT_QUERY_REF_T2 = "t2";
    public static final String SUB_SELECT_QUERY_REF_T1 = "t1";
    public static final String INNER_SELECT_QUERY_REF_T3 = "t3";
    public static final String EQUALS = " = ";
    public static final String SQL_AS = " AS ";
    public static final String SQL_AND = " AND ";
    public static final String SQL_ON = " ON ";
    public static final String SQL_SELECT = " SELECT ";
    public static final String SQL_WHERE = " WHERE ";
    public static final String SQL_FROM = " FROM ";
    public static final String SQL_NOT_NULL = " IS NOT NULL ";

}
