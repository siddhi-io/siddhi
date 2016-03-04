/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.eventtable.sync.util;

public class ThrottleConstants {



    private ThrottleConstants(){
        //avoids initialization
    }

    public static final String THROTTLE_POLICY_XML = "throttle-policy.xml";
    public static final String POLICY_ELEMENT = "policy";
    public static final String NAME = "name";
    public static final String TIER = "tier";
    public static final String LEVEL = "level";
    public static final String DESCRIPTION = "description";
    public static final String ELIGIBILITY_QUERY = "eligibilityQuery";
    public static final String DECISION_QUERY = "decisionQuery";
    public static final String CEP_CONFIG_XML = "global-CEP-config.xml";
    public static final String THROTTLE_COMMON_CONFIG_XML = "common_throttle_config.xml";
    public static final String CONFIG_ELEMENT = "GlobalCEPConfig";
    public static final String HOST_NAME = "hostName";
    public static final String TCP_PORT = "binaryTCPPort";
    public static final String SSL_PORT = "binarySSLPort";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String STREAM_NAME = "streamName";
    public static final String STREAM_VERSION = "streamVersion";
    public static final String REQUEST_STREAM = "RequestStream";
    public static final String ELIGIBILITY_STREAM = "EligibilityStream";
    public static final String EVENT_TABLE = "EventTable";
    public static final String LOCAL_QUERY = "LocalQuery";
    public static final String GLOBAL_QUERY = "GlobalQuery";
    public static final String HTTPS_PORT = "HTTPSPort";
    public static final String HTTP_PORT = "HTTPPort";
    public static final String REQUEST_STREAM_ID = "RequestStreamID";
    public static final String THROTTLE_STREAM = "ThrottleStream";
    public static final String THROTTLE_STREAM_ID = "ThrottleStreamID";
    public static final String EMITTING_QUERY = "EmittingQuery";
    public static final String COMMON_PLAN = "common-throttling-plan";

}
