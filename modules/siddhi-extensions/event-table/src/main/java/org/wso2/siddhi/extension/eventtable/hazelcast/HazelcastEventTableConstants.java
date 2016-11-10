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

package org.wso2.siddhi.extension.eventtable.hazelcast;


public final class HazelcastEventTableConstants {
    private HazelcastEventTableConstants() {
    }

    public static final String HAZELCAST_INSTANCE_PREFIX = "org.wso2.siddhi.hazelcast.cluster.instance.";
    public static final String HAZELCAST_COLLECTION_PREFIX = "org.wso2.siddhi.hazelcast.cluster.collection.";

    public static final String ANNOTATION_ELEMENT_HAZELCAST_CLUSTER_NAME = "cluster.name";
    public static final String ANNOTATION_ELEMENT_HAZELCAST_CLUSTER_PASSWORD = "cluster.password";
    public static final String ANNOTATION_ELEMENT_HAZELCAST_CLUSTER_ADDRESSES = "cluster.addresses";
    public static final String ANNOTATION_ELEMENT_HAZELCAST_WELL_KNOWN_ADDRESSES = "well.known.addresses";
    public static final String ANNOTATION_ELEMENT_HAZELCAST_CLUSTER_COLLECTION = "collection.name";

}
