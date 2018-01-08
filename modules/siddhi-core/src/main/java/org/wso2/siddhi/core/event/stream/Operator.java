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
package org.wso2.siddhi.core.event.stream;

/**
 * The class which resembles a single operation performed on SnapshotableComplexEventChunk.
 */
public class Operator {
    public static final int ADD = 1;
    public static final int REMOVE = 2;
    public static final int PUT = 3;
    public static final int GET = 4;
    public static final int POLL = 5;
    public static final int CLEAR = 6;
    public static final int CLEARALL = 7;
    public static final int OVERWRITE = 8;
    public static final int REMOVE2 = 9;
    public static final int REMOVE3 = 10;
}
