/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/
package org.wso2.siddhi.classifiers.utils;

import org.wso2.siddhi.classifiers.trees.ht.DenseInstance;
import org.wso2.siddhi.classifiers.trees.ht.Instance;
import org.wso2.siddhi.classifiers.trees.ht.utils.Utils;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;

public class InstancesUtil {

    public static DenseInstance getInstance(List<Attribute> attributeList, Event events) {
        double[] instance = new double[attributeList.size()];
        for (int i = 0; i < attributeList.size(); i++) {
            String data = (String) events.getData(i);
            instance[i] = attributeList.get(i).indexOfValue(data);
        }
        double weight = 1.0;
        // todo add weight to be added during stream definition
        return new DenseInstance(weight, instance);
    }


}
