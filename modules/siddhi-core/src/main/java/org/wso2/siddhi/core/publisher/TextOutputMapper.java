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

package org.wso2.siddhi.core.publisher;

import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.HashMap;
import java.util.Map;

public class TextOutputMapper extends OutputMapper {

    @Override
    void init() {
        // if there's anything to be initialized
    }

    @Override
    String generateDefaultMapping(StreamDefinition streamDefinition) {
        Map<String, String> mapping = new HashMap<String, String>();
        for (String attributeName : streamDefinition.getAttributeNameArray()) {
            mapping.put(attributeName, String.format("{{%s}}", attributeName));
        }
        return mapping.toString();
    }
}