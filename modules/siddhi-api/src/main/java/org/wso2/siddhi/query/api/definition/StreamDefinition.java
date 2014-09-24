/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.query.api.definition;

import org.wso2.siddhi.query.api.expression.Expression;

import java.util.Arrays;
import java.util.List;

public class StreamDefinition extends AbstractDefinition {

    public StreamDefinition name(String streamId) {
        id = streamId;
        return this;
    }

    public StreamDefinition attribute(String attributeName, Attribute.Type type) {
        checkAttribute(attributeName);
        this.attributeList.add(new Attribute(attributeName, type));
        return this;
    }

    public StreamDefinition attribute(String attributeName,Attribute.Type type, Expression[] attributeValues) {
        if(attributeValues == null) {
            return attribute(attributeName, type);
        }else {
            checkAttribute(attributeName);
            this.attributeList.add(new Attribute(attributeName, attributeValues));
        }
        return this;
    }
    public StreamDefinition attribute(String attributeName,Attribute.Type type, String[] attributeValues) {
        if(attributeValues == null) {
            return attribute(attributeName, type);
        }else {
            checkAttribute(attributeName);
            this.attributeList.add(new Attribute(attributeName, Arrays.asList(attributeValues)));
        }
        return this;
    }

    public String getStreamId() {
        return id;
    }

    @Override
    public String toString() {
        return "StreamDefinition{" +
               "streamId='" + id + '\'' +
               ", attributeList=" + attributeList +
               '}';
    }

}
